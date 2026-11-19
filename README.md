# EventStorePoc

Many workers change the same entity at the same time, every change publishes an event, and something
downstream consumes those events. **What does that consumer need in order to apply them in the right order?**

Three scenarios, run side by side in one launch. Every one of them writes correctly — nothing is ever lost in
any of them — so the only variable left is what the published event gives a consumer to order by:

| Scenario | The event carries | |
|---|---|---|
| `TimestampOnly` | `{ Amount, PublishedAtUtc }` | the problem |
| `Guarded` | `{ Amount, PublishedAtUtc, Version }`, the version from a concurrency token | the solution |
| `AtomicIncrement` | the same event, the version from a single SQL statement | the alternative |

One variable changes per step: 1→2 adds the version to the event, 2→3 changes how the version is produced.
Each scenario owns its own table, so one run exercises all three against the same database:

```
dotnet run
dotnet run -- --Simulation:Scenarios TimestampOnly,Guarded
dotnet run -- --Database:Provider SqlServer
```

## The property that matters

A consumer receiving an update has to answer one question: *is this message newer than the state I already
have?* To answer it, the event needs a field with one property:

> Every write to an entity stamps a **strictly greater, per-entity sequence**, produced by the write itself.

Each part of that is doing work:

- **Strictly greater** makes "newer" decidable, and makes a duplicate impossible to mistake for progress.
- **Per-entity** is the honest limit. Version 5 of one balance says nothing about version 5 of another, so a
  consumer has to partition by entity id. This is not a global clock replacement.
- **Produced by the write** is what ties the value to *commit* order rather than to whoever stamped it. To
  write version N+1 a worker must first have read a committed version N, so
  `commit(N) < read(N) < commit(N+1)` — version order **is** commit order.

A publish timestamp fails the first part and the third. Pod clocks drift apart, so the readings are not
strictly increasing; and the reading is taken by the pod rather than by the database, so nothing ties it to
the commit at all. `TimestampOnly` exists to prove that rather than assert it.

## What comes out

```
  Scenario         Orders by  Attempts  Conflicts  Lost  Final amount  Final ver  Clock inv  Replay by clock  Replay by version
  TimestampOnly    clock            48         38     0          0.00         10        5/9  3 behind         exact
  Guarded          version          47         37     0          0.00         10        4/9  1 behind         exact
  AtomicIncrement  version          10          0     0          0.00         10        4/9  4 behind         exact

  Cost of being correct: Guarded needed 47 attempts to land 10 writes, AtomicIncrement needed 10.
```

Ten workers each take 10 out of a balance holding exactly `10 * 10`, so a correct run ends on 0. Read across
one row at a time:

- **`Lost` is 0 in every row, and every balance ends on 0.00 at version 10.** That column is the premise of
  the whole comparison: no scenario here loses a write, so nothing below is about lost updates.
- **`Clock inv`** is the finding. In every scenario, 4 or 5 of the 9 adjacent pairs in commit order have a
  *non-increasing* timestamp — the clock disagrees with the order the database committed in. That is a
  property of the field, so it shows up in all three rows; what differs is who depends on it.
- **`Replay by clock` vs `Replay by version`**: the same stream, delivered in the same scrambled order, into
  the same projection twice. The version lands on the source of truth every time. The clock does not.
- **`exact (luck)`**, which turns up on one row or another from run to run, is not a pass. It means that
  guard finished on the right value although the stream still arrived out of order, because the newest event
  happened to carry the latest reading too. On the next run the same code is several updates behind. **This
  is why the verdict is taken from `Clock inv` and not from the replay** — one replay landing right proves
  nothing, while a single inversion disproves the field.

## The three scenarios

A bank account (a `Balance`) holds 100. Ten workers each take 10 out, all at the same moment. Every change
also writes an event, in the same transaction, and something downstream consumes those events.

### TimestampOnly — the problem

The write is protected. `Version` is an EF Core concurrency token, so the save sends:

```sql
UPDATE TimestampBalances SET Amount = @a, Version = @newV WHERE Id = @id AND Version = @oldV
```

A stale writer updates 0 rows, gets `DbUpdateConcurrencyException` and retries from a fresh read. Nothing is
lost — the checks say so. The event published afterwards simply does not carry that version:

```
{ Amount, PublishedAtUtc }
```

This is a realistic shape rather than a strawman. A service using optimistic concurrency for write safety has
no obvious ordering field to publish — a SQL Server `rowversion` is a database-wide opaque counter, not
something to hand a consumer — so it publishes a timestamp instead.

The run then proves the timestamp cannot do the job:

```
  Ordering field - whether PublishedAtUtc could order this stream at all:
    [FAIL] PublishedAtUtc increases with commit order (5 of 9 adjacent pairs inverted: v1->v2, v3->v4, v4->v5, v7->v8, v9->v10)
    [PASS] No two events share a millisecond (0 of 10 events do)
```

The run reads its events back in version order, which *is* commit order, so walking them and watching the
timestamps is a direct test — and note that this is the harness checking, with ground truth a consumer does
not have. Five inversions means five places where a consumer ordering by the clock would reject an update
that was in fact newer. The consequence, replayed:

```
    Guard: PublishedAtUtc (pod clock)   <- the field this scenario's consumers use
      events applied        : 3
      stale events applied  : 0   (an update overwritten by an older one)
      events discarded      : 7
      final consumer state  : amount 30.00 at version 7
      verdict               : MISMATCH - the consumer ends up 3 update(s) behind the source of truth

    Guard: Version (entity version)
      events applied        : 4
      stale events applied  : 0   (an update overwritten by an older one)
      events discarded      : 6
      final consumer state  : amount 0.00 at version 10
      verdict               : MATCH
```

The balance in the database is correct and the consumer is three updates behind it, permanently. The second
guard is the same stream ordered by the field this scenario refuses to publish — it is there to show that the
information existed and was thrown away.

### Guarded — the solution

Identical write, identical retries, one field more on the event:

```
{ Amount, PublishedAtUtc, Version }
```

That is the entire difference between scenario 1 and scenario 2. The mappings are the same line for line,
which is deliberate: the fix is not a different write, it is publishing what the write already produced.

### Why the version has to come from a guarded write

The tempting wrong fix, once a timestamp has failed, is to add a `Version` column and increment it:

```sql
UPDATE Balances SET Amount = @a, Version = @newV WHERE Id = @id
```

That statement always succeeds. Two workers that read version 7 both write version 8, and the second silently
erases the first one's deduction — so the stream carries two events claiming version 8, a consumer ordering by
version is no better off than it was with the clock, and the balance is wrong as well. The property is
*strictly greater, produced by the write*, and a plain column has neither, because nothing made the read and
the increment atomic. `Guarded` and `AtomicIncrement` are the two ways to actually get it.

### AtomicIncrement — the alternative

The same published event, with the version produced a cheaper way. Never read-modify-write at all: one
statement does the arithmetic in the database and hands back the version it just produced.

```sql
-- PostgreSQL
UPDATE "AtomicBalances" SET "Amount" = "Amount" - @decrement, "Version" = "Version" + 1
WHERE "Id" = @id
RETURNING "Version", "Amount"

-- SQL Server
UPDATE [AtomicBalances] SET [Amount] = [Amount] - @d, [Version] = [Version] + 1
OUTPUT INSERTED.[Version], INSERTED.[Amount] WHERE [Id] = @id
```

There is no stale read to lose, so there is nothing to reject and nothing to retry: concurrent callers queue
on the row lock instead of colliding. It produces the identical gapless sequence for 10 attempts instead of
45.

The catch, and the reason the token still matters: the change has to be expressible **relative to the current
value**. A payload that is replaced wholesale — a rate, a calendar, an availability window — cannot be written
this way. It also costs a hand-written statement per database provider.

In every scenario the balance update and the event insert happen in **one transaction**, so either both are
visible or neither is.

## The events raised

Each scenario prints every event it produced, with its parameters. At the default ten workers all three lists
print whole, so they can be compared row by row.

`TimestampOnly` is printed the way its consumers receive it — sorted by the only field it publishes, and
without the version column it never gave them:

```
     #  Amount  Published  Worker  Att  Notes
     1   80.00  35:10.139     w10    2
     2   50.00  35:10.178      w3    4
     3   60.00  35:10.214      w5    3  balance went UP by 10.00
     4   40.00  35:10.341      w1    5
     5   20.00  35:10.592      w8    7
     6    0.00  35:10.847      w9    9
     7   10.00  35:11.170      w7    8  balance went UP by 10.00
     8   90.00  35:11.216      w2    1  balance went UP by 80.00
     9   70.00  35:11.424      w4    3
    10   30.00  35:11.443      w6    6
```

Nothing in that table is a false record. Every row is a real event carrying the real balance at the moment it
was written, and every one of them is individually consistent. It is the *order* that is wrong — and the
order is all a consumer has.

So read the `Amount` column downwards. The account is drawn down to **0.00 at row 6**, and then has money in
it again, twice more, finishing at **30.00**. A balance that is only ever drawn down cannot rise, so each
rise is a pair of events the clock placed the wrong way round. A consumer replaying this stream in the order
it can see ends up believing the account holds 30 when it holds nothing — which is exactly what the replay
below reports.

That is the argument for publishing a monotonic version, made without needing to mention versions at all.

`Guarded` — the same write, the same retries, the version now published, so the stream can be printed in
commit order:

```
     #  Version  Amount  Published  Worker  Att  Notes
     1        1   90.00  35:10.463      w1    1
     2        2   80.00  35:10.714      w8    2
     3        3   70.00  35:11.575      w6    3
     4        4   60.00  35:10.319      w3    3  published before previous
     5        5   50.00  35:10.310     w10    4  published before previous
     6        6   40.00  35:11.303      w7    5
     7        7   30.00  35:10.983      w9    6  published before previous
     8        8   20.00  35:11.492      w2    7
     9        9   10.00  35:11.598      w4    7
    10       10    0.00  35:10.388      w5    9  published before previous
```

`AtomicIncrement` — the same gapless sequence, every write on its first attempt:

```
     #  Version  Amount  Published  Worker  Att  Notes
     1        1   90.00  35:10.383      w3    1
     2        2   80.00  35:11.650      w6    1
     3        3   70.00  35:10.554      w1    1  published before previous
     4        4   60.00  35:10.801      w8    1
     5        5   50.00  35:10.441      w5    1  published before previous
     6        6   40.00  35:11.659      w4    1
     7        7   30.00  35:11.560      w2    1  published before previous
     8        8   20.00  35:10.397     w10    1  published before previous
     9        9   10.00  35:11.063      w9    1
    10       10    0.00  35:11.394      w7    1
```

`Amount` falls cleanly from 90 to 0 in both, because `Version` could be used to sort them. `TimestampOnly`
would look exactly like this if it published the version its write path already produced — that is the only
difference between the first table and these two.

Two columns carry the rest of the argument:

- `Version` is `1..10` and gapless in both, because both write safely — and `TimestampOnly` is too, since it
  writes identically. Not publishing it is enough on its own to make the stream unorderable downstream.
- `Att` is the attempt the write finally landed on, which is where the cost of each strategy shows up: the
  token climbs to attempt 9 for ten writes, `AtomicIncrement` stays on attempt 1 throughout.

`Worker` and `Att` are harness metadata rather than published fields. They are in the table so the retrying
can be seen, not because a consumer would receive them.

In the version-ordered tables, `published before previous` marks a row whose timestamp is earlier than the
previous version's. It appears in every scenario, because the clock is skewed in every scenario — that is the
point. It is noted on rows that print but never causes a row to print, since with pod skew about half the
stream qualifies.

Raise the worker count and the full list stops being readable, so `--Simulation:ShowEvents Anomalies` keeps a
few leading rows plus anything worth looking at and elides the rest. `TimestampOnly` at 100 workers:

```
  Events raised, in the only order these consumers can impose - by PublishedAtUtc (100 rows, balance rises 48 times, showing 17)

      #  Amount  Published  Worker  Att  Notes
      1  990.00  35:38.856     w84    1
      2  980.00  35:39.101     w24    1
      3  870.00  35:39.334     w82    1
      4  810.00  35:39.393     w93    2
      5  710.00  35:39.547     w87    4
    ...                                  1 rows not shown
```

The balance rises 48 times in 100 events. The gaps show how far the stream has been shuffled even where it
is falling: 980 straight to 870 means the ten events in between sort somewhere else entirely.

Run the same view on `Guarded` or `AtomicIncrement` and it reports `0 notable`, which is itself a result —
nothing in those 100 events duplicated a version or skipped one. A duplicate or a gap would be called out
there, which makes that view the thing to watch if a write path ever regresses.

## What gets checked

Two groups, kept apart on purpose.

**Writes** — every scenario is expected to pass all of these:

- Final amount is `0`; the balance must end empty, whatever the worker count
- Final version is `Workers`, and there are exactly `Workers` events
- Event versions are `1..Workers`, with no duplicates and no gaps
- Each event's amount matches its version (`100 - Version * 10`)

**Ordering field** — whether `PublishedAtUtc` could order the stream at all:

- Timestamps increase with commit order (they do not, in any scenario)
- No two events share a millisecond

A scenario that publishes a version passes when its writes are clean *and* a consumer ordering by `Version`
reproduces the source of truth under out-of-order delivery. `TimestampOnly` passes when its writes are clean
*and* the timestamps are demonstrably not in commit order. The run exits non-zero if any scenario misses its
expectation — if `TimestampOnly` ever reports zero inversions, the clocks did not actually disagree on that
run and `Simulation:ClockSkewMs` wants raising.

### How it scales

Ten workers is the default because all three event logs then fit on one screen. Raise `Simulation:Workers`
and two things grow:

- **The evidence.** At 100 workers about half of all adjacent pairs are inverted (48 of 99 on one run), and
  events start sharing a millisecond with each other, which no timestamp at that precision can separate.
- **The cost of the token.** At 100 workers the guarded scenarios need around 1400 attempts on PostgreSQL to
  land 100 writes, and between three and four thousand on SQL Server. `AtomicIncrement` needs exactly 100 on
  both. These counts move from run to run; the shape does not.

What does *not* grow reliably is the replay outcome. Whether a clock-ordered consumer ends up 4 or 18 updates
behind, or lands on the right value by luck, depends on whether the newest event happened to carry the latest
reading. That is the whole reason the verdict is the inversion count.

### What this assumes, and where it stops

- The version increment and the event insert are in the **same transaction**.
- **Every** write path goes through a protected one. Anything that updates the row without producing a
  version breaks the invariant silently.
- Ordering holds **per entity**. A consumer must partition by entity id.
- A version orders one entity's stream. It is not a global sequence and will not order a cross-entity feed or
  drive a watermark poll across a whole table.

There is deliberately **no unique index** on `(Scenario, BalanceId, Version)`: every scenario has to prove its
write path alone produces unique versions, and an index that rejected a duplicate would be doing that work
instead of the thing under test. In production you would add it — it is the only thing that catches a write
path which bypasses the tracked entity (`ExecuteUpdate`, raw SQL, bulk backfills), where a concurrency token
does not apply at all.

## How to run

You need the .NET 10 SDK and Docker running (the database starts in a throwaway Testcontainer).

```
dotnet run
```

Settings live in `appsettings.json` and can be overridden on the command line:

| `Simulation` setting | Default | What it does |
|---|---|---|
| `Scenarios` | `all` | Which to run and in what order, e.g. `Guarded,AtomicIncrement` |
| `Workers` | 10 | How many workers hit the same row at once; raise it to show how each strategy scales |
| `Decrement` | 10 | How much each worker takes out; the starting balance is `Workers * Decrement`, so a correct run always ends on 0 |
| `MaxAttemptsPerWorker` | 1000 | Retry ceiling before a worker gives up |
| `ClockSkewMs` | 1000 | How far the simulated pod clocks drift apart (+/-) |
| `DeliveryJitter` | 5 | How many positions redelivery may displace a message |
| `Seed` | 1234 | Seeds the skew and the delivery reordering, so a run can be reproduced |
| `ShowEvents` | `All` | Event log detail: `None`, `Anomalies` or `All` |
| `EventContextRows` / `MaxNotableRows` | 5 / 10 | How much the `Anomalies` view prints |

## Files

| File | What it does |
|---|---|
| `Program.cs` | Starts the database, runs each selected scenario, prints the summary |
| `Scenarios.cs` | The three scenario definitions, the ordering field each publishes, and `--Simulation:Scenarios` parsing |
| `ScenarioRunner.cs` | Releases the workers; holds the two write paths the three scenarios share |
| `Domain.cs` | The three balance types, `BalanceSnapshot`, `BalanceSnapshotEvent` |
| `AppDbContext.cs` | EF Core mapping; the first two are identical on purpose |
| `Validation.cs` | The write checks and the ordering-field checks |
| `ConsumerOrderingDemo.cs` | Replays events out of order, comparing the timestamp and version guards |
| `Reporting.cs` | The annotated event log and the summary table |
| `SimulationSettings.cs` | Everything configurable |
| `DatabaseProviders.cs` | Starts PostgreSQL or SQL Server in Docker |

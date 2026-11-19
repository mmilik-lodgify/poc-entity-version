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
  TimestampOnly    clock            47         37     0          0.00         10        4/9  4 behind         exact
  Guarded          version          45         35     0          0.00         10        4/9  exact (luck)     exact
  AtomicIncrement  version          10          0     0          0.00         10        4/9  3 behind         exact

  Cost of being correct: Guarded needed 45 attempts to land 10 writes, AtomicIncrement needed 10.
```

Ten workers each take 10 out of a balance holding exactly `10 * 10`, so a correct run ends on 0. Read across
one row at a time:

- **`Lost` is 0 in every row, and every balance ends on 0.00 at version 10.** That column is the premise of
  the whole comparison: no scenario here loses a write, so nothing below is about lost updates.
- **`Clock inv`** is the finding. In every scenario, 4 of the 9 adjacent pairs in commit order have a
  *non-increasing* timestamp — the clock disagrees with the order the database committed in. That is a
  property of the field, so it shows up in all three rows; what differs is who depends on it.
- **`Replay by clock` vs `Replay by version`**: the same stream, delivered in the same scrambled order, into
  the same projection twice. The version lands on the source of truth every time. The clock does not.
- **`exact (luck)`** on the `Guarded` row is not a pass. That guard finished on the right value although the
  stream still arrived out of order, because the newest event happened to carry the latest reading too. On
  the next run it is several updates behind. **This is why the verdict is taken from `Clock inv` and not
  from the replay** — one replay landing right proves nothing, while a single inversion disproves the field.

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
    [FAIL] PublishedAtUtc increases with commit order (4 of 9 adjacent pairs inverted: v3->v4, v6->v7, v8->v9, v9->v10)
    [PASS] No two events share a millisecond (0 of 10 events do)
```

Events are read back in version order, which *is* commit order, so walking them and watching the timestamps
is a direct test. Four inversions means four places where a consumer ordering by the clock would reject an
update that was in fact newer. The consequence, replayed:

```
    Guard: PublishedAtUtc (pod clock)   <- the field this scenario's consumers use
      events applied        : 4
      stale events applied  : 0   (an update overwritten by an older one)
      events discarded      : 6
      final consumer state  : amount 40.00 at version 6
      verdict               : MISMATCH - the consumer ends up 4 update(s) behind the source of truth

    Guard: Version (entity version)
      events applied        : 4
      stale events applied  : 0   (an update overwritten by an older one)
      events discarded      : 6
      final consumer state  : amount 0.00 at version 10
      verdict               : MATCH
```

The balance in the database is correct and the consumer is four updates behind it, permanently. The second
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

`TimestampOnly`. `Version` is shown here as the ground truth the run uses to judge the timestamp — it is not
in the published event:

```
     #  Version  Amount  Published  Worker  Att  Notes
     1        1   90.00  07:12.630     w10    1
     2        2   80.00  07:12.886      w1    2
     3        3   70.00  07:13.985      w4    2
     4        4   60.00  07:12.747      w3    3  published before previous
     5        5   50.00  07:13.154      w8    4
     6        6   40.00  07:14.013      w6    5
     7        7   30.00  07:12.799      w5    6  published before previous
     8        8   20.00  07:13.916      w2    7
     9        9   10.00  07:13.745      w7    8  published before previous
    10       10    0.00  07:13.421      w9    9  published before previous
```

Read the `Published` column downwards. It goes 12.630, 12.886, 13.985, 12.747, 13.154, 14.013 — the fourth
event was committed after the third and carries a reading more than a second earlier. A consumer sorting by
that column does not get this stream back in this order, and no amount of care downstream can recover it.

`Guarded` — same write, same retries, the version now published:

```
     #  Version  Amount  Published  Worker  Att  Notes
     1        1   90.00  07:12.916      w5    1
     2        2   80.00  07:12.870     w10    2  published before previous
     3        3   70.00  07:13.864      w7    2
     4        4   60.00  07:13.541      w9    3  published before previous
     5        5   50.00  07:14.156      w6    4
     6        6   40.00  07:13.302      w8    5  published before previous
     7        7   30.00  07:12.903      w3    5  published before previous
     8        8   20.00  07:13.067      w1    7
     9        9   10.00  07:14.069      w2    7
    10       10    0.00  07:14.176      w4    9
```

`AtomicIncrement` — the same gapless sequence, every write on its first attempt:

```
     #  Version  Amount  Published  Worker  Att  Notes
     1        1   90.00  07:12.996      w5    1
     2        2   80.00  07:14.224      w6    1
     3        3   70.00  07:12.968      w3    1  published before previous
     4        4   60.00  07:12.958     w10    1  published before previous
     5        5   50.00  07:13.378      w8    1
     6        6   40.00  07:13.955      w7    1
     7        7   30.00  07:14.236      w4    1
     8        8   20.00  07:14.137      w2    1  published before previous
     9        9   10.00  07:13.146      w1    1  published before previous
    10       10    0.00  07:13.639      w9    1
```

Two columns carry most of the argument:

- `Version` is `1..10` and gapless in all three, because all three write safely. Only the first scenario
  declines to publish it.
- `Att` is the attempt the write finally landed on, which is where the cost of each strategy shows up: the
  token climbs to attempt 9 for ten writes, `AtomicIncrement` stays on attempt 1 throughout.

`published before previous` marks a row whose timestamp is earlier than the previous version's. It appears in
every scenario, because the clock is skewed in every scenario — that is the point. It is noted on rows that
print but never causes a row to print, since with pod skew about half the stream qualifies.

Raise the worker count and the full list stops being readable, so `--Simulation:ShowEvents Anomalies` keeps a
few leading rows plus anything worth looking at and elides the rest. At 100 workers:

```
  Events raised (100 rows, 0 notable, showing 7)

      #  Version  Amount  Published  Worker  Att  Notes
      1        1  990.00  08:24.094     w12    1
      2        2  980.00  08:23.106      w3    1  published before previous
      3        3  970.00  08:23.222     w99    2
      4        4  960.00  08:23.160     w96    1  published before previous
      5        5  950.00  08:23.210      w5    2
    ...                                           93 rows not shown
     99       99   10.00  08:24.574     w26   34  published before previous
    100      100    0.00  08:25.239     w23   33
```

`0 notable` is itself a result: nothing in those 100 events duplicated a version or skipped one. A duplicate
or a gap would be called out here, which makes this view the thing to watch if a write path ever regresses.

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

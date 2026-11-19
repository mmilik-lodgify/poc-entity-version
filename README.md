# EventStorePoc

Many workers change the same entity at the same time. What does it take to not lose any of those changes,
and to give whatever consumes the resulting events a reliable way to order them?

Three write paths, run side by side in one launch and compared at the end:

| Scenario | What it does | |
|---|---|---|
| `Unguarded` | `Version` is a plain column: `UPDATE ... WHERE Id = @id` | the problem |
| `Guarded` | `Version` is an EF concurrency token: `UPDATE ... WHERE Id = @id AND Version = @oldV` | the solution |
| `AtomicIncrement` | one statement increments and returns the new version | the alternative |

Each scenario owns its own table, so one run exercises all three against the same database:

```
dotnet run
dotnet run -- --Simulation:Scenarios Unguarded,Guarded
dotnet run -- --Database:Provider SqlServer
```

## What comes out

```
  Scenario         Attempts  Conflicts  Lost  Final amount  Final ver  Dup ver  Consumer by clock  Consumer by version
  Unguarded              10          0     9         90.00          1        1  unorderable        unorderable
  Guarded                44         34     0          0.00         10        0  exact (luck)       exact
  AtomicIncrement        10          0     0          0.00         10        0  6 behind           exact

  Cost of being correct: Guarded needed 44 attempts to land 10 writes, AtomicIncrement needed 10.
```

Ten workers each take 10 out of a balance holding exactly `10 * 10`, so **a correct run ends on 0** and
anything left in the account is a write that went missing. Read across one row at a time:

- **Unguarded** lost 9 of 10 deductions and left 90 of the 100 still sitting there. Nothing threw, nothing
  retried, nothing was logged. All ten events claim version 1.
- **Guarded** got every deduction, at the cost of 34 rejected attempts (a similar count on SQL Server).
- **AtomicIncrement** got every deduction with no rejected attempts at all.
- **Consumer by clock** vs **by version**: the same event stream replayed into a downstream projection,
  ordered first by publish timestamp and then by entity version. The version lands on the source of truth
  every time. The clock does not: it left `AtomicIncrement`'s consumer six updates behind, and the
  `exact (luck)` on the `Guarded` row is not a pass — that guard finished on the right value although the
  stream still arrived out of order, because the newest event happened to carry the latest reading too. On
  a different run the same code is several updates behind. That is the difference between a field that
  orders a stream and one that often happens to.

Ten workers is the default because all three event logs then fit on one screen and can be read whole, which
is what makes the write paths comparable row by row. The failure does not need a crowd: ten workers lose
nine of ten writes on every run. Raise `Simulation:Workers` to see how the cost of each strategy scales — at
100 workers `Unguarded` loses 94 or 95 of the 100, `Guarded` needs around 1500 attempts on PostgreSQL and
around 3300 on SQL Server, and `AtomicIncrement` still needs exactly 100. Those counts move a little from
run to run; the shape does not.

One thing to expect when raising the worker count: the clock column starts saying `exact (luck)` more often,
which is not the clock getting better. A longer run spreads the commits across a window comparable to the
skew, so later versions do tend to carry later readings. The ratio that matters in production is the other
one — writes milliseconds apart, pod clocks seconds or minutes apart — and that is the one the default
models.

## The idea in simple words

A bank account (a `Balance`) holds 100. 10 workers each take 10 out, all at the same moment, so a run that
loses nothing empties the account exactly. Every change
also writes an event, and something downstream consumes those events.

### Unguarded — what goes wrong

A worker reads the balance, subtracts 10, adds 1 to the version and saves:

```sql
UPDATE UnguardedBalances SET Amount = @a, Version = @newV WHERE Id = @id
```

That statement always succeeds. Two workers that read version 7 both write version 8, and the second silently
erases the first one's deduction.

### Guarded — how we prevent it

`Version` is mapped as an EF Core **concurrency token**, so the same save sends:

```sql
UPDATE GuardedBalances SET Amount = @a, Version = @newV WHERE Id = @id AND Version = @oldV
```

1. Every balance has a **Version** number (starts at 0).
2. A worker reads the balance, subtracts 10, and adds 1 to the version.
3. The database only accepts the change if the version is still the one the worker originally read.
4. If someone got there first, 0 rows are updated and EF Core throws `DbUpdateConcurrencyException`.
5. The worker **starts over**: new read, new calculation, new save, until it succeeds.

### AtomicIncrement — the alternative

Never read-modify-write at all. One statement does the arithmetic in the database and hands back the version
it just produced, which then goes on the event:

```sql
-- PostgreSQL
UPDATE "AtomicBalances" SET "Amount" = "Amount" - @d, "Version" = "Version" + 1
WHERE "Id" = @id RETURNING "Version", "Amount"

-- SQL Server
UPDATE [AtomicBalances] SET [Amount] = [Amount] - @d, [Version] = [Version] + 1
OUTPUT INSERTED.[Version], INSERTED.[Amount] WHERE [Id] = @id
```

There is no stale read to lose, so there is nothing to reject and nothing to retry: concurrent callers queue
on the row lock instead of colliding. It produces exactly the same gapless sequence the token produces, for
10 attempts instead of 44.

The catch, and the reason the token still matters: the change has to be expressible **relative to the current
value**. A payload that is replaced wholesale — a rate, a calendar, an availability window — cannot be
written this way. It also costs a hand-written statement per database provider.

In every scenario the balance update and the event insert happen in **one transaction**, so either both are
saved or neither is.

## Why a version and not a timestamp

Preventing lost updates is only half the reason the version exists. The other half is downstream.

A consumer receiving an update has to answer one question: *is this message newer than the state I already
have?* The field usually reached for is a publish timestamp — but that timestamp is read from the clock of
whichever pod handled the write. Pods drift apart, two writes can land in the same millisecond, and nothing
ties the value to the order the database committed in.

The version has none of those problems, because of how it is produced: to write version N+1 a worker must
first have read a committed version N. So `commit(N) < read(N) < commit(N+1)` — **version order is commit
order**, per entity.

So after the workers finish, each scenario replays its recorded events into a small downstream projection in
the order a consumer would really see them (each message displaced by a few positions, the way a retry or a
slow partition reorders a stream) and does it twice: once deciding "is this newer?" by `PublishedAtUtc`, and
once by `Version`. `AtomicIncrement`'s stream, which is a gapless `1..10`:

```
    Guard: PublishedAtUtc (pod clock)
      events applied        : 3
      stale events applied  : 0   (an update overwritten by an older one)
      events discarded      : 7
      final consumer state  : amount 60.00 at version 4
      verdict               : MISMATCH - the consumer ends up 6 update(s) behind the source of truth

    Guard: Version (entity version)
      events applied        : 4
      stale events applied  : 0   (an update overwritten by an older one)
      events discarded      : 6
      final consumer state  : amount 0.00 at version 10
      verdict               : MATCH

    Clock skew put 3 of 9 adjacent version pairs in the wrong timestamp order.
    0 events share a millisecond with another event - no timestamp can order those at all.
```

The clock reading is not merely imprecise. It leaves the consumer holding permanently wrong state.

Ten writes are spread far enough apart that none of them share a millisecond. Raise the worker count and
they start colliding — at 100 workers a dozen or so events share a millisecond with another, and no
timestamp at that precision can separate those at all. The field is not merely skewed, it is blind.

`Unguarded` skips this comparison, because there is nothing left to order: all ten events claim version 1,
so the stream never reaches the version the entity ended on. (At higher worker counts it hands out a few
versions, and the highest one in the stream is still not the entity's.) The ordering information was
destroyed at write time, not in transit. **A version is only a trustworthy ordering field when the write
path that produced it guaranteed it was unique.**

## The events raised

Each scenario prints every event it produced, with its parameters. At the default ten workers all three
lists print whole, so the write paths can be compared row by row.

`Unguarded` — ten writes, one version:

```
     #  Version  Amount  Published  Worker  Att  Notes
     1        1   90.00  45:14.393     w10    1  version claimed by 10 events
     2        1   90.00  45:14.404      w3    1  duplicate version
     3        1   90.00  45:14.432      w5    1  duplicate version
     4        1   90.00  45:14.573      w1    1  duplicate version
     5        1   90.00  45:14.811      w8    1  duplicate version
     6        1   90.00  45:15.040      w9    1  duplicate version
     7        1   90.00  45:15.383      w7    1  duplicate version
     8        1   90.00  45:15.553      w2    1  duplicate version
     9        1   90.00  45:15.644      w4    1  duplicate version
    10        1   90.00  45:15.673      w6    1  duplicate version
```

Ten events, each one reporting a balance of 90 at version 1. Nine of the ten deductions are not in the
record at all, and no single event looks wrong.

`Guarded` — the same ten writes, each on a version of its own:

```
     #  Version  Amount  Published  Worker  Att  Notes
     1        1   90.00  45:15.755      w2    1
     2        2   80.00  45:14.765      w1    1  published before previous
     3        3   70.00  45:15.598      w7    2
     4        4   60.00  45:15.029      w8    3  published before previous
     5        5   50.00  45:14.632      w3    4  published before previous
     6        6   40.00  45:14.675      w5    5
     7        7   30.00  45:14.623     w10    5  published before previous
     8        8   20.00  45:15.293      w9    7
     9        9   10.00  45:15.909      w6    7
    10       10    0.00  45:15.915      w4    9
```

`AtomicIncrement` — the same gapless sequence, every write on its first attempt:

```
     #  Version  Amount  Published  Worker  Att  Notes
     1        1   90.00  45:15.120      w8    1
     2        2   80.00  45:15.985      w6    1
     3        3   70.00  45:15.883      w2    1  published before previous
     4        4   60.00  45:15.989      w4    1
     5        5   50.00  45:14.724     w10    1  published before previous
     6        6   40.00  45:14.741      w3    1
     7        7   30.00  45:14.784      w5    1
     8        8   20.00  45:15.398      w9    1
     9        9   10.00  45:14.911      w1    1  published before previous
    10       10    0.00  45:15.733      w7    1
```

Two columns carry most of the argument:

- `Version` is the ordering field. In `Unguarded` it is the same value ten times, so nothing downstream can
  order the stream by it — or by anything else. In the other two it is `1..10`, gapless.
- `Att` is the attempt the write finally landed on, which is where the cost of each strategy shows up:
  `Guarded` climbs to attempt 9 for ten writes, `AtomicIncrement` stays on attempt 1 throughout.

`Published` increases in none of the three lists. Pod clocks disagree by up to a second, so a later version
routinely carries an earlier reading — that is what `published before previous` marks, and with skew it is
the normal state of affairs rather than an anomaly.

Raise the worker count and the full list stops being readable, so `--Simulation:ShowEvents Anomalies` keeps
a few leading rows for context plus the rows worth looking at and elides the rest. The same scenario at 100
workers, in eleven lines:

```
  Events raised (100 rows, 6 notable, showing 11)

      #  Version  Amount  Published  Worker  Att  Notes
      1        1  990.00  27:56.903     w10    1  version claimed by 17 events
      2        1  990.00  27:56.903      w3    1  duplicate version
      3        1  990.00  27:56.906      w5    1  duplicate version
      4        1  990.00  27:56.927     w11    1  duplicate version
      5        1  990.00  27:56.940      w1    1  duplicate version
    ...                                           12 rows not shown
     18        2  980.00  27:56.900     w93    1  version claimed by 25 events, published before previous
    ...                                           24 rows not shown
     43        3  970.00  27:56.971     w63    1  version claimed by 32 events, published before previous
    ...                                           31 rows not shown
     75        4  960.00  27:56.966     w68    1  version claimed by 18 events, published before previous
    ...                                           17 rows not shown
     93        5  950.00  27:56.982     w18    1  version claimed by 6 events, published before previous
    ...                                           5 rows not shown
     99        6  940.00  27:57.237     w32    1  version claimed by 2 events, published before previous
    100        6  940.00  27:57.361     w64    1  duplicate version
```

A contested version is called out once, on the first event claiming it, with how many events claim it. Gaps
are flagged. Clock inversions are noted on rows that print, but never cause a row to print — with pod skew,
half the stream qualifies. `None` prints no event log at all.

## What gets checked

The same checks run against every scenario:

- Final amount is `0` — the balance must end empty, whatever the worker count
- Final version is `Workers` (10)
- There are exactly `Workers` events
- Event versions are `1..Workers`, with no duplicates and no gaps
- Each event's amount matches its version (`100 - Version * 10`)
- A consumer ordering by `Version` reproduces the source of truth under out-of-order delivery

`Guarded` and `AtomicIncrement` must pass all of them. `Unguarded` is *expected* to fail, and the run exits
non-zero if it does not — if the workers happen not to interleave, raise `Workers` and try again.

One trap worth noticing in the `Unguarded` output: `Each event snapshot Amount == 100 - Version * 10` still
passes. Every event is individually consistent with its own duplicated version, so the corrupted stream looks
perfectly healthy one event at a time. Only comparing events to each other reveals it.

### What this assumes, and where it stops

- The version increment and the event insert are in the **same transaction**.
- **Every** write path goes through the protected one. Anything that updates the row without producing a
  version breaks the invariant silently.
- Ordering holds **per entity**. Version 5 of one balance says nothing about version 5 of another, so a
  consumer must partition by entity id.

There is deliberately **no unique index** on `(Scenario, BalanceId, Version)`: the guarded scenarios have to
prove their write path alone is enough, and `Unguarded` has to be free to write the duplicates it produces so
they can be counted. In production you would add that index — it is the only thing that catches a write path
which bypasses the tracked entity (`ExecuteUpdate`, raw SQL, bulk backfills), where a concurrency token does
not apply at all.

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
| `Scenarios.cs` | The three scenario definitions and the `--Simulation:Scenarios` parsing |
| `ScenarioRunner.cs` | Releases the workers; holds all three write paths |
| `Domain.cs` | The three balance types, `BalanceSnapshot`, `BalanceSnapshotEvent` |
| `AppDbContext.cs` | EF Core mapping; the three mappings side by side |
| `Validation.cs` | The checks, applied identically to every scenario |
| `ConsumerOrderingDemo.cs` | Replays events out of order, comparing the timestamp and version guards |
| `Reporting.cs` | The annotated event log and the summary table |
| `SimulationSettings.cs` | Everything configurable |
| `DatabaseProviders.cs` | Starts PostgreSQL or SQL Server in Docker |

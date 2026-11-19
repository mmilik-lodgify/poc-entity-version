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
  Unguarded             100          0    95        950.00          5        6  unorderable        unorderable
  Guarded              1504       1404     0          0.00        100        0  6 behind           exact
  AtomicIncrement       100          0     0          0.00        100        0  14 behind          exact

  Cost of being correct: Guarded needed 1504 attempts to land 100 writes, AtomicIncrement needed 100.
```

Read across one row at a time:

- **Unguarded** lost 95 of 100 deductions and left the balance at 950 instead of 0. Nothing threw, nothing
  retried, nothing was logged. Six versions were handed out to a hundred events.
- **Guarded** got every deduction, at the cost of 1404 rejected attempts (3266 on SQL Server).
- **AtomicIncrement** got every deduction with no rejected attempts at all.
- **Consumer by clock** vs **by version**: the same event stream replayed into a downstream projection,
  ordered first by publish timestamp and then by entity version. The clock leaves the consumer several
  updates behind. The version lands exactly on the source of truth.

## The idea in simple words

A bank account (a `Balance`) holds 1000. 100 workers each take 10 out, all at the same moment. Every change
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
100 attempts instead of 1504.

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
once by `Version`:

```
    Guard: PublishedAtUtc (pod clock)
      events applied        : 23
      stale events applied  : 4   (an update overwritten by an older one)
      events discarded      : 77
      final consumer state  : amount 60.00 at version 94
      verdict               : MISMATCH - the consumer ends up 6 update(s) behind the source of truth

    Guard: Version (entity version)
      events applied        : 34
      stale events applied  : 0   (an update overwritten by an older one)
      events discarded      : 66
      final consumer state  : amount 0.00 at version 100
      verdict               : MATCH

    Clock skew put 50 of 99 adjacent version pairs in the wrong timestamp order.
    12 events share a millisecond with another event - no timestamp can order those at all.
```

The clock reading is not merely imprecise. It leaves the consumer holding permanently wrong state.

`Unguarded` skips this comparison, because there is nothing left to order: a hundred events claim six
versions between them, and the highest version in the stream is not even the one the entity ended on. The
ordering information was destroyed at write time, not in transit. **A version is only a trustworthy ordering
field when the write path that produced it guaranteed it was unique.**

## The events raised

Each scenario prints the events it actually produced, with their parameters, annotated rather than dumped.
`Unguarded`, in eleven lines:

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
half the stream qualifies.

`Att` is the attempt the write finally landed on, which is where the cost of the token shows up: in
`Guarded` the last few events are written on attempt 35 and 39, while `AtomicIncrement` is on attempt 1
throughout.

`--Simulation:ShowEvents All` prints every row, `None` prints none.

## What gets checked

The same checks run against every scenario:

- Final amount is `1000 - 100 * 10 = 0`
- Final version is `100`
- There are exactly 100 events
- Event versions are `1..100`, with no duplicates and no gaps
- Each event's amount matches its version (`1000 - Version * 10`)
- A consumer ordering by `Version` reproduces the source of truth under out-of-order delivery

`Guarded` and `AtomicIncrement` must pass all of them. `Unguarded` is *expected* to fail, and the run exits
non-zero if it does not — if the workers happen not to interleave, raise `Workers` and try again.

One trap worth noticing in the `Unguarded` output: `Each event snapshot Amount == 1000 - Version * 10` still
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
| `Workers` | 100 | How many workers hit the same row at once |
| `InitialAmount` / `Decrement` | 1000 / 10 | The arithmetic each worker performs |
| `MaxAttemptsPerWorker` | 1000 | Retry ceiling before a worker gives up |
| `ClockSkewMs` | 250 | How far the simulated pod clocks drift apart (+/-) |
| `DeliveryJitter` | 5 | How many positions redelivery may displace a message |
| `Seed` | 1234 | Seeds the skew and the delivery reordering, so a run can be reproduced |
| `ShowEvents` | `Anomalies` | Event log detail: `None`, `Anomalies` or `All` |
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

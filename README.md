# EventStorePoc

A small proof of concept, in two halves:

1. **The problem** — many workers change the same entity at the same time, and the updates are lost.
2. **The solution** — an entity version, maintained as a concurrency token, which stops the loss *and*
   gives downstream consumers something they can order by.

Pick the half with `Simulation:Mode`:

```
dotnet run -- --Simulation:Mode Broken
dotnet run -- --Simulation:Mode Fixed
```

## The idea in simple words

Imagine a bank account (a `Balance`) with 1000 in it. 100 workers each want to take 10 out, all at the
same moment. Every change also publishes an event, and something downstream consumes those events.

### Broken mode: what goes wrong

`Balance.Version` is an ordinary column. A worker reads the balance, subtracts 10, adds 1 to the version
and saves, which sends:

```sql
UPDATE Balances SET Amount = @a, Version = @newV WHERE Id = @id
```

That statement always succeeds. Two workers that read version 7 both write version 8, and the second one
silently erases the first one's deduction. Nothing throws, nothing retries, nothing is logged.

### Fixed mode: how we prevent it

`Balance.Version` is mapped as an EF Core **concurrency token**, so the same save sends:

```sql
UPDATE Balances SET Amount = @a, Version = @newV WHERE Id = @id AND Version = @oldV
```

1. Every `Balance` has a **Version** number (starts at 0).
2. A worker reads the balance, subtracts 10, and adds 1 to the version.
3. The database only accepts the change if the version is still the one the worker originally read.
4. If someone else got there first, 0 rows are updated and EF Core throws `DbUpdateConcurrencyException`.
5. The worker simply **starts over**: new read, new calculation, new save, until it succeeds.

Each successful save also writes an **event** (`BalanceSnapshotEvent`): a snapshot of the balance (id and
amount), the version number, and the publish time. The balance update and the event insert happen in
**one transaction**, so either both are saved or neither is.

## Why a version and not a timestamp

Preventing lost updates is only half the reason the version exists. The other half is downstream.

A consumer receiving an update has to answer one question: *is this message newer than the state I already
have?* The field usually reached for is a publish timestamp — but that timestamp is read from the clock of
whichever pod handled the write. Pods drift apart, two writes can land in the same millisecond, and nothing
ties the value to the order the database committed in.

The version has none of those problems, because of how it is produced: to write version N+1 a worker must
first have read a committed version N. So `commit(N) < read(N) < commit(N+1)` — **version order is commit
order**, per entity.

So after the workers finish, Fixed mode replays the recorded events into a small downstream projection in
the order a consumer would really see them (each message displaced by a few positions, the way a retry or a
slow partition reorders a stream) and does it twice: once deciding "is this newer?" by `PublishedAtUtc`, and
once by `Version`. A measured run:

```
  Guard: PublishedAtUtc (pod clock)
    events applied        : 22
    stale events applied  : 4   (an update overwritten by an older one)
    events discarded      : 78
    final consumer state  : amount 100.00 at version 90
    verdict               : MISMATCH - the consumer ends up 10 update(s) behind the source of truth

  Guard: Version (entity version)
    events applied        : 34
    stale events applied  : 0   (an update overwritten by an older one)
    events discarded      : 66
    final consumer state  : amount 0.00 at version 100
    verdict               : MATCH

  Clock skew put 42 of 99 adjacent version pairs in the wrong timestamp order.
  6 events share a millisecond with another event - no timestamp can order those at all.
```

The clock reading is not merely imprecise — it leaves the consumer holding permanently wrong state.

Broken mode skips this comparison, because there is nothing left to order: many events carry the same
version, and the highest version in the stream is not even the one the entity ended on. The ordering
information was destroyed at write time, not in transit. **The version is only a trustworthy ordering field
when it is produced under the concurrency token.**

### "Why not just `SET Amount = Amount - 10`?"

For this toy balance, a relative update would indeed be atomic and conflict-free. It would not produce a
number a consumer can order by, which is the thing we actually need. Real payloads are replaced wholesale
(a rate, a calendar, an availability window), not incremented.

## What we check at the end

In **Fixed** mode every check must pass:

- Final amount is `1000 - 100 * 10 = 0`
- Final version is `100`
- There are exactly 100 events
- Event versions are `1..100`, with no duplicates and no gaps
- Each event's amount matches its version (`1000 - Version * 10`)
- A consumer ordering by `Version` reproduces the source of truth under out-of-order delivery

In **Broken** mode those checks are *expected* to fail, and the run reports how much damage was done. It
exits 0 when the damage is there and 1 when the workers happened not to interleave (raise `Workers` and
try again).

Note: there is deliberately **no unique index** on `(BalanceId, Version)`. Fixed mode has to prove the
concurrency token alone is enough, and Broken mode has to be free to write the duplicates it produces. In
production you would add that index — it is the only thing that catches a write path which bypasses the
tracked entity (`ExecuteUpdate`, raw SQL, bulk backfills), where the concurrency token does not apply at all.

### What the approach assumes

- The version increment and the event insert are in the **same transaction**.
- **Every** write path goes through the tracked entity. Anything that updates the row without bumping the
  version breaks the invariant silently.
- Ordering holds **per entity**. Version 5 of one balance says nothing about version 5 of another, so a
  consumer must partition by entity id.

## How to run

You need the .NET 10 SDK and Docker running (the database starts in a throwaway Testcontainer).

```
dotnet run
```

Pick the database and the mode in `appsettings.json`, or override them:

```
dotnet run -- --Database:Provider SqlServer
dotnet run -- --Database:Provider Postgres
dotnet run -- --Simulation:Mode Broken
```

### Settings

| `Simulation` setting | Default | What it does |
|---|---|---|
| `Mode` | `Fixed` | `Broken` = plain column (the problem), `Fixed` = concurrency token (the solution) |
| `Workers` | 100 | How many workers hit the same row at once |
| `InitialAmount` / `Decrement` | 1000 / 10 | The arithmetic each worker performs |
| `MaxAttemptsPerWorker` | 1000 | Retry ceiling before a worker gives up |
| `ClockSkewMs` | 250 | How far the simulated pod clocks drift apart (+/-) |
| `DeliveryJitter` | 5 | How many positions redelivery may displace a message |
| `Seed` | 1234 | Seeds the skew and the delivery reordering, so a run can be reproduced |

## Files

| File | What it does |
|---|---|
| `Program.cs` | Starts the DB, runs the workers, validates the result |
| `Domain.cs` | `Balance`, `BalanceSnapshot`, `BalanceSnapshotEvent` |
| `AppDbContext.cs` | EF Core mapping; maps `Version` as a concurrency token in Fixed mode only |
| `ConsumerOrderingDemo.cs` | Replays the events out of order, comparing the timestamp and version guards |
| `SimulationMode.cs` | The two modes |
| `DatabaseProviders.cs` | Starts PostgreSQL or SQL Server in Docker |

## Example output

### Broken mode (PostgreSQL)

```
MODE    : Broken - Version is a plain column (UPDATE ... WHERE Id = @id)
DATABASE: PostgreSQL

Inserted Balance { Id = 1, Amount = 1000, Version = 0 }
Releasing 100 workers at once...

All workers finished in 0.7s
  Total attempts    : 100
  Max attempts      : 1 (single worker)
  Version conflicts : 0   <- nothing rejected any writer

The checks below are EXPECTED to fail in Broken mode - that failure is the point of this run.
Final Balance: Amount = 970.00, Version = 3
[FAIL] Balance.Amount == 0 (actual 970.00)
[FAIL] Balance.Version == 100 (actual 3)
[PASS] Event count == 100 (actual 100)
[PASS] Every event Version is > 0 and < 101
[FAIL] No duplicated versions (6 duplicated: 1, 2, 3, 4, 5, 6)
[FAIL] Each event Version is previous + 1 (sequence 1..N) (first = 1, gaps: 1->1, 1->1, ... (+83))
[PASS] Each event snapshot Amount == 1000 - Version * 10
[PASS] Each event snapshot Id == 1

=== Downstream consumer: which ordering field survives out-of-order delivery? ===
Not run in Broken mode: there is nothing left to order.

  99 of 100 events carry a version that another event also carries.
  The highest version in the stream is 7, but the entity ended on version 3.

  No consumer can recover the right order from that, whichever field it sorts by - the ordering
  information was destroyed at write time, not in transit. Fix the write path first.

Damage report:
  Updates lost             : 97 of 100
  Amount never deducted    : 970.00
  Duplicated event versions: 6
  Highest version reached  : 3 (should be 100)

RESULT: PROBLEM REPRODUCED - updates were lost and versions duplicated.
        Re-run with --Simulation:Mode Fixed to watch it disappear.
```

97 of the 100 deductions vanished. Note the two traps: no exception was raised anywhere, and
`Each event snapshot Amount == 1000 - Version * 10` still passes — every event is individually consistent
with its own version, so the corrupted stream looks perfectly healthy one event at a time.

### Fixed mode (PostgreSQL)

```
MODE    : Fixed - Version is an EF concurrency token (UPDATE ... WHERE Id = @id AND Version = @oldV)
DATABASE: PostgreSQL

Inserted Balance { Id = 1, Amount = 1000, Version = 0 }
Releasing 100 workers at once...

All workers finished in 2.1s
  Total attempts    : 1333
  Max attempts      : 39 (single worker)
  Version conflicts : 1233

Final Balance: Amount = 0.00, Version = 100
[PASS] Balance.Amount == 0 (actual 0.00)
[PASS] Balance.Version == 100 (actual 100)
[PASS] Event count == 100 (actual 100)
[PASS] Every event Version is > 0 and < 101
[PASS] No duplicated versions
[PASS] Each event Version is previous + 1 (sequence 1..N)
[PASS] Each event snapshot Amount == 1000 - Version * 10
[PASS] Each event snapshot Id == 1

=== Downstream consumer: which ordering field survives out-of-order delivery? ===
...

RESULT: SUCCESS - no lost updates, no duplicated versions, and a consumer that orders by
        Version stays correct under out-of-order delivery.
```

### Reading the output

- **1333 attempts for 100 workers**: 1233 attempts hit a version conflict and had to retry. That is the
  concurrency protection doing its job — and the cost of it. One worker needed 39 attempts, which is worth
  remembering before putting a hot entity behind this without backoff.
- **0 conflicts in Broken mode** is not good news. Nothing was rejected because nothing was checked.
- **All checks pass in Fixed mode** despite the conflicts: no update was lost and no version was duplicated.

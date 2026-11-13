# EventStorePoc

A small proof of concept: **can many workers change the same number at the same time without losing any change?**

## The idea in simple words

Imagine a bank account (a `Balance`) with 1000 in it. 100 workers each want to take 10 out, all at the same moment.

If they all read "1000" and all write "990", we lose updates. The final amount would be 990 instead of 0. That's the problem.

### How we prevent it

1. Every `Balance` has a **Version** number (starts at 0).
2. A worker reads the balance, subtracts 10, and adds 1 to the version.
3. When saving, the database only accepts the change if the version is still the one the worker originally read (`UPDATE ... WHERE Id = 1 AND Version = <old version>`).
4. If someone else got there first, nothing is updated and EF Core throws `DbUpdateConcurrencyException`.
5. The worker simply **starts over**: new read, new calculation, new save, until it succeeds.

Each successful save also writes an **event** (`BalanceSnapshotEvent`): a snapshot of the balance (id and amount) plus the same version number. The balance update and the event insert happen in **one transaction**, so either both are saved or neither is.

### What we check at the end

- Final amount is `1000 - 100 * 10 = 0`
- Final version is `100`
- There are exactly 100 events
- Event versions are `1..100`, with no duplicates and no gaps
- Each event's amount matches its version (`1000 - Version * 10`)

Note: there is deliberately **no unique index** on `(BalanceId, Version)`. The goal is to prove the concurrency token alone is enough. In production you would add that index as an extra safety net.

## How to run

You need the .NET 10 SDK and Docker running (the database starts in a throwaway Testcontainer).

```
dotnet run
```

Pick the database in `appsettings.json`, or override it:

```
dotnet run -- --Database:Provider SqlServer
dotnet run -- --Database:Provider Postgres
```

The `Simulation` section of `appsettings.json` controls the number of workers, starting amount, and so on.

## Files

| File | What it does |
|---|---|
| `Program.cs` | Starts the DB, runs the workers, validates the result |
| `Domain.cs` | `Balance`, `BalanceSnapshot`, `BalanceSnapshotEvent` |
| `AppDbContext.cs` | EF Core mapping; marks `Version` as the concurrency token |
| `DatabaseProviders.cs` | Starts PostgreSQL or SQL Server in Docker |

## Example output (PostgreSQL)

```
Starting PostgreSQL test container...
[testcontainers.org 00:00:00.22] Connected to Docker:
  Host: npipe://./pipe/docker_engine
  Server Version: 28.4.0
  Kernel Version: 6.18.58.1-microsoft-standard-WSL2
  API Version: 1.51
  Operating System: Docker Desktop (containerized)
  Total Memory: 9.72 GB
  Labels: 
    com.docker.desktop.address=npipe://\\.\pipe\docker_cli
[testcontainers.org 00:00:00.46] Docker container 14400fef7642 created
[testcontainers.org 00:00:00.54] Start Docker container 14400fef7642
[testcontainers.org 00:00:00.92] Wait for Docker container 14400fef7642 to complete readiness checks
[testcontainers.org 00:00:01.99] Docker container 14400fef7642 ready
[testcontainers.org 00:00:02.11] Docker container b545cd4c18a1 created
[testcontainers.org 00:00:02.13] Start Docker container b545cd4c18a1
[testcontainers.org 00:00:02.45] Wait for Docker container b545cd4c18a1 to complete readiness checks
[testcontainers.org 00:00:02.48] Execute "pg_isready --host localhost --dbname postgres --username postgres" at Docker container b545cd4c18a1
[testcontainers.org 00:00:03.67] Execute "pg_isready --host localhost --dbname postgres --username postgres" at Docker container b545cd4c18a1
[testcontainers.org 00:00:04.76] Execute "pg_isready --host localhost --dbname postgres --username postgres" at Docker container b545cd4c18a1
[testcontainers.org 00:00:04.86] Docker container b545cd4c18a1 ready
PostgreSQL ready in 4.9s
Inserted Balance { Id = 1, Amount = 1000, Version = 0 }
Releasing 100 workers at once...

All workers finished in 1.9s
  Total attempts : 834
  Max attempts   : 32 (single worker)
  Retried on DbUpdateConcurrencyException: 734x

Final Balance: Amount = 0.00, Version = 100
[PASS] Balance.Amount == 0 (actual 0.00)
[PASS] Balance.Version == 100 (actual 100)
[PASS] Event count == 100 (actual 100)
[PASS] Every event Version is > 0 and < 101
[PASS] No duplicated versions
[PASS] Each event Version is previous + 1 (sequence 1..N)
[PASS] Each event snapshot Amount == 1000 - Version * 10
[PASS] Each event snapshot Id == 1

RESULT: SUCCESS - no lost updates, no duplicated versions.
[testcontainers.org 00:00:08.28] Delete Docker container b545cd4c18a1

Process finished with exit code 0.
```

### Reading the output

- **834 total attempts for 100 workers**: 734 attempts hit a version conflict and had to retry. That's the concurrency protection doing its job.
- **All checks pass**: despite the conflicts, no update was lost and no version was duplicated.

using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace EventStorePoc;

public sealed record ScenarioRun(
    ScenarioDefinition Scenario,
    int TotalAttempts,
    int MaxAttempts,
    int Conflicts,
    IReadOnlyDictionary<string, int> OtherErrors,
    TimeSpan Elapsed,
    decimal FinalAmount,
    int FinalVersion,
    IReadOnlyList<BalanceSnapshotEvent> Events);

/// <summary>
/// Releases N workers at the same moment onto one balance row and records what came out. Each scenario owns
/// its own table, so scenarios never interfere with one another - but they are still run one after another
/// rather than side by side, because workers from two scenarios competing for the same connection pool would
/// make the attempt and conflict counts meaningless.
/// </summary>
public static class ScenarioRunner
{
    public static async Task<ScenarioRun> RunAsync(
        ScenarioDefinition scenario,
        DbContextOptions<AppDbContext> options,
        SimulationSettings settings,
        IReadOnlyList<TimeSpan> clockSkew)
    {
        await SeedAsync(scenario, options, settings);

        var errors = new ConcurrentDictionary<string, int>();
        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var workers = Enumerable.Range(1, settings.Workers)
            .Select(workerId => Task.Run(async () =>
            {
                await startGate.Task;
                return await RunWorkerAsync(scenario, options, settings, clockSkew[workerId - 1], workerId, errors);
            }))
            .ToArray();

        var stopwatch = Stopwatch.StartNew();
        startGate.SetResult();
        var attempts = await Task.WhenAll(workers);
        stopwatch.Stop();

        var (amount, version) = await LoadBalanceAsync(scenario, options, settings);
        var events = await LoadEventsAsync(scenario, options, settings);

        return new ScenarioRun(
            Scenario: scenario,
            TotalAttempts: attempts.Sum(),
            MaxAttempts: attempts.Max(),
            Conflicts: errors.GetValueOrDefault(nameof(DbUpdateConcurrencyException)),
            OtherErrors: errors
                .Where(x => x.Key != nameof(DbUpdateConcurrencyException))
                .ToDictionary(x => x.Key, x => x.Value),
            Elapsed: stopwatch.Elapsed,
            FinalAmount: amount,
            FinalVersion: version,
            Events: events);
    }

    private static async Task<int> RunWorkerAsync(
        ScenarioDefinition scenario,
        DbContextOptions<AppDbContext> options,
        SimulationSettings settings,
        TimeSpan skew,
        int workerId,
        ConcurrentDictionary<string, int> errors)
    {
        for (var attempt = 1; attempt <= settings.MaxAttemptsPerWorker; attempt++)
        {
            try
            {
                // Fresh DbContext per attempt -> fresh read of the current Amount/Version.
                await using var db = new AppDbContext(options);

                await (scenario.Kind switch
                {
                    ScenarioKind.TimestampOnly => WriteThroughEfAsync<TimestampBalance>(db, scenario, settings, skew, workerId, attempt),
                    ScenarioKind.Guarded => WriteThroughEfAsync<GuardedBalance>(db, scenario, settings, skew, workerId, attempt),
                    ScenarioKind.AtomicIncrement => WriteAtomicallyAsync(db, scenario, settings, skew, workerId, attempt),
                    _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario.Kind, "Unsupported scenario"),
                });

                return attempt;
            }
            catch (Exception ex)
            {
                // DbUpdateConcurrencyException is the expected one in the Guarded scenario; anything else
                // (e.g. a SQL Server deadlock victim) is also retried with a brand-new DbContext as required.
                errors.AddOrUpdate(ex.GetType().Name, 1, (_, count) => count + 1);
            }
        }

        throw new InvalidOperationException($"Worker {workerId} gave up after {settings.MaxAttemptsPerWorker} attempts.");
    }

    /// <summary>
    /// Read, change in memory, write back, under the concurrency token TBalance maps on its Version. Both
    /// scenarios that come through here - TimestampOnly and Guarded - are protected identically and lose
    /// nothing. They differ only in which field the event they publish gives a consumer to order by, so this
    /// method is deliberately unaware of which one it is serving.
    /// </summary>
    private static async Task WriteThroughEfAsync<TBalance>(
        AppDbContext db,
        ScenarioDefinition scenario,
        SimulationSettings settings,
        TimeSpan skew,
        int workerId,
        int attempt)
        where TBalance : class, IBalance
    {
        var balance = await db.Set<TBalance>().FindAsync(settings.BalanceId)
            ?? throw new InvalidOperationException($"Balance {settings.BalanceId} was not seeded.");

        balance.Amount -= settings.Decrement;
        balance.Version += 1;

        db.BalanceSnapshotEvents.Add(NewEvent(scenario, settings, balance.Amount, balance.Version, skew, workerId, attempt));

        // One implicit transaction: the UPDATE and the event INSERT either both land or neither does.
        // 0 rows updated => DbUpdateConcurrencyException => the event insert rolls back with it.
        await db.SaveChangesAsync();
    }

    private static async Task WriteAtomicallyAsync(
        AppDbContext db,
        ScenarioDefinition scenario,
        SimulationSettings settings,
        TimeSpan skew,
        int workerId,
        int attempt)
    {
        // An explicit transaction this time, because the increment is a raw statement rather than a
        // SaveChanges: the UPDATE takes the row lock and holds it until the event insert commits alongside it.
        await using var transaction = await db.Database.BeginTransactionAsync();

        var (version, amount) = await IncrementAsync(db, settings);

        db.BalanceSnapshotEvents.Add(NewEvent(scenario, settings, amount, version, skew, workerId, attempt));
        await db.SaveChangesAsync();

        await transaction.CommitAsync();
    }

    /// <summary>
    /// Does the whole change in a single statement and returns what it produced. There is no read-modify-write
    /// window, so no concurrency token is needed and no writer is ever rejected: concurrent callers queue on
    /// the row lock instead of colliding and retrying.
    ///
    /// The price is a hand-written statement per provider, and the hard limit is that the change has to be
    /// expressible relative to the current value. A payload that is replaced wholesale - a rate, a calendar,
    /// an availability window - cannot be written this way, which is why the Guarded scenario still matters.
    /// </summary>
    private static async Task<(int Version, decimal Amount)> IncrementAsync(AppDbContext db, SimulationSettings settings)
    {
        var sql = db.Database.IsNpgsql()
            ? """
              UPDATE "AtomicBalances"
              SET "Amount" = "Amount" - @decrement, "Version" = "Version" + 1
              WHERE "Id" = @id
              RETURNING "Version", "Amount"
              """
            : """
              UPDATE [AtomicBalances]
              SET [Amount] = [Amount] - @decrement, [Version] = [Version] + 1
              OUTPUT INSERTED.[Version], INSERTED.[Amount]
              WHERE [Id] = @id
              """;

        var connection = db.Database.GetDbConnection();

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = db.Database.CurrentTransaction?.GetDbTransaction();
        command.Parameters.Add(Parameter(command, "@decrement", settings.Decrement));
        command.Parameters.Add(Parameter(command, "@id", settings.BalanceId));

        await using var reader = await command.ExecuteReaderAsync();

        if (!await reader.ReadAsync())
        {
            throw new InvalidOperationException($"Balance {settings.BalanceId} was not seeded.");
        }

        return (reader.GetInt32(0), reader.GetDecimal(1));
    }

    private static DbParameter Parameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        return parameter;
    }

    private static BalanceSnapshotEvent NewEvent(
        ScenarioDefinition scenario,
        SimulationSettings settings,
        decimal amount,
        int version,
        TimeSpan skew,
        int workerId,
        int attempt) =>
        new()
        {
            Id = Guid.NewGuid(),
            Scenario = scenario.Name,
            BalanceId = settings.BalanceId,
            BalanceSnapshot = new BalanceSnapshot { Id = settings.BalanceId, Amount = amount },
            Version = version,

            // The ordering field today's code would reach for: wall-clock time, read from the clock of
            // whichever pod handled the request and wrong by up to Simulation:ClockSkewMs.
            PublishedAtUtc = DateTime.UtcNow + skew,
            WorkerId = workerId,
            Attempt = attempt,
        };

    private static async Task SeedAsync(
        ScenarioDefinition scenario,
        DbContextOptions<AppDbContext> options,
        SimulationSettings settings)
    {
        await using var db = new AppDbContext(options);

        switch (scenario.Kind)
        {
            case ScenarioKind.TimestampOnly:
                db.TimestampBalances.Add(new TimestampBalance
                {
                    Id = settings.BalanceId, Amount = settings.InitialAmount, Version = 0,
                });
                break;

            case ScenarioKind.Guarded:
                db.GuardedBalances.Add(new GuardedBalance
                {
                    Id = settings.BalanceId, Amount = settings.InitialAmount, Version = 0,
                });
                break;

            case ScenarioKind.AtomicIncrement:
                db.AtomicBalances.Add(new AtomicBalance
                {
                    Id = settings.BalanceId, Amount = settings.InitialAmount, Version = 0,
                });
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(scenario), scenario.Kind, "Unsupported scenario");
        }

        await db.SaveChangesAsync();
    }

    private static async Task<(decimal Amount, int Version)> LoadBalanceAsync(
        ScenarioDefinition scenario,
        DbContextOptions<AppDbContext> options,
        SimulationSettings settings)
    {
        await using var db = new AppDbContext(options);

        IBalance balance = scenario.Kind switch
        {
            ScenarioKind.TimestampOnly => await db.TimestampBalances.AsNoTracking().SingleAsync(b => b.Id == settings.BalanceId),
            ScenarioKind.Guarded => await db.GuardedBalances.AsNoTracking().SingleAsync(b => b.Id == settings.BalanceId),
            ScenarioKind.AtomicIncrement => await db.AtomicBalances.AsNoTracking().SingleAsync(b => b.Id == settings.BalanceId),
            _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario.Kind, "Unsupported scenario"),
        };

        return (balance.Amount, balance.Version);
    }

    private static async Task<List<BalanceSnapshotEvent>> LoadEventsAsync(
        ScenarioDefinition scenario,
        DbContextOptions<AppDbContext> options,
        SimulationSettings settings)
    {
        await using var db = new AppDbContext(options);

        return await db.BalanceSnapshotEvents.AsNoTracking()
            .Where(e => e.Scenario == scenario.Name && e.BalanceId == settings.BalanceId)
            .OrderBy(e => e.Version)
            .ThenBy(e => e.PublishedAtUtc)
            .ToListAsync();
    }
}

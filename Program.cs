using System.Collections.Concurrent;
using System.Diagnostics;
using EventStorePoc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

// Configuration: appsettings.json < environment variables (Simulation__Mode=Broken) < command line (--Simulation:Mode Broken)
var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables()
    .AddCommandLine(args)
    .Build();

var provider = configuration.GetValue<DatabaseProvider>("Database:Provider");
var settings = configuration.GetSection("Simulation").Get<SimulationSettings>() ?? new SimulationSettings();
var mode = settings.Mode;

await using var database = DatabaseContainerFactory.Create(provider);

Console.WriteLine($"MODE    : {mode} - {ModeDescription(mode)}");
Console.WriteLine($"DATABASE: {database.Name}");
Console.WriteLine();

Console.WriteLine($"Starting {database.Name} test container...");
var sw = Stopwatch.StartNew();
await database.StartAsync();
Console.WriteLine($"{database.Name} ready in {sw.Elapsed.TotalSeconds:F1}s");

var dbOptions = database.CreateDbContextOptions();

// 1. Schema + initial Balance(Amount = 1000, Version = 0)
await using (var db = NewContext())
{
    await db.Database.EnsureCreatedAsync();
    db.Balances.Add(new Balance { Id = settings.BalanceId, Amount = settings.InitialAmount, Version = 0 });
    await db.SaveChangesAsync();
}
Console.WriteLine($"Inserted Balance {{ Id = {settings.BalanceId}, Amount = {settings.InitialAmount}, Version = 0 }}");

// 2. N workers released at the same moment, each one: load -> Amount -= 10, Version += 1 -> add event -> save.
var stats = new ConcurrentDictionary<string, int>();
var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

var workers = Enumerable.Range(1, settings.Workers)
    .Select(workerId => Task.Run(async () =>
    {
        await startGate.Task;
        return await RunWorkerAsync(workerId);
    }))
    .ToArray();

Console.WriteLine($"Releasing {settings.Workers} workers at once...");
sw.Restart();
startGate.SetResult();
var attemptsPerWorker = await Task.WhenAll(workers);
sw.Stop();

var conflicts = stats.GetValueOrDefault(nameof(DbUpdateConcurrencyException));

Console.WriteLine();
Console.WriteLine($"All workers finished in {sw.Elapsed.TotalSeconds:F1}s");
Console.WriteLine($"  Total attempts    : {attemptsPerWorker.Sum()}");
Console.WriteLine($"  Max attempts      : {attemptsPerWorker.Max()} (single worker)");
Console.WriteLine($"  Version conflicts : {conflicts}{(conflicts == 0 ? "   <- nothing rejected any writer" : "")}");
foreach (var (exceptionType, count) in stats.Where(x => x.Key != nameof(DbUpdateConcurrencyException)).OrderByDescending(x => x.Value))
{
    Console.WriteLine($"  Also retried on {exceptionType}: {count}x");
}

// 3. Validation
Console.WriteLine();
if (mode == SimulationMode.Broken)
{
    Console.WriteLine("The checks below are EXPECTED to fail in Broken mode - that failure is the point of this run.");
}

var report = await ValidateAsync();
Console.WriteLine();

if (mode == SimulationMode.Fixed)
{
    Console.WriteLine(report.AllPassed
        ? "RESULT: SUCCESS - no lost updates, no duplicated versions."
        : "RESULT: FAILURE - the concurrency token did not hold.");
    return report.AllPassed ? 0 : 1;
}

// Broken mode inverts the verdict: the run is only useful if the damage actually showed up.
var lostUpdates = settings.Workers - report.FinalVersion;
var reproduced = !report.AllPassed;

Console.WriteLine("Damage report:");
Console.WriteLine($"  Updates lost             : {lostUpdates} of {settings.Workers}");
Console.WriteLine($"  Amount never deducted    : {report.FinalAmount - report.ExpectedFinalAmount}");
Console.WriteLine($"  Duplicated event versions: {report.DuplicateVersionCount}");
Console.WriteLine($"  Highest version reached  : {report.FinalVersion} (should be {settings.Workers})");
Console.WriteLine();
Console.WriteLine(reproduced
    ? "RESULT: PROBLEM REPRODUCED - updates were lost and versions duplicated."
      + Environment.NewLine
      + "        Re-run with --Simulation:Mode Fixed to watch it disappear."
    : "RESULT: PROBLEM NOT REPRODUCED - the workers happened not to interleave this run."
      + Environment.NewLine
      + "        Increase Simulation:Workers and try again.");
return reproduced ? 0 : 1;

AppDbContext NewContext() => new(dbOptions, mode);

static string ModeDescription(SimulationMode m) => m switch
{
    SimulationMode.Fixed => "Version is an EF concurrency token (UPDATE ... WHERE Id = @id AND Version = @oldV)",
    SimulationMode.Broken => "Version is a plain column (UPDATE ... WHERE Id = @id)",
    _ => throw new ArgumentOutOfRangeException(nameof(m), m, "Unsupported mode"),
};

async Task<int> RunWorkerAsync(int workerId)
{
    for (var attempt = 1; attempt <= settings.MaxAttemptsPerWorker; attempt++)
    {
        try
        {
            // Fresh DbContext per attempt -> fresh read of the current Amount/Version.
            await using var db = NewContext();

            var balance = await db.Balances.SingleAsync(b => b.Id == settings.BalanceId);
            balance.Amount -= settings.Decrement;
            balance.Version += 1;

            db.BalanceSnapshotEvents.Add(new BalanceSnapshotEvent
            {
                Id = Guid.NewGuid(),
                BalanceId = balance.Id,
                BalanceSnapshot = new BalanceSnapshot { Id = balance.Id, Amount = balance.Amount },
                Version = balance.Version,
            });

            // Fixed mode : UPDATE Balances SET Amount = @a, Version = @newV WHERE Id = @id AND Version = @oldV;
            // Broken mode: UPDATE Balances SET Amount = @a, Version = @newV WHERE Id = @id;
            //              INSERT INTO BalanceSnapshotEvents ...;
            // -> both statements in one transaction; 0 rows updated => DbUpdateConcurrencyException => rollback.
            await db.SaveChangesAsync();
            return attempt;
        }
        catch (Exception ex)
        {
            // DbUpdateConcurrencyException is the expected one in Fixed mode; anything else (e.g. SQL Server
            // deadlock victim) is also retried with a brand-new DbContext as required.
            stats.AddOrUpdate(ex.GetType().Name, 1, (_, c) => c + 1);
        }
    }

    throw new InvalidOperationException($"Worker {workerId} gave up after {settings.MaxAttemptsPerWorker} attempts.");
}

async Task<ValidationReport> ValidateAsync()
{
    await using var db = NewContext();

    var balance = await db.Balances.AsNoTracking().SingleAsync(b => b.Id == settings.BalanceId);
    var events = await db.BalanceSnapshotEvents.AsNoTracking()
        .Where(e => e.BalanceId == settings.BalanceId)
        .OrderBy(e => e.Version)
        .ToListAsync();

    var expectedFinalAmount = settings.InitialAmount - settings.Workers * settings.Decrement;
    var allOk = true;

    void Check(string description, bool ok, string? details = null)
    {
        allOk &= ok;
        Console.WriteLine($"[{(ok ? "PASS" : "FAIL")}] {description}{(details is null ? "" : $" ({details})")}");
    }

    Console.WriteLine($"Final Balance: Amount = {balance.Amount}, Version = {balance.Version}");

    Check($"Balance.Amount == {expectedFinalAmount}", balance.Amount == expectedFinalAmount, $"actual {balance.Amount}");
    Check($"Balance.Version == {settings.Workers}", balance.Version == settings.Workers, $"actual {balance.Version}");
    Check($"Event count == {settings.Workers}", events.Count == settings.Workers, $"actual {events.Count}");

    var outOfRange = events.Where(e => e.Version <= 0 || e.Version > settings.Workers).ToList();
    Check($"Every event Version is > 0 and < {settings.Workers + 1}", outOfRange.Count == 0,
        outOfRange.Count == 0 ? null : $"out of range: {string.Join(", ", outOfRange.Select(e => e.Version))}");

    var duplicates = events.GroupBy(e => e.Version).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
    Check("No duplicated versions", duplicates.Count == 0,
        duplicates.Count == 0 ? null : $"{duplicates.Count} duplicated: {Preview(duplicates.Select(v => v.ToString()))}");

    var gaps = events.Zip(events.Skip(1))
        .Where(pair => pair.Second.Version != pair.First.Version + 1)
        .Select(pair => $"{pair.First.Version}->{pair.Second.Version}")
        .ToList();
    var startsAtOne = events.Count == 0 || events[0].Version == 1;
    Check("Each event Version is previous + 1 (sequence 1..N)", startsAtOne && gaps.Count == 0,
        startsAtOne && gaps.Count == 0 ? null : $"first = {events.FirstOrDefault()?.Version}, gaps: {Preview(gaps)}");

    var wrongAmounts = events
        .Where(e => e.BalanceSnapshot.Amount != settings.InitialAmount - e.Version * settings.Decrement)
        .Select(e => $"v{e.Version}={e.BalanceSnapshot.Amount}")
        .ToList();
    Check($"Each event snapshot Amount == {settings.InitialAmount} - Version * {settings.Decrement}", wrongAmounts.Count == 0,
        wrongAmounts.Count == 0 ? null : $"{wrongAmounts.Count} wrong: {Preview(wrongAmounts)}");

    var wrongSnapshotIds = events.Count(e => e.BalanceSnapshot.Id != settings.BalanceId);
    Check($"Each event snapshot Id == {settings.BalanceId}", wrongSnapshotIds == 0, wrongSnapshotIds == 0 ? null : $"{wrongSnapshotIds} wrong");

    return new ValidationReport(
        AllPassed: allOk,
        FinalAmount: balance.Amount,
        ExpectedFinalAmount: expectedFinalAmount,
        FinalVersion: balance.Version,
        EventCount: events.Count,
        DuplicateVersionCount: duplicates.Count);
}

static string Preview(IEnumerable<string> values, int take = 10)
{
    var list = values.ToList();
    var head = string.Join(", ", list.Take(take));
    return list.Count > take ? $"{head}, ... (+{list.Count - take})" : head;
}

public sealed record ValidationReport(
    bool AllPassed,
    decimal FinalAmount,
    decimal ExpectedFinalAmount,
    int FinalVersion,
    int EventCount,
    int DuplicateVersionCount);

public sealed class SimulationSettings
{
    /// <summary>Broken = no concurrency token (shows the problem), Fixed = concurrency token (shows the solution).</summary>
    public SimulationMode Mode { get; init; } = SimulationMode.Fixed;

    public int BalanceId { get; init; } = 1;
    public decimal InitialAmount { get; init; } = 1000m;
    public decimal Decrement { get; init; } = 10m;
    public int Workers { get; init; } = 100;
    public int MaxAttemptsPerWorker { get; init; } = 1000;
}

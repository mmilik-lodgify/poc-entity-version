using System.Diagnostics;
using EventStorePoc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

// Configuration: appsettings.json < environment variables (Simulation__Scenarios=Guarded) < command line
// (--Simulation:Scenarios Guarded).
var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables()
    .AddCommandLine(args)
    .Build();

var provider = configuration.GetValue<DatabaseProvider>("Database:Provider");
var settings = configuration.GetSection("Simulation").Get<SimulationSettings>() ?? new SimulationSettings();
IReadOnlyList<ScenarioDefinition> scenarios;

try
{
    scenarios = ScenarioDefinition.Parse(settings.Scenarios);
}
catch (ArgumentException ex)
{
    // A typo in the scenario list should say so in one line, not in a stack trace.
    Console.Error.WriteLine(ex.Message);
    return 2;
}

// Every worker stands in for a pod with its own slightly wrong clock. The same skew is handed to every
// scenario, so they are compared under identical conditions.
var clockSkew = BuildClockSkew(settings);

await using var database = DatabaseContainerFactory.Create(provider);

Console.WriteLine($"DATABASE : {database.Name}");
Console.WriteLine($"SCENARIOS: {string.Join(", ", scenarios.Select(s => s.Name))}");
Console.WriteLine($"WORKERS  : {settings.Workers}, released at once onto a single balance row");
Console.WriteLine();

Console.WriteLine($"Starting {database.Name} test container...");
var stopwatch = Stopwatch.StartNew();
await database.StartAsync();
Console.WriteLine($"{database.Name} ready in {stopwatch.Elapsed.TotalSeconds:F1}s");

var dbOptions = database.CreateDbContextOptions();

// One schema holding one table per scenario, so every scenario can run in this single launch without
// touching the others' rows.
await using (var db = new AppDbContext(dbOptions))
{
    await db.Database.EnsureCreatedAsync();
}

var outcomes = new List<ScenarioOutcome>();

foreach (var scenario in scenarios)
{
    outcomes.Add(await RunScenarioAsync(scenario));
}

return Summary.Print(outcomes, settings) ? 0 : 1;

async Task<ScenarioOutcome> RunScenarioAsync(ScenarioDefinition scenario)
{
    Console.WriteLine();
    Console.WriteLine(new string('=', 100));
    Console.WriteLine($"SCENARIO: {scenario.Name}");
    Console.WriteLine($"  {scenario.Statement}");
    Console.WriteLine(new string('=', 100));
    Console.WriteLine($"  {scenario.Summary}");
    Console.WriteLine();

    var run = await ScenarioRunner.RunAsync(scenario, dbOptions, settings, clockSkew);

    Console.WriteLine($"  {settings.Workers} workers finished in {run.Elapsed.TotalSeconds:F1}s");
    Console.WriteLine($"    Total attempts    : {run.TotalAttempts}");
    Console.WriteLine($"    Max attempts      : {run.MaxAttempts} (single worker)");
    Console.WriteLine($"    Version conflicts : {run.Conflicts}{ConflictNote(scenario, run)}");

    foreach (var (error, count) in run.OtherErrors.OrderByDescending(x => x.Value))
    {
        Console.WriteLine($"    Also retried on {error}: {count}x");
    }

    Console.WriteLine();
    Console.WriteLine($"  Final Balance: Amount = {run.FinalAmount}, Version = {run.FinalVersion}");

    var validation = Validation.Run(run, settings);
    Console.WriteLine();

    EventLog.Print(run, settings);

    // The guard comparison is only worth running on a stream that is internally consistent. Decide that from
    // the events themselves rather than from which scenario produced them.
    var ordering = ConsumerOrderingDemo.IsOrderable(run.Events)
        ? ConsumerOrderingDemo.Run(run.Events, run.FinalAmount, settings)
        : null;

    if (ordering is null)
    {
        ConsumerOrderingDemo.PrintUnorderableStream(run.Events, run.FinalVersion);
    }
    else
    {
        ConsumerOrderingDemo.Print(ordering, settings);
    }

    var metExpectation = scenario.ExpectsLostUpdates
        ? !validation.AllPassed
        : validation.AllPassed && ordering?.VersionGuardMatches == true;

    return new ScenarioOutcome(run, validation, ordering, metExpectation);
}

// Zero conflicts means something quite different in each scenario, and the difference is the whole point.
static string ConflictNote(ScenarioDefinition scenario, ScenarioRun run) => (scenario.Kind, run.Conflicts) switch
{
    (ScenarioKind.Unguarded, 0) => "   <- nothing rejected any writer",
    (ScenarioKind.AtomicIncrement, 0) => "   <- nothing to reject: there is no stale read to lose",
    _ => "",
};

static TimeSpan[] BuildClockSkew(SimulationSettings simulation)
{
    // Deterministic, so that a surprising run can be reproduced from the seed printed in the output.
    var rng = new Random(simulation.Seed);

    return Enumerable.Range(0, simulation.Workers)
        .Select(_ => TimeSpan.FromMilliseconds((rng.NextDouble() * 2 - 1) * simulation.ClockSkewMs))
        .ToArray();
}

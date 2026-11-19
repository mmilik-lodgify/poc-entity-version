namespace EventStorePoc;

public enum ScenarioKind
{
    /// <summary>Read-modify-write through EF with Version as a plain column.</summary>
    Unguarded,

    /// <summary>Read-modify-write through EF with Version as a concurrency token, plus retry.</summary>
    Guarded,

    /// <summary>One atomic SQL statement that increments in place and returns the version it produced.</summary>
    AtomicIncrement,
}

public sealed record ScenarioDefinition(
    ScenarioKind Kind,
    string Name,
    string Statement,
    string Summary,
    bool ExpectsLostUpdates)
{
    public static readonly ScenarioDefinition Unguarded = new(
        ScenarioKind.Unguarded,
        "Unguarded",
        "UPDATE ... SET Amount = @a, Version = @newV WHERE Id = @id",
        "Version is an ordinary column, so two workers that read the same version both write it back and "
        + "the second silently erases the first. Nothing throws and nothing retries.",
        ExpectsLostUpdates: true);

    public static readonly ScenarioDefinition Guarded = new(
        ScenarioKind.Guarded,
        "Guarded",
        "UPDATE ... SET Amount = @a, Version = @newV WHERE Id = @id AND Version = @oldV",
        "Version is an EF concurrency token, so a writer whose version is stale updates 0 rows, is told so "
        + "with DbUpdateConcurrencyException, and starts over from a fresh read.",
        ExpectsLostUpdates: false);

    public static readonly ScenarioDefinition AtomicIncrement = new(
        ScenarioKind.AtomicIncrement,
        "AtomicIncrement",
        "UPDATE ... SET Amount = Amount - @d, Version = Version + 1 WHERE Id = @id  (RETURNING / OUTPUT)",
        "No read-modify-write at all: the database does the arithmetic and hands back the version it just "
        + "produced, so there is no stale read to lose and nothing to retry.",
        ExpectsLostUpdates: false);

    public static readonly IReadOnlyList<ScenarioDefinition> All = [Unguarded, Guarded, AtomicIncrement];

    /// <summary>
    /// Parses the comma-separated Simulation:Scenarios setting, e.g. "Guarded,AtomicIncrement" or "all".
    /// Order is preserved so the output can be read in whatever order was asked for.
    /// </summary>
    public static IReadOnlyList<ScenarioDefinition> Parse(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Trim().Equals("all", StringComparison.OrdinalIgnoreCase))
        {
            return All;
        }

        var selected = new List<ScenarioDefinition>();

        foreach (var name in value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var match = All.SingleOrDefault(s => s.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                ?? throw new ArgumentException(
                    $"Unknown scenario '{name}'. Known scenarios: {string.Join(", ", All.Select(s => s.Name))}, or 'all'.");

            if (!selected.Contains(match))
            {
                selected.Add(match);
            }
        }

        return selected;
    }
}

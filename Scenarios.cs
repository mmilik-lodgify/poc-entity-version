namespace EventStorePoc;

/// <summary>
/// What a downstream consumer is given to order the event stream by. This is the one variable the POC
/// changes between scenarios - every scenario's write path is correct.
/// </summary>
public enum OrderingField
{
    /// <summary>Wall-clock time read from the pod that handled the write, and nothing else.</summary>
    PublishedAtUtc,

    /// <summary>The entity version the write produced.</summary>
    Version,
}

public enum ScenarioKind
{
    /// <summary>Correct writes, but the event publishes only a timestamp for consumers to order by.</summary>
    TimestampOnly,

    /// <summary>The same write, publishing the version the concurrency token produced.</summary>
    Guarded,

    /// <summary>One atomic SQL statement that increments in place and returns the version it produced.</summary>
    AtomicIncrement,
}

public sealed record ScenarioDefinition(
    ScenarioKind Kind,
    string Name,
    string Statement,
    string Summary,
    OrderingField ConsumerOrdersBy)
{
    /// <summary>
    /// Whether this scenario exists to show that the field its consumers depend on cannot order the stream.
    /// A pod clock is not commit order, so a scenario that publishes nothing else is expected to fail - and
    /// proving that failure is the whole reason it is in the set.
    /// </summary>
    public bool ExpectsUnsoundOrdering => ConsumerOrdersBy == OrderingField.PublishedAtUtc;

    public static readonly ScenarioDefinition TimestampOnly = new(
        ScenarioKind.TimestampOnly,
        "TimestampOnly",
        "UPDATE ... WHERE Id = @id AND Version = @oldV, then publish { Amount, PublishedAtUtc }",
        "The write is guarded exactly as in Guarded, down to the retry loop, so nothing is lost - losing "
        + "updates is not what this scenario is about. The event published afterwards carries only the "
        + "pod's wall-clock time, which is therefore all a consumer has to order the stream by.",
        ConsumerOrdersBy: OrderingField.PublishedAtUtc);

    public static readonly ScenarioDefinition Guarded = new(
        ScenarioKind.Guarded,
        "Guarded",
        "UPDATE ... WHERE Id = @id AND Version = @oldV, then publish { Amount, PublishedAtUtc, Version }",
        "Same write, same retries, one field more on the event. Version is an EF concurrency token, so a "
        + "writer whose version is stale updates 0 rows, is told so with DbUpdateConcurrencyException, and "
        + "starts over from a fresh read - which is what makes the published version unique and ordered.",
        ConsumerOrdersBy: OrderingField.Version);

    public static readonly ScenarioDefinition AtomicIncrement = new(
        ScenarioKind.AtomicIncrement,
        "AtomicIncrement",
        "UPDATE ... SET Version = Version + 1 RETURNING Version, then publish { Amount, PublishedAtUtc, Version }",
        "The same published event, with the version produced a cheaper way: no read-modify-write at all, so "
        + "the database does the arithmetic and hands back the version it just produced. There is no stale "
        + "read to lose and nothing to retry.",
        ConsumerOrdersBy: OrderingField.Version);

    public static readonly IReadOnlyList<ScenarioDefinition> All = [TimestampOnly, Guarded, AtomicIncrement];

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

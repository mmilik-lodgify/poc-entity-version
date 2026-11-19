namespace EventStorePoc;

/// <summary>
/// Replays the recorded events into a downstream projection the way a real consumer receives them:
/// out of order, because of retries, partitions and redelivery.
///
/// A consumer that must not apply a stale update needs one question answered: "is the message I just got
/// newer than the state I already have?" Both candidate fields are replayed here, for every scenario:
///
///   * <see cref="BalanceSnapshotEvent.PublishedAtUtc"/> - wall-clock time read from the producing pod.
///   * <see cref="BalanceSnapshotEvent.Version"/>        - the per-entity sequence the write produced.
///
/// Replaying both every time is what keeps the comparison fair: same stream, same delivery order, and the
/// only thing that changes is which field decides. Which of the two a given scenario's consumers are
/// actually allowed to use is <see cref="ScenarioDefinition.ConsumerOrdersBy"/>.
///
/// The projection is trivially simple on purpose: keep the amount of the newest event seen so far. With a
/// sound ordering field the result equals the source of truth no matter how the events arrive.
/// </summary>
public static class ConsumerOrderingDemo
{
    private const string Indent = "  ";

    public sealed record GuardResult(
        string Name,
        OrderingField Field,
        decimal FinalAmount,
        int FinalVersion,
        int Applied,
        int StaleApplied,
        int Discarded,
        bool MatchesSourceOfTruth);

    public sealed record Result(
        GuardResult Timestamp,
        GuardResult Version,
        decimal SourceOfTruth,
        int SourceVersion,
        int DeliveryOutOfOrderPairs,
        int AdjacentPairs)
    {
        public bool VersionGuardMatches => Version.MatchesSourceOfTruth;
    }

    public static Result Run(IReadOnlyList<BalanceSnapshotEvent> events, decimal sourceOfTruth, SimulationSettings settings)
    {
        // The order the consumer happens to receive them in - nothing about delivery preserves publish order.
        var delivery = Deliver(events, settings.Seed, settings.DeliveryJitter);

        var deliveryOutOfOrder = delivery
            .Zip(delivery.Skip(1))
            .Count(pair => pair.Second.Version < pair.First.Version);

        var byTimestamp = Replay(
            "PublishedAtUtc (pod clock)",
            OrderingField.PublishedAtUtc,
            delivery,
            sourceOfTruth,
            isNewer: (candidate, lastApplied) => candidate.PublishedAtUtc > lastApplied.PublishedAtUtc);

        var byVersion = Replay(
            "Version (entity version)",
            OrderingField.Version,
            delivery,
            sourceOfTruth,
            isNewer: (candidate, lastApplied) => candidate.Version > lastApplied.Version);

        return new Result(
            Timestamp: byTimestamp,
            Version: byVersion,
            SourceOfTruth: sourceOfTruth,
            SourceVersion: events.Count == 0 ? 0 : events.Max(e => e.Version),
            DeliveryOutOfOrderPairs: deliveryOutOfOrder,
            AdjacentPairs: Math.Max(events.Count - 1, 0));
    }

    public static void Print(
        Result result,
        ScenarioDefinition scenario,
        ValidationReport validation,
        SimulationSettings settings)
    {
        Console.WriteLine($"{Indent}Downstream consumer: which ordering field survives out-of-order delivery?");
        Console.WriteLine($"{Indent}  Clocks differ by up to +/-{settings.ClockSkewMs} ms; messages arrive displaced by up to "
            + $"{settings.DeliveryJitter} positions (seed {settings.Seed}),");
        Console.WriteLine($"{Indent}  so {result.DeliveryOutOfOrderPairs} of {result.AdjacentPairs} neighbouring messages arrive out of version order.");
        Console.WriteLine($"{Indent}  Source of truth: Amount = {result.SourceOfTruth} at version {result.SourceVersion}");
        Console.WriteLine();

        PrintGuard(result.Timestamp, result.SourceVersion, scenario);
        PrintGuard(result.Version, result.SourceVersion, scenario);

        if (result.Timestamp.MatchesSourceOfTruth && validation.ClockInversions > 0)
        {
            Console.WriteLine($"{Indent}  Note: the timestamp guard happened to land on the right final value this run, because the");
            Console.WriteLine($"{Indent}  event with the latest clock reading also happened to be the newest one. That is luck, not");
            Console.WriteLine($"{Indent}  correctness - {validation.ClockInversions} pairs still arrived in the wrong order, and the next run can land");
            Console.WriteLine($"{Indent}  anywhere. It is exactly why this scenario is judged on those inversions rather than on");
            Console.WriteLine($"{Indent}  whether one replay came out right.");
            Console.WriteLine();
        }
    }

    private static void PrintGuard(GuardResult guard, int sourceVersion, ScenarioDefinition scenario)
    {
        var verdict = guard.MatchesSourceOfTruth
            ? "MATCH"
            : $"MISMATCH - the consumer ends up {sourceVersion - guard.FinalVersion} update(s) behind the source of truth";
        var dependency = guard.Field == scenario.ConsumerOrdersBy ? "   <- the field this scenario's consumers use" : "";

        Console.WriteLine($"{Indent}  Guard: {guard.Name}{dependency}");
        Console.WriteLine($"{Indent}    events applied        : {guard.Applied}");
        Console.WriteLine($"{Indent}    stale events applied  : {guard.StaleApplied}   (an update overwritten by an older one)");
        Console.WriteLine($"{Indent}    events discarded      : {guard.Discarded}");
        Console.WriteLine($"{Indent}    final consumer state  : amount {guard.FinalAmount} at version {guard.FinalVersion}");
        Console.WriteLine($"{Indent}    verdict               : {verdict}");
        Console.WriteLine();
    }

    private static GuardResult Replay(
        string name,
        OrderingField field,
        IReadOnlyList<BalanceSnapshotEvent> delivery,
        decimal sourceOfTruth,
        Func<BalanceSnapshotEvent, BalanceSnapshotEvent, bool> isNewer)
    {
        BalanceSnapshotEvent? lastApplied = null;
        var highestVersionApplied = 0;
        var applied = 0;
        var staleApplied = 0;
        var discarded = 0;
        var projectedAmount = 0m;

        foreach (var candidate in delivery)
        {
            if (lastApplied is not null && !isNewer(candidate, lastApplied))
            {
                discarded++;
                continue;
            }

            // The guard let this one through. Was it actually newer than what the consumer already had?
            if (candidate.Version < highestVersionApplied)
            {
                staleApplied++;
            }

            highestVersionApplied = Math.Max(highestVersionApplied, candidate.Version);
            lastApplied = candidate;
            projectedAmount = candidate.BalanceSnapshot.Amount;
            applied++;
        }

        return new GuardResult(
            Name: name,
            Field: field,
            FinalAmount: projectedAmount,
            FinalVersion: lastApplied?.Version ?? 0,
            Applied: applied,
            StaleApplied: staleApplied,
            Discarded: discarded,
            MatchesSourceOfTruth: projectedAmount == sourceOfTruth);
    }

    /// <summary>
    /// Produces the order the consumer receives the events in. Real redelivery does not shuffle a stream
    /// uniformly - it displaces individual messages by a few positions (a retry, a slow partition, a
    /// redelivered batch), so each event is moved by up to <paramref name="jitter"/> places.
    ///
    /// This matters for the demo: a uniform shuffle would push the consumer's high watermark close to the
    /// maximum on the first message and almost everything after it would be discarded unexamined.
    /// </summary>
    private static List<BalanceSnapshotEvent> Deliver(IReadOnlyList<BalanceSnapshotEvent> events, int seed, int jitter)
    {
        var rng = new Random(seed);

        return events
            .Select((e, index) => (
                Event: e,
                Position: index + (jitter <= 0 ? 0 : rng.Next(-jitter, jitter + 1)),
                Tiebreak: rng.Next()))
            .OrderBy(x => x.Position)
            .ThenBy(x => x.Tiebreak)
            .Select(x => x.Event)
            .ToList();
    }
}

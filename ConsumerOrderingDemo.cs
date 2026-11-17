namespace EventStorePoc;

/// <summary>
/// Replays the recorded events into a downstream projection the way a real consumer receives them:
/// out of order, because of retries, partitions and redelivery.
///
/// A consumer that must not apply a stale update needs one question answered: "is the message I just
/// got newer than the state I already have?" This compares the two fields usually available to answer it:
///
///   * <see cref="BalanceSnapshotEvent.PublishedAtUtc"/> - wall-clock time read from the producing pod.
///   * <see cref="BalanceSnapshotEvent.Version"/>        - the entity version produced under the concurrency token.
///
/// The projection is trivially simple on purpose: keep the amount of the newest event seen so far.
/// With a correct ordering field the result equals the source of truth no matter how the events arrive.
/// </summary>
public static class ConsumerOrderingDemo
{
    public sealed record GuardResult(
        string Name,
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
        int TimestampInversions,
        int AdjacentPairs,
        int SameMillisecondEvents)
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
            delivery,
            sourceOfTruth,
            isNewer: (candidate, lastApplied) => candidate.PublishedAtUtc > lastApplied.PublishedAtUtc);

        var byVersion = Replay(
            "Version (entity version)",
            delivery,
            sourceOfTruth,
            isNewer: (candidate, lastApplied) => candidate.Version > lastApplied.Version);

        // How often the clock disagrees with the real order: walk the events in version order and count
        // the neighbours whose timestamps do not increase.
        var inVersionOrder = events.OrderBy(e => e.Version).ToList();
        var inversions = inVersionOrder
            .Zip(inVersionOrder.Skip(1))
            .Count(pair => pair.Second.PublishedAtUtc <= pair.First.PublishedAtUtc);

        // Events a millisecond-precision timestamp cannot separate at all, however accurate the clocks are.
        var sameMillisecond = events
            .GroupBy(e => new DateTime(e.PublishedAtUtc.Ticks - e.PublishedAtUtc.Ticks % TimeSpan.TicksPerMillisecond))
            .Where(g => g.Count() > 1)
            .Sum(g => g.Count());

        return new Result(
            Timestamp: byTimestamp,
            Version: byVersion,
            SourceOfTruth: sourceOfTruth,
            SourceVersion: events.Count == 0 ? 0 : events.Max(e => e.Version),
            DeliveryOutOfOrderPairs: deliveryOutOfOrder,
            TimestampInversions: inversions,
            AdjacentPairs: Math.Max(inVersionOrder.Count - 1, 0),
            SameMillisecondEvents: sameMillisecond);
    }

    /// <summary>
    /// Broken mode produces a stream that cannot be ordered by anything, so there is no comparison to make:
    /// several events carry the same version, and the highest version in the stream is not necessarily the
    /// version the entity ended on. This states that instead of printing a meaningless guard comparison.
    /// </summary>
    public static void PrintUnorderableStream(IReadOnlyList<BalanceSnapshotEvent> events, Balance balance)
    {
        var sharingAVersion = events
            .GroupBy(e => e.Version)
            .Where(g => g.Count() > 1)
            .Sum(g => g.Count());
        var highestInStream = events.Count == 0 ? 0 : events.Max(e => e.Version);

        Console.WriteLine("=== Downstream consumer: which ordering field survives out-of-order delivery? ===");
        Console.WriteLine("Not run in Broken mode: there is nothing left to order.");
        Console.WriteLine();
        Console.WriteLine($"  {sharingAVersion} of {events.Count} events carry a version that another event also carries.");
        Console.WriteLine(highestInStream == balance.Version
            ? $"  {events.Count} writes produced a stream that only reaches version {highestInStream}."
            : $"  The highest version in the stream is {highestInStream}, but the entity ended on version {balance.Version}.");
        Console.WriteLine();
        Console.WriteLine("  No consumer can recover the right order from that, whichever field it sorts by - the ordering");
        Console.WriteLine("  information was destroyed at write time, not in transit. Fix the write path first.");
        Console.WriteLine("  Re-run with --Simulation:Mode Fixed to see the guards compared on a stream worth ordering.");
    }

    public static void Print(Result result, SimulationSettings settings)
    {
        Console.WriteLine("=== Downstream consumer: which ordering field survives out-of-order delivery? ===");
        Console.WriteLine($"Events were stamped by workers whose clocks differ by up to +/-{settings.ClockSkewMs} ms (pod clock skew)");
        Console.WriteLine($"and are delivered displaced by up to {settings.DeliveryJitter} positions (seed {settings.Seed}), the way retries and");
        Console.WriteLine($"redelivery reorder a stream: {result.DeliveryOutOfOrderPairs} of {result.AdjacentPairs} neighbouring messages arrive out of version order.");
        Console.WriteLine($"Source of truth: Balance.Amount = {result.SourceOfTruth} at version {result.SourceVersion}");
        Console.WriteLine();

        PrintGuard(result.Timestamp, result.SourceVersion);
        PrintGuard(result.Version, result.SourceVersion);

        Console.WriteLine($"  Clock skew put {result.TimestampInversions} of {result.AdjacentPairs} adjacent version pairs in the wrong timestamp order.");
        Console.WriteLine($"  {result.SameMillisecondEvents} events share a millisecond with another event - no timestamp can order those at all.");

        if (result.Timestamp.MatchesSourceOfTruth)
        {
            Console.WriteLine();
            Console.WriteLine("  Note: the timestamp guard happened to land on the right final value this run - the event with");
            Console.WriteLine("  the latest clock reading also happened to be the newest one. It is luck, not correctness:");
            Console.WriteLine($"  {result.TimestampInversions} pairs still arrived in the wrong order. Raise Simulation:ClockSkewMs and run it again.");
        }
    }

    private static void PrintGuard(GuardResult guard, int sourceVersion)
    {
        var behind = sourceVersion - guard.FinalVersion;
        var verdict = guard.MatchesSourceOfTruth
            ? "MATCH"
            : $"MISMATCH - the consumer ends up {behind} update(s) behind the source of truth";

        Console.WriteLine($"  Guard: {guard.Name}");
        Console.WriteLine($"    events applied        : {guard.Applied}");
        Console.WriteLine($"    stale events applied  : {guard.StaleApplied}   (an update overwritten by an older one)");
        Console.WriteLine($"    events discarded      : {guard.Discarded}");
        Console.WriteLine($"    final consumer state  : amount {guard.FinalAmount} at version {guard.FinalVersion}");
        Console.WriteLine($"    verdict               : {verdict}");
        Console.WriteLine();
    }

    private static GuardResult Replay(
        string name,
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

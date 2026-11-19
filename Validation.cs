namespace EventStorePoc;

public sealed record ValidationReport(
    bool WritesPassed,
    bool ClockIsCommitOrder,
    decimal ExpectedFinalAmount,
    int DuplicateVersionCount,
    int LostUpdates,
    int ClockInversions,
    int AdjacentPairs,
    int SameMillisecondEvents);

/// <summary>
/// Two groups of checks, deliberately kept apart.
///
/// The write checks ask whether the scenario lost anything. Every scenario is expected to pass all of them:
/// every write path here is correct, and keeping lost updates out of the comparison is what leaves the
/// ordering field as the only variable.
///
/// The ordering-field checks ask whether PublishedAtUtc is in commit order. Every scenario is expected to
/// fail those, because a pod clock is not commit order no matter who wrote the row. What differs between the
/// scenarios is only whether anything downstream depends on it - see
/// <see cref="ScenarioDefinition.ConsumerOrdersBy"/>.
/// </summary>
public static class Validation
{
    public static ValidationReport Run(ScenarioRun run, SimulationSettings settings)
    {
        var events = run.Events;
        var expectedFinalAmount = settings.InitialAmount - settings.Workers * settings.Decrement;
        var writesOk = true;

        void Check(string description, bool ok, string? details = null)
        {
            writesOk &= ok;
            Console.WriteLine($"    [{(ok ? "PASS" : "FAIL")}] {description}{(details is null ? "" : $" ({details})")}");
        }

        Console.WriteLine("  Writes - every scenario is expected to pass these:");

        Check($"Balance.Amount == {expectedFinalAmount}", run.FinalAmount == expectedFinalAmount, $"actual {run.FinalAmount}");
        Check($"Balance.Version == {settings.Workers}", run.FinalVersion == settings.Workers, $"actual {run.FinalVersion}");
        Check($"Event count == {settings.Workers}", events.Count == settings.Workers, $"actual {events.Count}");

        var outOfRange = events.Where(e => e.Version <= 0 || e.Version > settings.Workers).ToList();
        Check($"Every event Version is > 0 and < {settings.Workers + 1}", outOfRange.Count == 0,
            outOfRange.Count == 0 ? null : $"out of range: {Preview(outOfRange.Select(e => e.Version.ToString()))}");

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
        Check($"Each event snapshot Id == {settings.BalanceId}", wrongSnapshotIds == 0,
            wrongSnapshotIds == 0 ? null : $"{wrongSnapshotIds} wrong");

        // Events arrive ordered by Version, which is commit order: to write version N+1 a worker must first
        // have read a committed version N. So walking the list and watching the timestamps is enough to say
        // whether the clock agrees with the order the database committed in.
        var inversions = events.Zip(events.Skip(1))
            .Where(pair => pair.Second.PublishedAtUtc <= pair.First.PublishedAtUtc)
            .Select(pair => $"v{pair.First.Version}->v{pair.Second.Version}")
            .ToList();
        var adjacentPairs = Math.Max(events.Count - 1, 0);

        // Events a millisecond-precision timestamp cannot separate at all, however accurate the clocks are.
        var sameMillisecond = events
            .GroupBy(e => new DateTime(e.PublishedAtUtc.Ticks - e.PublishedAtUtc.Ticks % TimeSpan.TicksPerMillisecond))
            .Where(g => g.Count() > 1)
            .Sum(g => g.Count());

        Console.WriteLine();
        Console.WriteLine("  Ordering field - whether PublishedAtUtc could order this stream at all:");
        Console.WriteLine($"    [{(inversions.Count == 0 ? "PASS" : "FAIL")}] PublishedAtUtc increases with commit order"
            + $" ({inversions.Count} of {adjacentPairs} adjacent pairs inverted"
            + $"{(inversions.Count == 0 ? "" : $": {Preview(inversions, 5)}")})");
        Console.WriteLine($"    [{(sameMillisecond == 0 ? "PASS" : "FAIL")}] No two events share a millisecond"
            + $" ({sameMillisecond} of {events.Count} events do)");

        return new ValidationReport(
            WritesPassed: writesOk,
            ClockIsCommitOrder: inversions.Count == 0,
            ExpectedFinalAmount: expectedFinalAmount,
            DuplicateVersionCount: duplicates.Count,
            LostUpdates: settings.Workers - run.FinalVersion,
            ClockInversions: inversions.Count,
            AdjacentPairs: adjacentPairs,
            SameMillisecondEvents: sameMillisecond);
    }

    private static string Preview(IEnumerable<string> values, int take = 10)
    {
        var list = values.ToList();
        var head = string.Join(", ", list.Take(take));
        return list.Count > take ? $"{head}, ... (+{list.Count - take})" : head;
    }
}

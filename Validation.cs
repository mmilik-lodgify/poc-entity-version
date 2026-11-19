namespace EventStorePoc;

public sealed record ValidationReport(
    bool AllPassed,
    decimal ExpectedFinalAmount,
    int DuplicateVersionCount,
    int LostUpdates);

/// <summary>
/// The same checks are applied to every scenario. What differs is whether failing them is the expected
/// outcome - see <see cref="ScenarioDefinition.ExpectsLostUpdates"/>.
/// </summary>
public static class Validation
{
    public static ValidationReport Run(ScenarioRun run, SimulationSettings settings)
    {
        var events = run.Events;
        var expectedFinalAmount = settings.InitialAmount - settings.Workers * settings.Decrement;
        var allOk = true;

        void Check(string description, bool ok, string? details = null)
        {
            allOk &= ok;
            Console.WriteLine($"  [{(ok ? "PASS" : "FAIL")}] {description}{(details is null ? "" : $" ({details})")}");
        }

        if (run.Scenario.ExpectsLostUpdates)
        {
            Console.WriteLine("  The checks below are EXPECTED to fail here - that failure is the point of this scenario.");
        }

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

        return new ValidationReport(
            AllPassed: allOk,
            ExpectedFinalAmount: expectedFinalAmount,
            DuplicateVersionCount: duplicates.Count,
            LostUpdates: settings.Workers - run.FinalVersion);
    }

    private static string Preview(IEnumerable<string> values, int take = 10)
    {
        var list = values.ToList();
        var head = string.Join(", ", list.Take(take));
        return list.Count > take ? $"{head}, ... (+{list.Count - take})" : head;
    }
}

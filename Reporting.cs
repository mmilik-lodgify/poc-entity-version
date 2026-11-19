namespace EventStorePoc;

public sealed record ScenarioOutcome(
    ScenarioRun Run,
    ValidationReport Validation,
    ConsumerOrderingDemo.Result? Ordering,
    bool MetExpectation);

/// <summary>
/// Prints the events a scenario actually raised. Rows carry the parameters that matter - version, snapshot
/// amount, publish time, which worker produced it and on which attempt - and anything notable about a row is
/// called out in the Notes column, so the log is evidence rather than a dump.
/// </summary>
public static class EventLog
{
    private sealed record Row(int Number, BalanceSnapshotEvent Event, string Notes, bool Notable);

    public static void Print(ScenarioRun run, SimulationSettings settings)
    {
        if (settings.ShowEvents == ShowEvents.None || run.Events.Count == 0)
        {
            return;
        }

        var rows = Annotate(run.Events);
        var shown = Select(rows, settings);
        var notable = rows.Count(r => r.Notable);

        Console.WriteLine($"  Events raised ({rows.Count} rows, {notable} notable, showing {shown.Count})");
        Console.WriteLine();

        var table = new List<string[]> { new[] { "#", "Version", "Amount", "Published", "Worker", "Att", "Notes" } };
        var previousNumber = 0;

        foreach (var row in shown)
        {
            if (row.Number > previousNumber + 1)
            {
                table.Add(["...", "", "", "", "", "", $"{row.Number - previousNumber - 1} rows not shown"]);
            }

            table.Add([
                row.Number.ToString(),
                row.Event.Version.ToString(),
                row.Event.BalanceSnapshot.Amount.ToString("F2"),
                row.Event.PublishedAtUtc.ToString("mm:ss.fff"),
                $"w{row.Event.WorkerId}",
                row.Event.Attempt.ToString(),
                row.Notes,
            ]);

            previousNumber = row.Number;
        }

        if (previousNumber < rows.Count)
        {
            table.Add(["...", "", "", "", "", "", $"{rows.Count - previousNumber} rows not shown"]);
        }

        Table.Write(table, [true, true, true, true, true, true, false], indent: "    ");
        Console.WriteLine();

        if (settings.ShowEvents != ShowEvents.All && shown.Count < rows.Count)
        {
            Console.WriteLine("    (pass --Simulation:ShowEvents All for every row)");
            Console.WriteLine();
        }
    }

    private static List<Row> Annotate(IReadOnlyList<BalanceSnapshotEvent> events)
    {
        // Events arrive ordered by version, so every set of events claiming one version is contiguous.
        var claimsPerVersion = events.GroupBy(e => e.Version).ToDictionary(g => g.Key, g => g.Count());
        var rows = new List<Row>(events.Count);

        for (var i = 0; i < events.Count; i++)
        {
            var current = events[i];
            var previous = i > 0 ? events[i - 1] : null;
            var notes = new List<string>();
            var notable = false;

            if (claimsPerVersion[current.Version] > 1)
            {
                // Only the first event of a contested version is worth printing: the other eighteen rows
                // saying "duplicate version" underneath it add nothing.
                if (previous is null || previous.Version != current.Version)
                {
                    notes.Add($"version claimed by {claimsPerVersion[current.Version]} events");
                    notable = true;
                }
                else
                {
                    notes.Add("duplicate version");
                }
            }

            if (previous is not null && current.Version > previous.Version + 1)
            {
                notes.Add($"gap after v{previous.Version}");
                notable = true;
            }

            // A clock inversion is the expected state of affairs once the pods disagree, so it is worth
            // seeing on a row that gets printed but is not a reason to print one - half the stream qualifies.
            if (previous is not null && current.Version > previous.Version && current.PublishedAtUtc <= previous.PublishedAtUtc)
            {
                notes.Add("published before previous");
            }

            rows.Add(new Row(i + 1, current, string.Join(", ", notes), notable));
        }

        return rows;
    }

    private static List<Row> Select(List<Row> rows, SimulationSettings settings)
    {
        if (settings.ShowEvents == ShowEvents.All)
        {
            return rows;
        }

        var keep = new HashSet<int>();

        // A few rows at each end so the stream's start and end state are always visible, plus a capped
        // sample of the rows that are actually worth looking at.
        foreach (var row in rows.Take(settings.EventContextRows))
        {
            keep.Add(row.Number);
        }

        foreach (var row in rows.TakeLast(2))
        {
            keep.Add(row.Number);
        }

        foreach (var row in rows.Where(r => r.Notable).Take(settings.MaxNotableRows))
        {
            keep.Add(row.Number);
        }

        return rows.Where(r => keep.Contains(r.Number)).ToList();
    }
}

public static class Summary
{
    /// <summary>Prints the side-by-side comparison and returns whether every scenario met its expectation.</summary>
    public static bool Print(IReadOnlyList<ScenarioOutcome> outcomes, SimulationSettings settings)
    {
        Console.WriteLine();
        Console.WriteLine(new string('=', 100));
        Console.WriteLine("SUMMARY");
        Console.WriteLine(new string('=', 100));
        Console.WriteLine();

        var table = new List<string[]>
        {
            new[]
            {
                "Scenario", "Attempts", "Conflicts", "Lost", "Final amount", "Final ver", "Dup ver",
                "Consumer by clock", "Consumer by version",
            },
        };

        foreach (var outcome in outcomes)
        {
            table.Add([
                outcome.Run.Scenario.Name,
                outcome.Run.TotalAttempts.ToString(),
                outcome.Run.Conflicts.ToString(),
                outcome.Validation.LostUpdates.ToString(),
                outcome.Run.FinalAmount.ToString("F2"),
                outcome.Run.FinalVersion.ToString(),
                outcome.Validation.DuplicateVersionCount.ToString(),
                Guard(outcome.Ordering, o => o.Timestamp, luckAware: true),
                Guard(outcome.Ordering, o => o.Version, luckAware: false),
            ]);
        }

        Table.Write(table, [false, true, true, true, true, true, true, false, false], indent: "  ");
        Console.WriteLine();

        // Only worth comparing the cost of the scenarios that actually got the right answer.
        var correct = outcomes.Where(o => !o.Run.Scenario.ExpectsLostUpdates).ToList();
        var cheapest = correct.MinBy(o => o.Run.TotalAttempts);
        var dearest = correct.MaxBy(o => o.Run.TotalAttempts);

        if (cheapest is not null && dearest is not null && cheapest != dearest)
        {
            Console.WriteLine($"  Cost of being correct: {dearest.Run.Scenario.Name} needed {dearest.Run.TotalAttempts} attempts to land "
                + $"{settings.Workers} writes, {cheapest.Run.Scenario.Name} needed {cheapest.Run.TotalAttempts}.");
            Console.WriteLine();
        }

        var missed = outcomes.Where(o => !o.MetExpectation).ToList();

        if (missed.Count == 0)
        {
            Console.WriteLine($"  RESULT: all {outcomes.Count} scenario(s) behaved as expected.");
            return true;
        }

        Console.WriteLine($"  RESULT: {missed.Count} scenario(s) did not behave as expected: "
            + string.Join(", ", missed.Select(o => o.Run.Scenario.Name)));

        foreach (var outcome in missed)
        {
            Console.WriteLine(outcome.Run.Scenario.ExpectsLostUpdates
                ? $"    {outcome.Run.Scenario.Name}: expected lost updates, found none. The workers happened not to "
                  + "interleave - raise Simulation:Workers and run it again."
                : $"    {outcome.Run.Scenario.Name}: expected a clean run, got one or more failed checks.");
        }

        return false;
    }

    private static string Guard(
        ConsumerOrderingDemo.Result? ordering,
        Func<ConsumerOrderingDemo.Result, ConsumerOrderingDemo.GuardResult> select,
        bool luckAware)
    {
        if (ordering is null)
        {
            return "unorderable";
        }

        var guard = select(ordering);

        if (!guard.MatchesSourceOfTruth)
        {
            return $"{ordering.SourceVersion - guard.FinalVersion} behind";
        }

        // A clock guard can land on the right value while the stream still arrived out of order - it only
        // needs the newest event to happen to carry the latest reading. Say so, or the summary row reads
        // as though ordering by a timestamp were sound.
        return luckAware && ordering.TimestampInversions > 0 ? "exact (luck)" : "exact";
    }
}

internal static class Table
{
    /// <summary>Writes rows as an aligned text table. The first row is the header.</summary>
    public static void Write(IReadOnlyList<string[]> rows, IReadOnlyList<bool> rightAlign, string indent)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var columns = rows[0].Length;
        var widths = new int[columns];

        foreach (var row in rows)
        {
            for (var i = 0; i < columns; i++)
            {
                widths[i] = Math.Max(widths[i], row[i].Length);
            }
        }

        foreach (var row in rows)
        {
            var cells = row.Select((cell, i) => rightAlign[i] ? cell.PadLeft(widths[i]) : cell.PadRight(widths[i]));
            Console.WriteLine((indent + string.Join("  ", cells)).TrimEnd());
        }
    }
}

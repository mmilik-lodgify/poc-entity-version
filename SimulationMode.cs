namespace EventStorePoc;

/// <summary>Which variant of the write path the simulation exercises.</summary>
public enum SimulationMode
{
    /// <summary>
    /// The problem. <see cref="Balance.Version"/> is a plain column: workers still read it, add one and
    /// write it back, but the UPDATE matches on the primary key alone, so two workers that read the same
    /// version both write it back and the last one silently wins.
    /// </summary>
    Broken,

    /// <summary>
    /// The fix. <see cref="Balance.Version"/> is mapped as an EF Core concurrency token, so every UPDATE
    /// carries "AND Version = @originalVersion", a losing writer affects 0 rows and is rejected and retried.
    /// </summary>
    Fixed,
}

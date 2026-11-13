namespace EventStorePoc;

public class Balance
{
    public int Id { get; set; }
    public decimal Amount { get; set; }

    /// <summary>Optimistic concurrency token, incremented by the application on every change.</summary>
    public int Version { get; set; }
}

/// <summary>Snapshot of the balance state at the moment the event was produced (mapped as an EF complex type).</summary>
public record BalanceSnapshot
{
    public int Id { get; init; }
    public decimal Amount { get; init; }
}

public class BalanceSnapshotEvent
{
    public Guid Id { get; set; }
    public int BalanceId { get; set; }
    public required BalanceSnapshot BalanceSnapshot { get; set; }

    /// <summary>Same value as <see cref="Balance.Version"/> after the change that produced this event.</summary>
    public int Version { get; set; }
}

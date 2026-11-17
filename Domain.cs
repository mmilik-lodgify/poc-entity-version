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

    /// <summary>
    /// Publish time read from the clock of the worker (pod) that produced the event. This is the field a
    /// consumer would reach for to decide whether a message is newer than the state it already has - and
    /// the one <see cref="ConsumerOrderingDemo"/> shows cannot do the job: pod clocks drift apart, two
    /// writes can land in the same millisecond, and nothing ties the value to the order the database committed.
    /// </summary>
    public DateTime PublishedAtUtc { get; set; }
}

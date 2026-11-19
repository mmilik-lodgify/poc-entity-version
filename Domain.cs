namespace EventStorePoc;

/// <summary>
/// The balance types below are deliberately identical in shape. The only thing that differs between them is
/// how the write is protected, configured per type in <see cref="AppDbContext.OnModelCreating"/>.
///
/// Keeping them as three separate entity types - rather than one type remapped at startup - is what lets
/// every scenario run in a single launch, against the same database, and be compared side by side at the end.
/// </summary>
public interface IBalance
{
    int Id { get; set; }
    decimal Amount { get; set; }
    int Version { get; set; }
}

/// <summary>
/// The problem. Version is an ordinary column, so EF emits
/// <c>UPDATE ... WHERE Id = @id</c> and two workers that read the same version both write it back.
/// </summary>
public class UnguardedBalance : IBalance
{
    public int Id { get; set; }
    public decimal Amount { get; set; }
    public int Version { get; set; }
}

/// <summary>
/// The solution. Version is an EF concurrency token, so EF emits
/// <c>UPDATE ... WHERE Id = @id AND Version = @oldV</c>, a stale writer updates 0 rows and is retried.
/// </summary>
public class GuardedBalance : IBalance
{
    public int Id { get; set; }
    public decimal Amount { get; set; }
    public int Version { get; set; }
}

/// <summary>
/// The alternative. Never read-modify-write at all: one statement does the arithmetic and the increment
/// inside the database and hands back the new version, so there is nothing to lose and nothing to retry.
/// This row is never updated through EF - see <c>ScenarioRunner.IncrementAsync</c>.
/// </summary>
public class AtomicBalance : IBalance
{
    public int Id { get; set; }
    public decimal Amount { get; set; }
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

    /// <summary>
    /// Which scenario produced this event. All scenarios share one event table on purpose: the event is
    /// identical in each of them, and only the protection on the balance row differs.
    /// </summary>
    public required string Scenario { get; set; }

    public int BalanceId { get; set; }
    public required BalanceSnapshot BalanceSnapshot { get; set; }

    /// <summary>Same value as the balance's Version after the change that produced this event.</summary>
    public int Version { get; set; }

    /// <summary>
    /// Publish time read from the clock of the worker (pod) that produced the event. This is the field a
    /// consumer would reach for to decide whether a message is newer than the state it already has - and the
    /// one <see cref="ConsumerOrderingDemo"/> shows cannot do the job: pod clocks drift apart, two writes can
    /// land in the same millisecond, and nothing ties the value to the order the database committed.
    /// </summary>
    public DateTime PublishedAtUtc { get; set; }

    /// <summary>
    /// Which worker produced the event, and which of its attempts succeeded. Simulation metadata rather than
    /// domain data - it is here so the printed event log can show who wrote what and what the retrying cost.
    /// </summary>
    public int WorkerId { get; set; }

    public int Attempt { get; set; }
}

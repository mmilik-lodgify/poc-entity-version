namespace EventStorePoc;

/// <summary>
/// The balance types below are deliberately identical in shape, and two of the three are mapped identically
/// as well (see <see cref="AppDbContext.OnModelCreating"/>). That is the point: every write path here is
/// correct, so the write is not the variable under test. What differs is what the resulting event publishes
/// for a consumer to order the stream by.
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
/// The problem - and note that it is not a write problem. This row is guarded exactly like
/// <see cref="GuardedBalance"/>, token and retry loop included, so it never loses an update. What its
/// scenario does differently is downstream: the event it publishes carries only the pod's wall-clock time,
/// leaving a consumer nothing else to order the stream by. The realistic shape of this is a service that
/// uses optimistic concurrency for write safety and then publishes a timestamp, because a SQL Server
/// <c>rowversion</c> is not something you would hand to a consumer as an ordering field.
/// </summary>
public class TimestampBalance : IBalance
{
    public int Id { get; set; }
    public decimal Amount { get; set; }
    public int Version { get; set; }
}

/// <summary>
/// The solution. The same guarded write, publishing the version it produced. Version is an EF concurrency
/// token, so EF emits <c>UPDATE ... WHERE Id = @id AND Version = @oldV</c>, a stale writer updates 0 rows
/// and is retried - which is what makes the published version unique, and therefore worth ordering by.
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
    /// Which scenario produced this event. All scenarios share one event table on purpose: the stored row is
    /// the same shape in each of them, and what differs is which of its fields the scenario's consumers are
    /// given to order by.
    /// </summary>
    public required string Scenario { get; set; }

    public int BalanceId { get; set; }
    public required BalanceSnapshot BalanceSnapshot { get; set; }

    /// <summary>
    /// The version the write produced: the entity's own commit sequence, and the field a consumer can trust.
    /// The TimestampOnly scenario models a service that does not publish it - the value is still recorded
    /// here, because it is the ground truth the run needs in order to prove what the timestamp gets wrong,
    /// but nothing downstream in that scenario is allowed to read it.
    /// See <see cref="ScenarioDefinition.ConsumerOrdersBy"/>.
    /// </summary>
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

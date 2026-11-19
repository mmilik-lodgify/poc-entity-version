namespace EventStorePoc;

/// <summary>How much of the per-scenario event log to print.</summary>
public enum ShowEvents
{
    /// <summary>Skip the event log entirely.</summary>
    None,

    /// <summary>A few leading rows for context, plus every row that is worth looking at.</summary>
    Anomalies,

    /// <summary>Every row.</summary>
    All,
}

public sealed class SimulationSettings
{
    /// <summary>
    /// Which scenarios to run, comma separated and in the order given, or "all".
    /// e.g. <c>--Simulation:Scenarios TimestampOnly,Guarded</c>
    /// </summary>
    public string Scenarios { get; init; } = "all";

    public int BalanceId { get; init; } = 1;
    public decimal Decrement { get; init; } = 10m;

    /// <summary>
    /// The starting balance, derived so that every worker can take its share and a correct run ends on
    /// exactly zero whatever <see cref="Workers"/> is. Money left in the account is then a lost write,
    /// visible without doing any arithmetic.
    /// </summary>
    public decimal InitialAmount => Workers * Decrement;
    public int Workers { get; init; } = 10;
    public int MaxAttemptsPerWorker { get; init; } = 1000;

    /// <summary>How far the simulated pod clocks drift apart, in milliseconds (+/- this value).</summary>
    public int ClockSkewMs { get; init; } = 1000;

    /// <summary>How many positions redelivery may displace a message from its publish order.</summary>
    public int DeliveryJitter { get; init; } = 5;

    /// <summary>Seed for the clock skew and for the delivery reordering, so a run can be reproduced.</summary>
    public int Seed { get; init; } = 1234;

    /// <summary>How much of the event log to print.</summary>
    public ShowEvents ShowEvents { get; init; } = ShowEvents.All;

    /// <summary>How many leading rows the <see cref="ShowEvents.Anomalies"/> view always shows for context.</summary>
    public int EventContextRows { get; init; } = 5;

    /// <summary>How many notable rows that view will print before it stops listing them.</summary>
    public int MaxNotableRows { get; init; } = 10;
}

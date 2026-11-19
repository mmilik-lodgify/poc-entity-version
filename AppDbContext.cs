using Microsoft.EntityFrameworkCore;

namespace EventStorePoc;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<TimestampBalance> TimestampBalances => Set<TimestampBalance>();
    public DbSet<GuardedBalance> GuardedBalances => Set<GuardedBalance>();
    public DbSet<AtomicBalance> AtomicBalances => Set<AtomicBalance>();
    public DbSet<BalanceSnapshotEvent> BalanceSnapshotEvents => Set<BalanceSnapshotEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // The three mappings are written out rather than shared through a helper, so that what is and is not
        // different between them can be read at a glance. The first two are identical on purpose: both write
        // safely, and the difference between those scenarios is not here at all - it is in which field the
        // published event gives a consumer to order by. Only the third mapping differs, and only because its
        // row is never written through EF.

        modelBuilder.Entity<TimestampBalance>(b =>
        {
            b.ToTable("TimestampBalances");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedNever();
            b.Property(x => x.Amount).HasPrecision(18, 2);

            // Identical to GuardedBalance below, down to this line: the write is protected, so this scenario
            // loses nothing. Its events simply do not carry the version this token produces.
            b.Property(x => x.Version).IsConcurrencyToken();
        });

        modelBuilder.Entity<GuardedBalance>(b =>
        {
            b.ToTable("GuardedBalances");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedNever();
            b.Property(x => x.Amount).HasPrecision(18, 2);

            // The key part of the POC. EF appends "AND Version = @originalVersion" to the UPDATE:
            //     UPDATE GuardedBalances SET Amount = @a, Version = @newV WHERE Id = @id AND Version = @oldV
            // If another worker already bumped the version, 0 rows are affected and EF throws
            // DbUpdateConcurrencyException, rolling back the whole SaveChanges transaction (incl. the event insert).
            b.Property(x => x.Version).IsConcurrencyToken();
        });

        modelBuilder.Entity<AtomicBalance>(b =>
        {
            b.ToTable("AtomicBalances");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedNever();
            b.Property(x => x.Amount).HasPrecision(18, 2);

            // No concurrency token, and none needed: this row is never read-modify-written. A single
            // statement does the arithmetic in the database and returns the version it produced, so EF
            // never issues an UPDATE for it at all.
        });

        modelBuilder.Entity<BalanceSnapshotEvent>(e =>
        {
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).ValueGeneratedNever();
            e.Property(x => x.Scenario).HasMaxLength(32);
            e.HasIndex(x => new { x.Scenario, x.BalanceId });
            e.ComplexProperty(x => x.BalanceSnapshot, s =>
            {
                s.Property(x => x.Amount).HasPrecision(18, 2);
            });

            // Intentionally NO unique index on (Scenario, BalanceId, Version): every scenario has to prove
            // that its write path alone produces unique versions, and an index that rejected a duplicate
            // would be doing that work instead of the thing under test. In production you would add it - it
            // is the only thing that catches a write path which bypasses the tracked entity (ExecuteUpdate,
            // raw SQL, bulk backfills), where a concurrency token does not apply at all.
        });
    }
}

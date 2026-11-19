using Microsoft.EntityFrameworkCore;

namespace EventStorePoc;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<UnguardedBalance> UnguardedBalances => Set<UnguardedBalance>();
    public DbSet<GuardedBalance> GuardedBalances => Set<GuardedBalance>();
    public DbSet<AtomicBalance> AtomicBalances => Set<AtomicBalance>();
    public DbSet<BalanceSnapshotEvent> BalanceSnapshotEvents => Set<BalanceSnapshotEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // The three mappings are written out rather than shared through a helper, because the single line of
        // difference between them is the entire subject of this POC and is worth being able to read at a glance.

        modelBuilder.Entity<UnguardedBalance>(b =>
        {
            b.ToTable("UnguardedBalances");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedNever();
            b.Property(x => x.Amount).HasPrecision(18, 2);

            // Version is left as an ordinary column, so EF emits
            //     UPDATE UnguardedBalances SET Amount = @a, Version = @newV WHERE Id = @id
            // which always affects 1 row. Nothing notices that @oldV was already stale when it was read.
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

            // Intentionally NO unique index on (Scenario, BalanceId, Version): the guarded scenarios have to
            // prove that their write path alone prevents duplicated versions, and the unguarded one has to be
            // free to write the duplicates it produces so they can be counted. In production you would add that
            // index - it is the only thing that catches a write path which bypasses the tracked entity
            // (ExecuteUpdate, raw SQL, bulk backfills), where a concurrency token does not apply at all.
        });
    }
}

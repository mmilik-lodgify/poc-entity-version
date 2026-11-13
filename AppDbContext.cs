using Microsoft.EntityFrameworkCore;

namespace EventStorePoc;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Balance> Balances => Set<Balance>();
    public DbSet<BalanceSnapshotEvent> BalanceSnapshotEvents => Set<BalanceSnapshotEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Balance>(b =>
        {
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedNever();
            b.Property(x => x.Amount).HasPrecision(18, 2);

            // The key part of the POC: EF adds "AND Version = @originalVersion" to the UPDATE's WHERE clause.
            // If another worker already bumped the version, 0 rows are affected and EF throws
            // DbUpdateConcurrencyException, rolling back the whole SaveChanges transaction (incl. the event insert).
            b.Property(x => x.Version).IsConcurrencyToken();
        });

        modelBuilder.Entity<BalanceSnapshotEvent>(e =>
        {
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).ValueGeneratedNever();
            e.HasIndex(x => x.BalanceId);
            e.ComplexProperty(x => x.BalanceSnapshot, s =>
            {
                s.Property(x => x.Amount).HasPrecision(18, 2);
            });
            // Intentionally NO unique index on (BalanceId, Version): the POC must prove that the
            // concurrency token alone prevents duplicated versions. In production you would add it as a safety net.
        });
    }
}

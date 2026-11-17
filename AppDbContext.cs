using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EventStorePoc;

public class AppDbContext(DbContextOptions<AppDbContext> options, SimulationMode mode) : DbContext(options)
{
    public SimulationMode Mode { get; } = mode;

    public DbSet<Balance> Balances => Set<Balance>();
    public DbSet<BalanceSnapshotEvent> BalanceSnapshotEvents => Set<BalanceSnapshotEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Balance>(b =>
        {
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedNever();
            b.Property(x => x.Amount).HasPrecision(18, 2);

            if (Mode == SimulationMode.Fixed)
            {
                // The key part of the POC: EF adds "AND Version = @originalVersion" to the UPDATE's WHERE clause.
                // If another worker already bumped the version, 0 rows are affected and EF throws
                // DbUpdateConcurrencyException, rolling back the whole SaveChanges transaction (incl. the event insert).
                //
                //   UPDATE Balances SET Amount = @a, Version = @newV WHERE Id = @id AND Version = @oldV
                b.Property(x => x.Version).IsConcurrencyToken();
            }

            // In Broken mode the same property is left as an ordinary column, so the statement becomes
            //
            //   UPDATE Balances SET Amount = @a, Version = @newV WHERE Id = @id
            //
            // which always affects 1 row. Nothing notices that @oldV was already stale when it was read.
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

            // Intentionally NO unique index on (BalanceId, Version): Fixed mode must prove that the
            // concurrency token alone prevents duplicated versions, and Broken mode must be free to write
            // the duplicates it produces so they can be counted. In production you would add that index -
            // it is the only thing that catches a write path which bypasses the tracked entity
            // (ExecuteUpdate, raw SQL, bulk backfills), where the concurrency token does not apply at all.
        });
    }
}

/// <summary>
/// EF Core caches one model per context type. The two simulation modes map <see cref="Balance.Version"/>
/// differently, so the mode has to be part of the model cache key.
/// </summary>
public sealed class ModeAwareModelCacheKeyFactory : IModelCacheKeyFactory
{
    public object Create(DbContext context, bool designTime)
        => (context.GetType(), ((AppDbContext)context).Mode, designTime);
}

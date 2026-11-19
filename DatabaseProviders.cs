using DotNet.Testcontainers.Containers;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace EventStorePoc;

public enum DatabaseProvider
{
    Postgres,
    SqlServer,
}

/// <summary>Starts a database in a Testcontainer and knows how to point EF Core at it.</summary>
public interface IDatabaseContainer : IAsyncDisposable
{
    string Name { get; }
    Task StartAsync();
    DbContextOptions<AppDbContext> CreateDbContextOptions();
}

public static class DatabaseContainerFactory
{
    public static IDatabaseContainer Create(DatabaseProvider provider) => provider switch
    {
        DatabaseProvider.Postgres => new PostgresDatabaseContainer(),
        DatabaseProvider.SqlServer => new SqlServerDatabaseContainer(),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, "Unsupported provider"),
    };
}

public sealed class PostgresDatabaseContainer : IDatabaseContainer
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine")
        // Default max_connections (100) is too tight once Simulation:Workers is raised into the hundreds.
        .WithCommand("-c", "max_connections=300")
        // The Docker lifecycle chatter is noise in output meant to be read in a review or pasted into
        // a document. The run prints its own progress line instead.
        .WithLogger(NullLogger.Instance)
        .Build();

    public string Name => "PostgreSQL";

    public Task StartAsync() => _container.StartAsync();

    public DbContextOptions<AppDbContext> CreateDbContextOptions()
    {
        var cs = new NpgsqlConnectionStringBuilder(_container.GetConnectionString()) { MaxPoolSize = 200 };
        return new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql(cs.ConnectionString)
            .Options;
    }

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}

public sealed class SqlServerDatabaseContainer : IDatabaseContainer
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
        .WithLogger(NullLogger.Instance)
        .Build();

    public string Name => "SQL Server";

    public Task StartAsync() => _container.StartAsync();

    public DbContextOptions<AppDbContext> CreateDbContextOptions()
    {
        var cs = new SqlConnectionStringBuilder(_container.GetConnectionString())
        {
            MaxPoolSize = 200,
            InitialCatalog = "EventStorePoc",
        };
        return new DbContextOptionsBuilder<AppDbContext>()
            .UseSqlServer(cs.ConnectionString)
            .Options;
    }

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}

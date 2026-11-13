using DotNet.Testcontainers.Containers;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
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
        // Default max_connections (100) is too tight for 100 concurrent workers + pool overhead.
        .WithCommand("-c", "max_connections=300")
        .Build();

    public string Name => "PostgreSQL";

    public Task StartAsync() => _container.StartAsync();

    public DbContextOptions<AppDbContext> CreateDbContextOptions()
    {
        var cs = new NpgsqlConnectionStringBuilder(_container.GetConnectionString()) { MaxPoolSize = 200 };
        return new DbContextOptionsBuilder<AppDbContext>().UseNpgsql(cs.ConnectionString).Options;
    }

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}

public sealed class SqlServerDatabaseContainer : IDatabaseContainer
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();

    public string Name => "SQL Server";

    public Task StartAsync() => _container.StartAsync();

    public DbContextOptions<AppDbContext> CreateDbContextOptions()
    {
        var cs = new SqlConnectionStringBuilder(_container.GetConnectionString())
        {
            MaxPoolSize = 200,
            InitialCatalog = "EventStorePoc",
        };
        return new DbContextOptionsBuilder<AppDbContext>().UseSqlServer(cs.ConnectionString).Options;
    }

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}

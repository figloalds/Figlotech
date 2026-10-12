using System.Data;
using System.Data.Common;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.MySqlDataAccessor;
using Figlotech.BDados.PgSQLDataAccessor;
using MySqlConnector;
using Npgsql;
using Xunit;
using static Figlotech.BDados.Tests.BDadosBackupReliabilityTests;
using static Figlotech.BDados.Tests.BDadosBackupTests;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupProviderIntegrationTests {
        [PostgreSqlBackupFact]
        public Task PostgreSqlPreservesIdentityDefaultsRelationsAndRollback() => ExerciseProviderAsync(true);

        [MySqlBackupFact]
        public Task MySqlPreservesIdentityDefaultsRelationsAndRollback() => ExerciseProviderAsync(false);

        private static async Task ExerciseProviderAsync(bool postgres) {
            await using var database = await ProviderDatabase.CreateAsync(postgres);
            string identity = postgres ? "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY" : "BIGINT AUTO_INCREMENT PRIMARY KEY";
            string time = postgres ? "TIMESTAMPTZ" : "DATETIME(6)";
            await database.ExecuteAsync($"CREATE TABLE TimedRow (Id {identity}, Text TEXT, CreatedAt {time}, UpdatedAt {time})");
            await database.ExecuteAsync("CREATE TABLE Parent (Id BIGINT PRIMARY KEY, Text TEXT, ChildId BIGINT)");
            await database.ExecuteAsync("CREATE TABLE Child (Id BIGINT PRIMARY KEY, Text TEXT, ParentId BIGINT REFERENCES Parent(Id))");
            if (!postgres) await database.ExecuteAsync("ALTER TABLE Child ADD FOREIGN KEY (ParentId) REFERENCES Parent(Id)");
            await database.ExecuteAsync("ALTER TABLE Parent ADD FOREIGN KEY (ChildId) REFERENCES Child(Id)");
            await database.ExecuteAsync("CREATE TABLE WithDefaults (Id BIGINT PRIMARY KEY, Text TEXT, Added VARCHAR(80) NOT NULL DEFAULT 'database', Number INTEGER NOT NULL DEFAULT 7)");
            var timestamp = new DateTime(2001, 2, 3, 4, 5, 6, DateTimeKind.Utc);
            using var source = new MemoryStream(await ArchiveAsync(new Child { Id = 8, ParentId = 42 }, new Parent { Id = 42, ChildId = 8 },
                new TimedRow { Id = 99, Text = "saved", CreatedAt = timestamp, UpdatedAt = timestamp }));
            var backup = new BDadosBackup(database.Accessor);
            await backup.RestoreAsync(source, new[] { typeof(Parent), typeof(Child), typeof(TimedRow) });
            Assert.Equal(99L, Convert.ToInt64(await database.ScalarAsync("SELECT Id FROM TimedRow")));
            Assert.Equal(timestamp.Ticks, ((DateTime)(await database.ScalarAsync("SELECT UpdatedAt FROM TimedRow"))!).Ticks);
            Assert.Equal(42L, Convert.ToInt64(await database.ScalarAsync("SELECT ParentId FROM Child")));
            if (postgres) {
                Assert.Equal(100L, Convert.ToInt64(await database.ScalarAsync("INSERT INTO TimedRow (Text) VALUES ('next') RETURNING Id")));
            } else {
                await database.ExecuteAsync("INSERT INTO TimedRow (Text) VALUES ('next')");
                Assert.Equal(100L, Convert.ToInt64(await database.ScalarAsync("SELECT LAST_INSERT_ID()")));
                using var zero = new MemoryStream(await ArchiveAsync(new TimedRow { Id = 0, Text = "zero", CreatedAt = timestamp }));
                await backup.RestoreAsync(zero, new[] { typeof(TimedRow) });
                Assert.Equal(0L, Convert.ToInt64(await database.ScalarAsync("SELECT Id FROM TimedRow WHERE Text = 'zero'")));
            }
            using var missing = new MemoryStream(await ArchiveAsync(new Row { Id = 7 }));
            await backup.RestoreAsync(missing, new[] { typeof(WithDefaults) });
            Assert.Equal("database", await database.ScalarAsync("SELECT Added FROM WithDefaults"));
            Assert.Equal(7L, Convert.ToInt64(await database.ScalarAsync("SELECT Number FROM WithDefaults")));
            using var orphan = new MemoryStream(await ArchiveAsync(new Child { Id = 9, ParentId = 404 }));
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => backup.RestoreAsync(orphan, new[] { typeof(Child) }));
            Assert.Contains("Foreign-key violation", failure.ToString());
            Assert.Equal(1L, Convert.ToInt64(await database.ScalarAsync("SELECT COUNT(*) FROM Child")));
            var corrupt = await ArchiveAsync(new TimedRow { Id = 1000, CreatedAt = timestamp });
            corrupt[^1] ^= 1;
            await Assert.ThrowsAnyAsync<Exception>(() => backup.RestoreAsync(new MemoryStream(corrupt), new[] { typeof(TimedRow) }));
            Assert.Equal(0L, Convert.ToInt64(await database.ScalarAsync("SELECT COUNT(*) FROM TimedRow WHERE Id = 1000")));
            if (postgres) Assert.Equal(101L, Convert.ToInt64(await database.ScalarAsync("INSERT INTO TimedRow (Text) VALUES ('after failure') RETURNING Id")));
        }

        [PostgreSqlBackupFact]
        public async Task PostgreSqlValidatesCompositeMatchFullAndPreservesSequenceAfterMerge() {
            await using var database = await ProviderDatabase.CreateAsync(true);
            await database.ExecuteAsync("CREATE TABLE CompositeParent (A BIGINT, B BIGINT, UNIQUE(A,B)); INSERT INTO CompositeParent VALUES(1,2)");
            await database.ExecuteAsync("CREATE TABLE CompositeChild (Id BIGSERIAL PRIMARY KEY, Text TEXT, A BIGINT, B BIGINT, FOREIGN KEY(A,B) REFERENCES CompositeParent(A,B) MATCH FULL)");
            var backup = new BDadosBackup(database.Accessor);
            using var valid = new MemoryStream(await ArchiveAsync(new CompositeChild { Id = 42, A = 1, B = 2 }));
            await backup.RestoreAsync(valid, new[] { typeof(CompositeChild) });
            using var partialNull = new MemoryStream(await ArchiveAsync(new CompositeChild { Id = 43, A = 1, B = null }));
            await Assert.ThrowsAnyAsync<Exception>(() => backup.RestoreAsync(partialNull, new[] { typeof(CompositeChild) }));
            Assert.Equal(1L, await database.ScalarAsync("SELECT COUNT(*) FROM CompositeChild"));
            await database.ExecuteAsync("SELECT setval(pg_get_serial_sequence('compositechild','id'), 500, true)");
            valid.Position = 0;
            await backup.RestoreAsync(valid, new[] { typeof(CompositeChild) });
            Assert.Equal(501L, await database.ScalarAsync("INSERT INTO CompositeChild DEFAULT VALUES RETURNING Id"));
        }

        public class CompositeChild : Row {
            [Field] public long? A { get; set; }
            [Field] public long? B { get; set; }
        }

        [PostgreSqlBackupFact]
        public async Task PostgreSqlRejectsSilentDecimalRounding() {
            await using var database = await ProviderDatabase.CreateAsync(true);
            await database.ExecuteAsync("CREATE TABLE AmountRow (Id BIGINT PRIMARY KEY, Text TEXT, Amount NUMERIC(10,2))");
            using var archive = new MemoryStream(await ArchiveAsync(new AmountRow { Id = 42, Amount = 12.345m }));
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(database.Accessor).RestoreAsync(archive, new[] { typeof(AmountRow) }));
            Assert.Contains("Database changed restored value", failure.ToString());
            Assert.Equal(0L, await database.ScalarAsync("SELECT COUNT(*) FROM AmountRow"));
        }

        public class AmountRow : Row { [Field] public decimal Amount { get; set; } }

        public sealed class PostgreSqlBackupFactAttribute : FactAttribute {
            public PostgreSqlBackupFactAttribute() {
                if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("FIGLOTECH_TEST_POSTGRES"))) Skip = "Set FIGLOTECH_TEST_POSTGRES to an isolated test server connection string.";
            }
        }
        public sealed class MySqlBackupFactAttribute : FactAttribute {
            public MySqlBackupFactAttribute() {
                if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("FIGLOTECH_TEST_MYSQL"))) Skip = "Set FIGLOTECH_TEST_MYSQL to an isolated test server connection string.";
            }
        }

        private sealed class ProviderDatabase : IAsyncDisposable {
            private DbConnection _admin = null!;
            private DbConnection _connection = null!;
            private string _name = null!;
            private bool _postgres;
            internal RdbmsDataAccessor Accessor { get; private set; } = null!;
            internal static async Task<ProviderDatabase> CreateAsync(bool postgres) {
                var database = new ProviderDatabase { _postgres = postgres, _name = "bdados_test_" + Guid.NewGuid().ToString("N") };
                string configured = Environment.GetEnvironmentVariable(postgres ? "FIGLOTECH_TEST_POSTGRES" : "FIGLOTECH_TEST_MYSQL")!;
                database._admin = postgres ? new NpgsqlConnection(configured) : new MySqlConnection(configured);
                await database._admin.OpenAsync();
                await using (var command = database._admin.CreateCommand()) {
                    command.CommandText = $"CREATE {(postgres ? "SCHEMA" : "DATABASE")} {database._name}";
                    await command.ExecuteNonQueryAsync();
                }
                string connectionString = postgres
                    ? new NpgsqlConnectionStringBuilder(configured) { SearchPath = database._name, Pooling = false }.ToString()
                    : new MySqlConnectionStringBuilder(configured) { Database = database._name, Pooling = false }.ToString();
                database._connection = postgres ? new NpgsqlConnection(connectionString) : new MySqlConnection(connectionString);
                await database._connection.OpenAsync();
                database.Accessor = new RdbmsDataAccessor(new TestProvider(connectionString, postgres));
                return database;
            }
            internal async Task ExecuteAsync(string sql) {
                await using var command = _connection.CreateCommand();
                command.CommandText = sql;
                await command.ExecuteNonQueryAsync();
            }
            internal async Task<object?> ScalarAsync(string sql) {
                await using var command = _connection.CreateCommand();
                command.CommandText = sql;
                return await command.ExecuteScalarAsync();
            }
            public async ValueTask DisposeAsync() {
                Accessor.Dispose();
                await _connection.DisposeAsync();
                await using var command = _admin.CreateCommand();
                command.CommandText = $"DROP {(_postgres ? "SCHEMA" : "DATABASE")} {_name}{(_postgres ? " CASCADE" : "")}";
                await command.ExecuteNonQueryAsync();
                await _admin.DisposeAsync();
            }
        }

        private sealed class TestProvider(string connectionString, bool postgres) : IRdbmsPluginAdapter {
            private readonly IRdbmsPluginAdapter _provider = postgres ? new PgSQLPlugin() : new MySqlPlugin();
            public IQueryGenerator QueryGenerator => _provider.QueryGenerator;
            public bool ContinuousConnection => false;
            public TimeSpan CommandTimeout => TimeSpan.FromSeconds(30);
            public TimeSpan ConnectTimeout => TimeSpan.FromSeconds(5);
            public int PoolSize => 2;
            public string SchemaName => "backup_test";
            public string DatabaseHost => "127.0.0.1";
            public string ConnectionString => connectionString;
            public IReadOnlyDictionary<string, string> InfoSchemaColumnsMap => _provider.InfoSchemaColumnsMap;
            public IDbConnection GetNewConnection() => postgres ? new NpgsqlConnection(connectionString) : new MySqlConnection(connectionString);
            public IDbConnection GetNewSchemalessConnection() => GetNewConnection();
            public void DiscardConnection(IDbConnection connection) => _provider.DiscardConnection(connection);
            public void SetConfiguration(IDictionary<string, object> settings) => throw new NotSupportedException();
            public object ProcessParameterValue(object value) => _provider.ProcessParameterValue(value);
        }
    }
}

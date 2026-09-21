using System;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.PgSQLDataAccessor;
using Npgsql;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class PgSQLDatabaseCreationTests {
        [Theory]
        [InlineData("application")]
        [InlineData("MixedCase")]
        [InlineData("my-database")]
        [InlineData("my database")]
        [InlineData("select")]
        [InlineData("my\"database")]
        [InlineData("db\"; DROP DATABASE other; --")]
        [InlineData("db@tenant")]
        public void CreateDatabasePreservesNameAsOneQuotedIdentifier(string databaseName) {
            var query = new PgSQLQueryGenerator().CreateDatabase(databaseName);
            using var builder = new NpgsqlCommandBuilder();

            Assert.Equal("CREATE DATABASE " + builder.QuoteIdentifier(databaseName), query.GetCommandText());
            Assert.Empty(query.GetParameters());
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" \t\r\n")]
        [InlineData("db\0name")]
        public async Task EnsureDatabaseRejectsInvalidNameBeforeOpeningConnection(string? databaseName) {
            var plugin = new PgSQLPlugin(new PgSQLPluginConfiguration {
                Database = databaseName,
                Host = "invalid host",
                User = "test"
            });
            using var accessor = new RdbmsDataAccessor(plugin);

            var exception = await Assert.ThrowsAnyAsync<ArgumentException>(async () => await accessor.EnsureDatabaseExistsAsync());

            Assert.Equal("schemaName", exception.ParamName);
            Assert.Contains("PgSQLPluginConfiguration.Database", exception.Message);
        }
    }
}

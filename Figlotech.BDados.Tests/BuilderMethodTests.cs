using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.SqliteDataAccessor;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Figlotech.BDados.Tests {
    public sealed class BuilderMethodTests {
        [Fact]
        public async Task GetJsonStringFromQueryAsyncWritesMappedTypedJsonValues() {
            using var accessor = CreateAccessor();
            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            await using DbCommand command = (DbCommand)await transaction.CreateCommandAsync();
            command.CommandText = "SELECT 42 AS Id, 'builder' AS Name, CAST(NULL AS INTEGER) AS OptionalCount, 'excluded' AS Unmapped";
            using var writer = new StringWriter();

            await accessor.GetJsonStringFromQueryAsync<JsonRow>(transaction, command, writer);

            using JsonDocument document = JsonDocument.Parse(writer.ToString());
            JsonElement row = Assert.Single(document.RootElement.EnumerateArray());
            Assert.Equal(JsonValueKind.Number, row.GetProperty(nameof(JsonRow.Id)).ValueKind);
            Assert.Equal(42L, row.GetProperty(nameof(JsonRow.Id)).GetInt64());
            Assert.Equal("builder", row.GetProperty(nameof(JsonRow.Name)).GetString());
            Assert.Equal(JsonValueKind.Null, row.GetProperty(nameof(JsonRow.OptionalCount)).ValueKind);
            Assert.False(row.TryGetProperty("Unmapped", out _));
        }

        [Fact]
        public void GetDataSetMaterializesTypedColumnsAndDbNullValues() {
            using var accessor = CreateAccessor();
            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null);
            using IDbCommand command = transaction.CreateCommand();
            command.CommandText = "SELECT 42 AS Id, 'builder' AS Name, CAST(NULL AS INTEGER) AS OptionalCount";

            DataSet dataSet = accessor.GetDataSet(transaction, command);

            DataTable table = Assert.Single(dataSet.Tables.Cast<DataTable>());
            Assert.Equal(new[] { "Id", "Name", "OptionalCount" }, table.Columns.Cast<DataColumn>().Select(column => column.ColumnName));
            Assert.Equal(typeof(long), table.Columns["Id"]!.DataType);
            Assert.Equal(typeof(string), table.Columns["Name"]!.DataType);
            Assert.Equal(typeof(byte[]), table.Columns["OptionalCount"]!.DataType);
            DataRow row = Assert.Single(table.Rows.Cast<DataRow>());
            Assert.Equal(42L, row["Id"]);
            Assert.Equal("builder", row["Name"]);
            Assert.Equal(DBNull.Value, row["OptionalCount"]);
        }

        private static RdbmsDataAccessor CreateAccessor() {
            return new RdbmsDataAccessor(new InMemorySqlitePlugin());
        }

        private sealed class JsonRow {
            public JsonRow() { }

            public long Id { get; set; }
            public string Name { get; set; } = string.Empty;
            public int? OptionalCount { get; set; }
        }

        private sealed class InMemorySqlitePlugin : IRdbmsPluginAdapter {
            public IQueryGenerator QueryGenerator { get; } = new SqliteQueryGenerator();
            public bool ContinuousConnection => true;
            public TimeSpan CommandTimeout => TimeSpan.FromSeconds(30);
            public TimeSpan ConnectTimeout => TimeSpan.FromSeconds(30);
            public int PoolSize => 1;
            public string SchemaName => "main";
            public string DatabaseHost => ":memory:";
            public string ConnectionString => "Data Source=:memory:";
            public IReadOnlyDictionary<string, string> InfoSchemaColumnsMap { get; } = new Dictionary<string, string>();

            public IDbConnection GetNewConnection() => new SqliteConnection(ConnectionString);
            public IDbConnection GetNewSchemalessConnection() => GetNewConnection();
            public void SetConfiguration(IDictionary<string, object> settings) { }
            public object ProcessParameterValue(object value) => value;
        }
    }
}

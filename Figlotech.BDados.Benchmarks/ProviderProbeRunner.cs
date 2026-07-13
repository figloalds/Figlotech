using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.SqliteDataAccessor;
using Figlotech.Data;
using Microsoft.Data.Sqlite;

namespace Figlotech.BDados.Benchmarks {
    public static class ProviderProbeRunner {
        public static void Run() {
            string databasePath = Path.Combine(Path.GetTempPath(), "figlotech-definitive-plan-" + Guid.NewGuid().ToString("N") + ".db");
            try {
                RunSqlite(databasePath);
                Console.WriteLine("PROVIDER_MYSQL=UNAVAILABLE (no deterministic local server configured)");
                Console.WriteLine("PROVIDER_POSTGRESQL=UNAVAILABLE (no deterministic local server configured)");
                Console.WriteLine("PROVIDER_PROBE_PASS");
            } finally {
                SqliteConnection.ClearAllPools();
                if (File.Exists(databasePath)) {
                    File.Delete(databasePath);
                }
            }
        }

        private static void RunSqlite(string databasePath) {
            var plugin = new BenchmarkSqlitePluginAdapter(databasePath);
            using var accessor = new RdbmsDataAccessor(plugin);
            BenchmarkMaterializationScenario scenario = BenchmarkScenarioFactory.Create(BenchmarkIdentifierKind.Int64, 8, 0);
            DefinitiveJoinPlan plan = scenario.Plan;
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            using (BDadosTransaction seedTransaction = accessor.CreateNewTransaction(CancellationToken.None)) {
                accessor.Execute(seedTransaction, Qb.Fmt(BuildCreateTableSql()));
                accessor.Execute(seedTransaction, Qb.Fmt(BuildInsertSql()));
                seedTransaction.Commit();
            }

            IQueryBuilder query = accessor.QueryGenerator.GenerateJoinQuery(plan, null);
            List<Int64BenchmarkRow> warmRows = ExecuteFrozenQuery(accessor, query, plan, materializer);
            if (warmRows.Count != BenchmarkScenarioFactory.RowsPerInvocation) {
                throw new InvalidOperationException("SQLite probe did not materialize the seeded row count.");
            }

            const int materializationIterations = 100;
            var stopwatch = Stopwatch.StartNew();
            int materialized = 0;
            for (int index = 0; index < materializationIterations; index++) {
                materialized += scenario.Materialize();
            }
            stopwatch.Stop();
            double materializationNsPerRow = ToNanosecondsPerRow(stopwatch.ElapsedTicks, materialized);

            const int endToEndIterations = 10;
            stopwatch.Restart();
            int queried = 0;
            for (int index = 0; index < endToEndIterations; index++) {
                queried += ExecuteFrozenQuery(accessor, query, plan, materializer).Count;
            }
            stopwatch.Stop();
            double endToEndNsPerRow = ToNanosecondsPerRow(stopwatch.ElapsedTicks, queried);

            Console.WriteLine("PROVIDER_SQLITE=AVAILABLE");
            Console.WriteLine($"SQLITE_ROWS={BenchmarkScenarioFactory.RowsPerInvocation}");
            Console.WriteLine($"SQLITE_MATERIALIZATION_NS_PER_ROW={materializationNsPerRow.ToString("F2", CultureInfo.InvariantCulture)}");
            Console.WriteLine($"SQLITE_END_TO_END_NS_PER_ROW={endToEndNsPerRow.ToString("F2", CultureInfo.InvariantCulture)}");
        }

        private static List<Int64BenchmarkRow> ExecuteFrozenQuery(
            RdbmsDataAccessor accessor,
            IQueryBuilder query,
            DefinitiveJoinPlan plan,
            CompiledAggregateMaterializerPlan materializer) {
            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None);
            using IDbCommand command = transaction.Connection.CreateCommand();
            command.CommandText = query.GetCommandText();
            return accessor.BuildAggregateListDirect<Int64BenchmarkRow>(transaction, command, plan, materializer, null);
        }

        private static string BuildCreateTableSql() {
            var sql = new StringBuilder("CREATE TABLE Int64BenchmarkRow (Id INTEGER NOT NULL PRIMARY KEY");
            for (int ordinal = 1; ordinal < 8; ordinal++) {
                sql.Append(", Value").Append(ordinal).Append(" INTEGER NULL");
            }
            return sql.Append(")").ToString();
        }

        private static string BuildInsertSql() {
            var sql = new StringBuilder("INSERT INTO Int64BenchmarkRow (Id");
            for (int ordinal = 1; ordinal < 8; ordinal++) {
                sql.Append(", Value").Append(ordinal);
            }
            sql.Append(") VALUES ");
            for (int row = 1; row <= BenchmarkScenarioFactory.RowsPerInvocation; row++) {
                if (row > 1) {
                    sql.Append(',');
                }
                sql.Append('(').Append(row);
                for (int ordinal = 1; ordinal < 8; ordinal++) {
                    sql.Append(',').Append(row + ordinal);
                }
                sql.Append(')');
            }
            return sql.ToString();
        }

        private static double ToNanosecondsPerRow(long elapsedTicks, int rows) {
            return elapsedTicks * (1_000_000_000.0 / Stopwatch.Frequency) / rows;
        }

        private sealed class BenchmarkSqlitePluginAdapter : IRdbmsPluginAdapter {
            public BenchmarkSqlitePluginAdapter(string databasePath) {
                DatabaseHost = databasePath;
                ConnectionString = $"Data Source={databasePath};Pooling=False";
            }

            public IQueryGenerator QueryGenerator { get; } = new SqliteQueryGenerator();
            public bool ContinuousConnection => false;
            public int CommandTimeout => 30;
            public int ConnectTimeout => 30;
            public int PoolSize => 1;
            public string SchemaName => "main";
            public string DatabaseHost { get; }
            public string ConnectionString { get; }
            public Dictionary<string, string> InfoSchemaColumnsMap { get; } = new Dictionary<string, string>();

            public IDbConnection GetNewConnection() {
                return new SqliteConnection(ConnectionString);
            }

            public IDbConnection GetNewSchemalessConnection() {
                return GetNewConnection();
            }

            public void SetConfiguration(IDictionary<string, object> settings) {
                throw new NotSupportedException("The deterministic benchmark adapter is immutable.");
            }

            public object ProcessParameterValue(object value) {
                return value;
            }
        }
    }
}

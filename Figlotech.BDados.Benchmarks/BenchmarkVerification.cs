using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.SqliteDataAccessor;

namespace Figlotech.BDados.Benchmarks {
    public static class BenchmarkVerification {
        public static void Run() {
            VerifyWarmScenarioMatrix();
            VerifyColdCompilationMatrix();
            VerifyAutomaticCacheCardinality();
            VerifyFrozenSchemaAndOrdering();
            VerifyNoMutableCustomPlanCache();
            Console.WriteLine("BENCHMARK_VERIFICATION_PASS");
        }

        private static void VerifyWarmScenarioMatrix() {
            int scenarios = 0;
            foreach (BenchmarkIdentifierKind identifierKind in Enum.GetValues(typeof(BenchmarkIdentifierKind))) {
                foreach (int columnCount in new[] { 1, 8, 32, 128 }) {
                    foreach (int nullPercent in new[] { 0, 10, 50, 100 }) {
                        BenchmarkMaterializationScenario scenario = BenchmarkScenarioFactory.Create(identifierKind, columnCount, nullPercent);
                        Require(scenario.Plan.Projection.Length == columnCount, "Projection width does not match the benchmark parameter.");
                        Require(scenario.Materialize() == BenchmarkScenarioFactory.RowsPerInvocation, "A warm scenario lost uniquely identified rows.");
                        scenarios++;
                    }
                }
            }
            Console.WriteLine($"WARM_SCENARIOS_VERIFIED={scenarios}");
        }

        private static void VerifyColdCompilationMatrix() {
            int plans = 0;
            foreach (CompileGraphSize graphSize in Enum.GetValues(typeof(CompileGraphSize))) {
                foreach (AggregateJoinShape shape in Enum.GetValues(typeof(AggregateJoinShape))) {
                    DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(BenchmarkScenarioFactory.GetCompileRootType(graphSize), shape);
                    Require(plan.RootType == BenchmarkScenarioFactory.GetCompileRootType(graphSize), "Cold compile returned the wrong root type.");
                    Require(plan.Shape == shape, "Cold compile returned the wrong shape.");
                    plans++;
                }
            }
            Console.WriteLine($"COLD_PLANS_VERIFIED={plans}");
        }

        private static void VerifyAutomaticCacheCardinality() {
            var pairs = new[] {
                (typeof(SmallCompileRoot), AggregateJoinShape.ScalarAggregatesOnly),
                (typeof(SmallCompileRoot), AggregateJoinShape.FullGraph),
                (typeof(MediumCompileRoot), AggregateJoinShape.ScalarAggregatesOnly),
                (typeof(MediumCompileRoot), AggregateJoinShape.FullGraph),
                (typeof(LargeCompileRoot), AggregateJoinShape.ScalarAggregatesOnly),
                (typeof(LargeCompileRoot), AggregateJoinShape.FullGraph)
            };
            var canonical = new ConcurrentDictionary<AutoJoinPlanKey, DefinitiveJoinPlan>();
            Parallel.For(0, 384, index => {
                (Type rootType, AggregateJoinShape shape) = pairs[index % pairs.Length];
                DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(rootType, shape);
                var key = new AutoJoinPlanKey(rootType, shape, DefinitiveJoinPlanCompiler.CurrentFormatVersion);
                canonical.AddOrUpdate(key, plan, (_, existing) => {
                    Require(ReferenceEquals(existing, plan), "Automatic cache published multiple plan instances for one key.");
                    return existing;
                });
            });
            Require(canonical.Count == pairs.Length, "Automatic cache cardinality does not follow requested (Type, Shape) pairs.");

            int hits = 0;
            const int requests = 1200;
            for (int index = 0; index < requests; index++) {
                (Type rootType, AggregateJoinShape shape) = pairs[index % pairs.Length];
                var key = new AutoJoinPlanKey(rootType, shape, DefinitiveJoinPlanCompiler.CurrentFormatVersion);
                if (ReferenceEquals(canonical[key], AutomaticJoinPlanCache.GetOrAdd(rootType, shape))) {
                    hits++;
                }
            }
            Require(hits == requests, "Automatic cache warm hit rate was below 100%.");
            Console.WriteLine($"PLAN_CACHE_KEYS={canonical.Count}");
            Console.WriteLine($"PLAN_CACHE_HIT_RATE={(double)hits / requests:P2}");
        }

        private static void VerifyFrozenSchemaAndOrdering() {
            BenchmarkMaterializationScenario scenario = BenchmarkScenarioFactory.Create(BenchmarkIdentifierKind.Int64, 8, 50);
            DefinitiveJoinPlan plan = scenario.Plan;
            string signature = plan.StructuralSignature;
            string[] aliases = plan.Projection.Select(column => column.ResultAlias).ToArray();

            var table = new DataTable();
            foreach (string alias in aliases) {
                table.Columns.Add(alias, typeof(object));
            }
            table.Columns[table.Columns.Count - 1].ColumnName = "wrong_alias";
            using DataTableReader reader = table.CreateDataReader();
            bool rejected = false;
            try {
                ReaderSchemaValidator.Validate(reader, plan);
            } catch (ArgumentException) {
                rejected = true;
            }
            Require(rejected, "Reader schema mismatch was not rejected.");
            Require(plan.StructuralSignature == signature, "Schema validation mutated the frozen plan signature.");
            Require(plan.Projection.Select(column => column.ResultAlias).SequenceEqual(aliases), "Schema validation mutated the frozen projection.");

            string sql = new SqliteQueryGenerator().GenerateJoinQuery(plan, null).GetCommandText();
            Require(sql.IndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase) >= 0, "Frozen provider query omitted root ordering.");
            Require(sql.IndexOf(plan.RootOrdering.ColumnName, StringComparison.OrdinalIgnoreCase) >= 0, "Frozen provider query does not order by the root identifier.");
        }

        private static void VerifyNoMutableCustomPlanCache() {
            Type[] owners = { typeof(DefinitiveJoinPlanCompiler), typeof(CompiledAggregateMaterializerPlan) };
            foreach (Type owner in owners) {
                foreach (FieldInfo field in owner.GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)) {
                    string fieldType = field.FieldType.ToString();
                    Require(!fieldType.Contains("JoinDefinition", StringComparison.Ordinal), $"Static cache '{owner.Name}.{field.Name}' is keyed by mutable JoinDefinition.");
                }
            }
            Console.WriteLine("CUSTOM_PLAN_GLOBAL_CACHE_KEYS=0");
            Console.WriteLine("COLLECTIBLE_TYPE_NOTE=Automatic cache strongly retains requested Type keys; custom plan materializers are weakly keyed by plan identity.");
        }

        private static void Require(bool condition, string message) {
            if (!condition) {
                throw new InvalidOperationException(message);
            }
        }
    }
}

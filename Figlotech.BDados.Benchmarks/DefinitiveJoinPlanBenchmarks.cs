using BenchmarkDotNet.Attributes;
using Figlotech.BDados.DataAccessAbstractions;

namespace Figlotech.BDados.Benchmarks {
    [Config(typeof(JoinPlanBenchmarkConfig))]
    public class ColdPlanCompilationBenchmarks {
        [Params(CompileGraphSize.Small, CompileGraphSize.Medium, CompileGraphSize.Large)]
        public CompileGraphSize GraphSize { get; set; }

        [Params(AggregateJoinShape.ScalarAggregatesOnly, AggregateJoinShape.FullGraph)]
        public AggregateJoinShape Shape { get; set; }

        [Benchmark]
        public DefinitiveJoinPlan Compile() {
            return DefinitiveJoinPlanCompiler.Compile(BenchmarkScenarioFactory.GetCompileRootType(GraphSize), Shape);
        }
    }

    [Config(typeof(JoinPlanBenchmarkConfig))]
    public class WarmMaterializationBenchmarks {
        private BenchmarkMaterializationScenario _scenario = null!;

        [Params(BenchmarkIdentifierKind.Int32, BenchmarkIdentifierKind.Int64, BenchmarkIdentifierKind.Guid, BenchmarkIdentifierKind.String)]
        public BenchmarkIdentifierKind IdentifierKind { get; set; }

        [Params(1, 8, 32, 128)]
        public int ColumnCount { get; set; }

        [Params(0, 10, 50, 100)]
        public int NullPercent { get; set; }

        [GlobalSetup]
        public void Setup() {
            _scenario = BenchmarkScenarioFactory.Create(IdentifierKind, ColumnCount, NullPercent);
            if (_scenario.Materialize() != BenchmarkScenarioFactory.RowsPerInvocation) {
                throw new InvalidOperationException("Warm benchmark scenario did not materialize every uniquely identified row.");
            }
        }

        [Benchmark(OperationsPerInvoke = BenchmarkScenarioFactory.RowsPerInvocation)]
        public int Materialize() {
            return _scenario.Materialize();
        }
    }

    [Config(typeof(JoinPlanBenchmarkConfig))]
    public class AutomaticPlanCacheBenchmarks {
        [Params(AggregateJoinShape.ScalarAggregatesOnly, AggregateJoinShape.FullGraph)]
        public AggregateJoinShape Shape { get; set; }

        [GlobalSetup]
        public void Setup() {
            _ = AutomaticJoinPlanCache.GetOrAdd(typeof(LargeCompileRoot), Shape);
        }

        [Benchmark]
        public DefinitiveJoinPlan GetOrAddWarm() {
            return AutomaticJoinPlanCache.GetOrAdd(typeof(LargeCompileRoot), Shape);
        }
    }
}

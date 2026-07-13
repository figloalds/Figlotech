using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;

namespace Figlotech.BDados.Benchmarks {
    public sealed class JoinPlanBenchmarkConfig : ManualConfig {
        public JoinPlanBenchmarkConfig() {
            AddJob(Job.ShortRun
                .WithRuntime(CoreRuntime.Core60)
                .WithGcServer(true)
                .WithGcForce(false)
                .WithId("net6-short"));
            AddDiagnoser(MemoryDiagnoser.Default);
            AddColumnProvider(DefaultColumnProviders.Instance);
            AddExporter(MarkdownExporter.GitHub, CsvExporter.Default);
            AddLogger(ConsoleLogger.Default);
        }
    }
}

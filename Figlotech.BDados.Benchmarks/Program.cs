using BenchmarkDotNet.Running;
using Figlotech.BDados.Benchmarks;

if (args.Length == 1 && args[0] == "--verify") {
    BenchmarkVerification.Run();
    return;
}
if (args.Length == 1 && args[0] == "--providers") {
    ProviderProbeRunner.Run();
    return;
}

BenchmarkSwitcher
    .FromAssembly(typeof(JoinPlanBenchmarkConfig).Assembly)
    .Run(args);

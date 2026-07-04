using BenchmarkDotNet.Running;

namespace IntegratedS3.Benchmarks;

/// <summary>
/// Entry point for the IntegratedS3 BenchmarkDotNet hot-path suite.
/// Run with <c>dotnet run -c Release -- --filter *</c> (or a specific filter) to execute benchmarks;
/// pass <c>--list flat</c> to enumerate available benchmarks.
/// </summary>
public static class Program
{
    public static void Main(string[] args)
        => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, new HotPathBenchmarkConfig());
}

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace IntegratedS3.Benchmarks;

/// <summary>
/// Shared BenchmarkDotNet configuration for the hot-path suite.
/// </summary>
/// <remarks>
/// Uses the in-process (emit) toolchain deliberately: the repository sets
/// <c>TreatWarningsAsErrors=true</c> together with SourceLink and code-style analyzers, which routinely
/// break BenchmarkDotNet's default out-of-process generated child project. The in-process toolchain runs
/// the already-optimized benchmark assembly directly, keeps allocation measurement (<see cref="MemoryDiagnoser"/>),
/// and stays robust in a strict build. Runs must still be launched with <c>-c Release</c> so the JIT
/// optimizes the host assembly (the optimizations validator enforces this).
/// </remarks>
public sealed class HotPathBenchmarkConfig : ManualConfig
{
    public HotPathBenchmarkConfig()
    {
        AddJob(Job.Default
            .WithToolchain(InProcessEmitToolchain.Instance)
            .WithWarmupCount(3)
            .WithIterationCount(6)
            .WithId("InProcess"));

        AddDiagnoser(MemoryDiagnoser.Default);
        AddColumnProvider(DefaultColumnProviders.Instance);
        AddLogger(ConsoleLogger.Default);

        // GitHub Markdown for humans; Full JSON for the regression-gate compare script.
        AddExporter(MarkdownExporter.GitHub);
        AddExporter(JsonExporter.Full);
    }
}

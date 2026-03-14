using System.Runtime.InteropServices;

namespace IntegratedS3.Benchmarks;

public sealed class BenchmarkScenarioDefinition
{
    public string Id { get; init; } = string.Empty;

    public string HotPath { get; init; } = string.Empty;

    public string Transport { get; init; } = string.Empty;

    public string ProviderTopology { get; init; } = string.Empty;

    public string WorkloadDescription { get; init; } = string.Empty;

    public long? PayloadBytes { get; init; }

    public int? LogicalItemCount { get; init; }
}

public sealed class BenchmarkScenarioRegistration
{
    public required BenchmarkScenarioDefinition Definition { get; init; }

    public required Func<IHotPathBenchmarkScenario> CreateScenario { get; init; }
}

public interface IHotPathBenchmarkScenario : IAsyncDisposable
{
    BenchmarkScenarioDefinition Definition { get; }

    IReadOnlyList<string> ObservedDirectories { get; }

    Task InitializeAsync(CancellationToken cancellationToken);

    Task PrepareIterationAsync(bool isWarmup, CancellationToken cancellationToken);

    ValueTask ExecuteIterationAsync(CancellationToken cancellationToken);
}

public sealed class HotPathBenchmarkReport
{
    public DateTimeOffset GeneratedAtUtc { get; init; }

    public BenchmarkEnvironmentInfo Environment { get; init; } = BenchmarkEnvironmentInfo.Create();

    public BenchmarkExecutionConfiguration Configuration { get; init; } = new();

    public IReadOnlyList<HotPathBenchmarkResult> Scenarios { get; init; } = [];

    public IReadOnlyList<string> Notes { get; init; } = [];
}

public sealed class BenchmarkEnvironmentInfo
{
    public string OsDescription { get; init; } = string.Empty;

    public string FrameworkDescription { get; init; } = string.Empty;

    public string ProcessArchitecture { get; init; } = string.Empty;

    public int ProcessorCount { get; init; }

    public bool ServerGc { get; init; }

    public static BenchmarkEnvironmentInfo Create()
    {
        return new BenchmarkEnvironmentInfo
        {
            OsDescription = RuntimeInformation.OSDescription,
            FrameworkDescription = RuntimeInformation.FrameworkDescription,
            ProcessArchitecture = RuntimeInformation.ProcessArchitecture.ToString(),
            ProcessorCount = Environment.ProcessorCount,
            ServerGc = System.Runtime.GCSettings.IsServerGC
        };
    }
}

public sealed class BenchmarkExecutionConfiguration
{
    public int WarmupIterations { get; init; }

    public int MeasuredIterations { get; init; }
}

public sealed class HotPathBenchmarkResult
{
    public BenchmarkScenarioDefinition Scenario { get; init; } = new();

    public LatencyMetrics Latency { get; init; } = new();

    public ThroughputMetrics Throughput { get; init; } = new();

    public AllocationMetrics Allocation { get; init; } = new();

    public LohPressureMetrics LohPressure { get; init; } = new();

    public TempFileChurnMetrics TempFileChurn { get; init; } = new();

    public ThreadPoolMetrics ThreadPool { get; init; } = new();

    public IReadOnlyList<ProviderLatencyMetrics> ProviderBreakdown { get; init; } = [];
}

public sealed class LatencyMetrics
{
    public double MeanMilliseconds { get; init; }

    public double P50Milliseconds { get; init; }

    public double P95Milliseconds { get; init; }

    public double P99Milliseconds { get; init; }
}

public sealed class ThroughputMetrics
{
    public double OperationsPerSecond { get; init; }

    public double? PayloadMiBPerSecond { get; init; }

    public double? LogicalItemsPerSecond { get; init; }
}

public sealed class AllocationMetrics
{
    public double MeanBytesPerOperation { get; init; }

    public double P95BytesPerOperation { get; init; }
}

public sealed class LohPressureMetrics
{
    public double MeanDeltaBytes { get; init; }

    public long MaxDeltaBytes { get; init; }
}

public sealed class TempFileChurnMetrics
{
    public double MeanFilesCreated { get; init; }

    public double MeanFilesDeleted { get; init; }

    public double MeanFilesRenamed { get; init; }

    public double MeanNetBytesDelta { get; init; }
}

public sealed class ThreadPoolMetrics
{
    public long MaxPendingWorkItemsDelta { get; init; }

    public int MaxThreadCountDelta { get; init; }

    public int MinAvailableWorkerThreads { get; init; }

    public int MinAvailableIoThreads { get; init; }
}

public sealed class ProviderLatencyMetrics
{
    public string ProviderName { get; init; } = string.Empty;

    public double MeanMilliseconds { get; init; }

    public double P95Milliseconds { get; init; }

    public double P99Milliseconds { get; init; }
}

public sealed class HotPathBenchmarkSample
{
    public long ElapsedTicks { get; init; }

    public long AllocatedBytes { get; init; }

    public long LargeObjectHeapDeltaBytes { get; init; }

    public int FilesCreated { get; init; }

    public int FilesDeleted { get; init; }

    public int FilesRenamed { get; init; }

    public long NetDirectoryBytesDelta { get; init; }

    public long PendingWorkItemsDelta { get; init; }

    public int ThreadCountDelta { get; init; }

    public int AvailableWorkerThreads { get; init; }

    public int AvailableIoThreads { get; init; }

    public IReadOnlyDictionary<string, long> ProviderLatencyTicks { get; init; } = new Dictionary<string, long>(StringComparer.Ordinal);
}

internal static class BenchmarkDefaults
{
    public const string RoutePrefix = "/integrated-s3";
    public const string AccessKeyId = "benchmark-access";
    public const string SecretAccessKey = "benchmark-secret";
    public const string Region = "us-east-1";
    public const string Service = "s3";
    public const string ApplicationOverheadBucket = "application-overhead";

    public static readonly IReadOnlyList<string> ReportNotes =
    [
        "Latency, throughput, and allocation values are regression-oriented baselines from the configured iteration counts, not pass/fail thresholds.",
        "Temp-file churn is collected from FileSystemWatcher events scoped to benchmark-owned roots, so provider work outside those roots is intentionally excluded.",
        "Provider breakdown adds an 'application-overhead' bucket for non-provider time inside the measured path."
    ];
}

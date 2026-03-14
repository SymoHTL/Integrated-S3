using System.Diagnostics;
using System.Globalization;

namespace IntegratedS3.Benchmarks;

public sealed class HotPathBenchmarkRunner
{
    public async Task<HotPathBenchmarkReport> RunAsync(
        IReadOnlyList<BenchmarkScenarioRegistration> scenarios,
        BenchmarkExecutionOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(scenarios);
        ArgumentNullException.ThrowIfNull(options);

        var results = new List<HotPathBenchmarkResult>(scenarios.Count);
        foreach (var registration in scenarios) {
            cancellationToken.ThrowIfCancellationRequested();
            Console.WriteLine($"-> {registration.Definition.Id}");

            await using var scenario = registration.CreateScenario();
            await scenario.InitializeAsync(cancellationToken);

            for (var warmupIndex = 0; warmupIndex < options.WarmupIterations; warmupIndex++) {
                await scenario.PrepareIterationAsync(isWarmup: true, cancellationToken);
                await scenario.ExecuteIterationAsync(cancellationToken);
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            using var fileActivityTracker = new FileSystemActivityTracker(scenario.ObservedDirectories);
            var samples = new List<HotPathBenchmarkSample>(options.MeasuredIterations);
            for (var measurementIndex = 0; measurementIndex < options.MeasuredIterations; measurementIndex++) {
                cancellationToken.ThrowIfCancellationRequested();
                await scenario.PrepareIterationAsync(isWarmup: false, cancellationToken);

                var directoryBytesBefore = FileSystemActivityTracker.MeasureDirectoryBytes(scenario.ObservedDirectories);
                fileActivityTracker.Reset();

                var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
                var largeObjectHeapBefore = BenchmarkRuntimeMetrics.CaptureLargeObjectHeapSizeBytes();
                var threadPoolBefore = ThreadPoolSnapshot.Capture();
                var latencyCollector = new ProviderLatencyCollector();

                var stopwatch = Stopwatch.StartNew();
                using (StorageBackendProfilingContext.Begin(latencyCollector)) {
                    await scenario.ExecuteIterationAsync(cancellationToken);
                }

                stopwatch.Stop();

                var fileActivity = await fileActivityTracker.CaptureAsync(cancellationToken);
                var threadPoolAfter = ThreadPoolSnapshot.Capture();
                var largeObjectHeapAfter = BenchmarkRuntimeMetrics.CaptureLargeObjectHeapSizeBytes();
                var allocatedAfter = GC.GetTotalAllocatedBytes(precise: true);
                var directoryBytesAfter = FileSystemActivityTracker.MeasureDirectoryBytes(scenario.ObservedDirectories);

                samples.Add(new HotPathBenchmarkSample
                {
                    ElapsedTicks = stopwatch.ElapsedTicks,
                    AllocatedBytes = allocatedAfter - allocatedBefore,
                    LargeObjectHeapDeltaBytes = largeObjectHeapAfter - largeObjectHeapBefore,
                    FilesCreated = fileActivity.FilesCreated,
                    FilesDeleted = fileActivity.FilesDeleted,
                    FilesRenamed = fileActivity.FilesRenamed,
                    NetDirectoryBytesDelta = directoryBytesAfter - directoryBytesBefore,
                    PendingWorkItemsDelta = threadPoolAfter.PendingWorkItems - threadPoolBefore.PendingWorkItems,
                    ThreadCountDelta = threadPoolAfter.ThreadCount - threadPoolBefore.ThreadCount,
                    AvailableWorkerThreads = threadPoolAfter.AvailableWorkerThreads,
                    AvailableIoThreads = threadPoolAfter.AvailableIoThreads,
                    ProviderLatencyTicks = latencyCollector.SnapshotTicks()
                });
            }

            results.Add(HotPathBenchmarkSummaryCalculator.Summarize(registration.Definition, samples));
        }

        return new HotPathBenchmarkReport
        {
            GeneratedAtUtc = DateTimeOffset.UtcNow,
            Environment = BenchmarkEnvironmentInfo.Create(),
            Configuration = new BenchmarkExecutionConfiguration
            {
                WarmupIterations = options.WarmupIterations,
                MeasuredIterations = options.MeasuredIterations
            },
            Scenarios = results,
            Notes = BenchmarkDefaults.ReportNotes
        };
    }
}

public static class HotPathBenchmarkSummaryCalculator
{
    public static HotPathBenchmarkResult Summarize(BenchmarkScenarioDefinition definition, IReadOnlyList<HotPathBenchmarkSample> samples)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(samples);
        if (samples.Count == 0) {
            throw new ArgumentException("At least one benchmark sample is required.", nameof(samples));
        }

        var elapsedMilliseconds = samples.Select(static sample => sample.ElapsedTicks * 1000d / Stopwatch.Frequency).ToArray();
        var allocatedBytes = samples.Select(static sample => (double)sample.AllocatedBytes).ToArray();
        var largeObjectHeapDeltas = samples.Select(static sample => (double)sample.LargeObjectHeapDeltaBytes).ToArray();
        var totalElapsedSeconds = elapsedMilliseconds.Sum() / 1000d;

        var providerTicks = new Dictionary<string, List<double>>(StringComparer.Ordinal);
        foreach (var sample in samples) {
            double providerElapsed = 0;
            foreach (var (providerName, ticks) in sample.ProviderLatencyTicks) {
                var milliseconds = ticks * 1000d / Stopwatch.Frequency;
                providerElapsed += milliseconds;
                GetOrAdd(providerTicks, providerName).Add(milliseconds);
            }

            GetOrAdd(providerTicks, BenchmarkDefaults.ApplicationOverheadBucket).Add(Math.Max(0, sample.ElapsedTicks * 1000d / Stopwatch.Frequency - providerElapsed));
        }

        return new HotPathBenchmarkResult
        {
            Scenario = definition,
            Latency = new LatencyMetrics
            {
                MeanMilliseconds = HotPathBenchmarkMath.Mean(elapsedMilliseconds),
                P50Milliseconds = HotPathBenchmarkMath.Percentile(elapsedMilliseconds, 0.50),
                P95Milliseconds = HotPathBenchmarkMath.Percentile(elapsedMilliseconds, 0.95),
                P99Milliseconds = HotPathBenchmarkMath.Percentile(elapsedMilliseconds, 0.99)
            },
            Throughput = new ThroughputMetrics
            {
                OperationsPerSecond = totalElapsedSeconds <= 0 ? 0 : samples.Count / totalElapsedSeconds,
                PayloadMiBPerSecond = definition.PayloadBytes.HasValue && totalElapsedSeconds > 0
                    ? definition.PayloadBytes.Value * samples.Count / totalElapsedSeconds / (1024d * 1024d)
                    : null,
                LogicalItemsPerSecond = definition.LogicalItemCount.HasValue && totalElapsedSeconds > 0
                    ? definition.LogicalItemCount.Value * samples.Count / totalElapsedSeconds
                    : null
            },
            Allocation = new AllocationMetrics
            {
                MeanBytesPerOperation = HotPathBenchmarkMath.Mean(allocatedBytes),
                P95BytesPerOperation = HotPathBenchmarkMath.Percentile(allocatedBytes, 0.95)
            },
            LohPressure = new LohPressureMetrics
            {
                MeanDeltaBytes = HotPathBenchmarkMath.Mean(largeObjectHeapDeltas),
                MaxDeltaBytes = samples.Max(static sample => sample.LargeObjectHeapDeltaBytes)
            },
            TempFileChurn = new TempFileChurnMetrics
            {
                MeanFilesCreated = samples.Average(static sample => sample.FilesCreated),
                MeanFilesDeleted = samples.Average(static sample => sample.FilesDeleted),
                MeanFilesRenamed = samples.Average(static sample => sample.FilesRenamed),
                MeanNetBytesDelta = samples.Average(static sample => sample.NetDirectoryBytesDelta)
            },
            ThreadPool = new ThreadPoolMetrics
            {
                MaxPendingWorkItemsDelta = samples.Max(static sample => sample.PendingWorkItemsDelta),
                MaxThreadCountDelta = samples.Max(static sample => sample.ThreadCountDelta),
                MinAvailableWorkerThreads = samples.Min(static sample => sample.AvailableWorkerThreads),
                MinAvailableIoThreads = samples.Min(static sample => sample.AvailableIoThreads)
            },
            ProviderBreakdown = providerTicks
                .OrderBy(static entry => entry.Key == BenchmarkDefaults.ApplicationOverheadBucket ? 1 : 0)
                .ThenBy(static entry => entry.Key, StringComparer.Ordinal)
                .Select(static entry => new ProviderLatencyMetrics
                {
                    ProviderName = entry.Key,
                    MeanMilliseconds = HotPathBenchmarkMath.Mean(entry.Value),
                    P95Milliseconds = HotPathBenchmarkMath.Percentile(entry.Value, 0.95),
                    P99Milliseconds = HotPathBenchmarkMath.Percentile(entry.Value, 0.99)
                })
                .ToArray()
        };
    }

    private static List<double> GetOrAdd(Dictionary<string, List<double>> source, string key)
    {
        if (source.TryGetValue(key, out var values)) {
            return values;
        }

        values = [];
        source[key] = values;
        return values;
    }
}

internal static class HotPathBenchmarkMath
{
    public static double Mean(IReadOnlyList<double> values)
    {
        return values.Count == 0 ? 0 : values.Sum() / values.Count;
    }

    public static double Percentile(IReadOnlyList<double> values, double percentile)
    {
        if (values.Count == 0) {
            return 0;
        }

        if (percentile <= 0) {
            return values.Min();
        }

        if (percentile >= 1) {
            return values.Max();
        }

        var ordered = values.OrderBy(static value => value).ToArray();
        var rank = (int)Math.Ceiling(percentile * ordered.Length) - 1;
        return ordered[Math.Clamp(rank, 0, ordered.Length - 1)];
    }
}

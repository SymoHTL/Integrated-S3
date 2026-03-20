using System.Diagnostics;
using System.Globalization;
using IntegratedS3.Benchmarks;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class HotPathBenchmarkReportTests
{
    [Fact]
    public void ScenarioCatalog_CoversTrackHHotPathsAndRepresentativeHttpAndAwsSdkScenarios()
    {
        var scenarioIds = HotPathBenchmarkScenarioCatalog.All
            .Select(static registration => registration.Definition.Id)
            .ToArray();

        Assert.Equal(19, scenarioIds.Length);
        Assert.Equal(scenarioIds.Length, scenarioIds.Distinct(StringComparer.Ordinal).Count());
        Assert.Contains("http-head-object-metadata", scenarioIds);
        Assert.Contains("http-put-object", scenarioIds);
        Assert.Contains("http-upload-multipart-part", scenarioIds);
        Assert.Contains("http-get-object", scenarioIds);
        Assert.Contains("http-list-objects", scenarioIds);
        Assert.Contains("aws-sdk-path-get-object-metadata", scenarioIds);
        Assert.Contains("aws-sdk-path-put-object", scenarioIds);
        Assert.Contains("aws-sdk-path-upload-multipart-part", scenarioIds);
        Assert.Contains("aws-sdk-path-get-object", scenarioIds);
        Assert.Contains("aws-sdk-path-list-objects-v2", scenarioIds);

        var hotPaths = HotPathBenchmarkScenarioCatalog.All
            .Select(static registration => registration.Definition.HotPath)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
        [
            "list operations",
            "metadata lookup",
            "mirrored writes",
            "multipart complete",
            "multipart part upload",
            "object download",
            "object upload",
            "presign generation",
            "request auth/signature validation"
        ], hotPaths);
    }

    [Fact]
    public void SummaryCalculator_ComputesLatencyThroughputAndProviderBreakdown()
    {
        var definition = new BenchmarkScenarioDefinition
        {
            Id = "sample-scenario",
            HotPath = "object upload",
            Transport = "in-process service",
            ProviderTopology = "single disk backend",
            WorkloadDescription = "Sample benchmark summary.",
            PayloadBytes = 1024,
            LogicalItemCount = 4
        };

        HotPathBenchmarkSample[] samples =
        [
            CreateSample(10, 100, 2, 1, 0, 1, 10, 0, 0, 10, 6, ("disk-primary", 4)),
            CreateSample(20, 300, 6, 2, 1, 1, 20, 2, 1, 8, 5, ("disk-primary", 8)),
            CreateSample(30, 500, 10, 3, 2, 1, 30, 1, 2, 9, 6, ("disk-primary", 12))
        ];

        var result = HotPathBenchmarkSummaryCalculator.Summarize(definition, samples);

        Assert.Equal(20, result.Latency.P50Milliseconds, precision: 5);
        Assert.Equal(30, result.Latency.P95Milliseconds, precision: 5);
        Assert.Equal(30, result.Latency.P99Milliseconds, precision: 5);
        Assert.Equal(50, result.Throughput.OperationsPerSecond, precision: 5);
        Assert.Equal(0.048828125, result.Throughput.PayloadMiBPerSecond!.Value, precision: 8);
        Assert.Equal(200, result.Throughput.LogicalItemsPerSecond!.Value, precision: 5);
        Assert.Equal(300, result.Allocation.MeanBytesPerOperation, precision: 5);
        Assert.Equal(500, result.Allocation.P95BytesPerOperation, precision: 5);

        var diskProvider = Assert.Single(result.ProviderBreakdown, static provider => provider.ProviderName == "disk-primary");
        Assert.Equal(8, diskProvider.MeanMilliseconds, precision: 5);
        Assert.Equal(12, diskProvider.P95Milliseconds, precision: 5);

        var applicationOverhead = Assert.Single(result.ProviderBreakdown, static provider => provider.ProviderName == "application-overhead");
        Assert.Equal(12, applicationOverhead.MeanMilliseconds, precision: 5);
        Assert.Equal(2, result.TempFileChurn.MeanFilesCreated, precision: 5);
        Assert.Equal(2, result.ThreadPool.MaxThreadCountDelta);
        Assert.Equal(8, result.ThreadPool.MinAvailableWorkerThreads);
    }

    [Fact]
    public void ReportWriter_BuildsMarkdownAndJsonOutput()
    {
        var result = HotPathBenchmarkSummaryCalculator.Summarize(
            new BenchmarkScenarioDefinition
            {
                Id = "report-scenario",
                HotPath = "metadata lookup",
                Transport = "in-process service",
                ProviderTopology = "single disk backend",
                WorkloadDescription = "Report writer validation."
            },
            [
                CreateSample(5, 10, 0, 0, 0, 0, 0, 0, 0, 12, 8, ("disk-primary", 4))
            ]);

        var report = new HotPathBenchmarkReport
        {
            GeneratedAtUtc = DateTimeOffset.Parse("2026-03-14T10:00:00Z", CultureInfo.InvariantCulture),
            Configuration = new BenchmarkExecutionConfiguration
            {
                WarmupIterations = 2,
                MeasuredIterations = 1
            },
            Scenarios = [result],
            Notes =
            [
                "Sample benchmark note."
            ]
        };

        var markdown = HotPathBenchmarkReportWriter.BuildMarkdown(report);
        Assert.Contains("IntegratedS3 hot-path benchmark baselines", markdown);
        Assert.Contains("report-scenario", markdown);
        Assert.Contains("disk-primary", markdown);
        Assert.Contains("application-overhead", markdown);

        var tempDirectory = Path.Combine(Path.GetTempPath(), "IntegratedS3.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDirectory);

        try {
            var jsonPath = Path.Combine(tempDirectory, "baseline.json");
            HotPathBenchmarkReportWriter.WriteJson(report, jsonPath);
            var json = File.ReadAllText(jsonPath);

            Assert.Contains("\"Id\": \"report-scenario\"", json);
            Assert.Contains("\"ProviderName\": \"disk-primary\"", json);
        }
        finally {
            Directory.Delete(tempDirectory, recursive: true);
        }
    }

    private static HotPathBenchmarkSample CreateSample(
        double elapsedMilliseconds,
        long allocatedBytes,
        long largeObjectHeapDeltaBytes,
        int filesCreated,
        int filesDeleted,
        int filesRenamed,
        long netDirectoryBytesDelta,
        long pendingWorkItemsDelta,
        int threadCountDelta,
        int availableWorkerThreads,
        int availableIoThreads,
        params (string ProviderName, double ElapsedMilliseconds)[] providerBreakdown)
    {
        return new HotPathBenchmarkSample
        {
            ElapsedTicks = MillisecondsToStopwatchTicks(elapsedMilliseconds),
            AllocatedBytes = allocatedBytes,
            LargeObjectHeapDeltaBytes = largeObjectHeapDeltaBytes,
            FilesCreated = filesCreated,
            FilesDeleted = filesDeleted,
            FilesRenamed = filesRenamed,
            NetDirectoryBytesDelta = netDirectoryBytesDelta,
            PendingWorkItemsDelta = pendingWorkItemsDelta,
            ThreadCountDelta = threadCountDelta,
            AvailableWorkerThreads = availableWorkerThreads,
            AvailableIoThreads = availableIoThreads,
            ProviderLatencyTicks = providerBreakdown.ToDictionary(
                static entry => entry.ProviderName,
                static entry => MillisecondsToStopwatchTicks(entry.ElapsedMilliseconds),
                StringComparer.Ordinal)
        };
    }

    private static long MillisecondsToStopwatchTicks(double milliseconds)
    {
        return (long)Math.Round(milliseconds * Stopwatch.Frequency / 1000d, MidpointRounding.AwayFromZero);
    }
}

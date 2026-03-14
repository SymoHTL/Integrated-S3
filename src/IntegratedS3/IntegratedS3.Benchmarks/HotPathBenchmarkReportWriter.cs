using System.Globalization;
using System.Text;
using System.Text.Json;

namespace IntegratedS3.Benchmarks;

public static class HotPathBenchmarkReportWriter
{
    public static void WriteJson(HotPathBenchmarkReport report, string path)
    {
        ArgumentNullException.ThrowIfNull(report);
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var fullPath = Path.GetFullPath(path);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        File.WriteAllText(
            fullPath,
            JsonSerializer.Serialize(report, HotPathBenchmarkJsonSerializerContext.Default.HotPathBenchmarkReport),
            Encoding.UTF8);
    }

    public static void WriteMarkdown(HotPathBenchmarkReport report, string path)
    {
        ArgumentNullException.ThrowIfNull(report);
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var fullPath = Path.GetFullPath(path);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        File.WriteAllText(fullPath, BuildMarkdown(report), Encoding.UTF8);
    }

    public static string BuildMarkdown(HotPathBenchmarkReport report)
    {
        ArgumentNullException.ThrowIfNull(report);

        var builder = new StringBuilder();
        builder.AppendLine("# IntegratedS3 hot-path benchmark baselines");
        builder.AppendLine();
        builder.AppendLine($"Generated: `{report.GeneratedAtUtc:O}`");
        builder.AppendLine();
        builder.AppendLine($"Environment: `{report.Environment.OsDescription}` | `{report.Environment.FrameworkDescription}` | `{report.Environment.ProcessArchitecture}` | `{report.Environment.ProcessorCount}` logical processors | server GC: `{report.Environment.ServerGc}`");
        builder.AppendLine();
        builder.AppendLine($"Configuration: warmup `{report.Configuration.WarmupIterations}`, measured `{report.Configuration.MeasuredIterations}`");
        builder.AppendLine();
        builder.AppendLine("| Scenario | Hot path | Transport | Topology | p50 ms | p95 ms | p99 ms | Ops/s | MiB/s | Items/s | Alloc KB/op |");
        builder.AppendLine("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");

        foreach (var result in report.Scenarios) {
            builder.Append("| ")
                .Append(result.Scenario.Id)
                .Append(" | ")
                .Append(result.Scenario.HotPath)
                .Append(" | ")
                .Append(result.Scenario.Transport)
                .Append(" | ")
                .Append(result.Scenario.ProviderTopology)
                .Append(" | ")
                .Append(Format(result.Latency.P50Milliseconds))
                .Append(" | ")
                .Append(Format(result.Latency.P95Milliseconds))
                .Append(" | ")
                .Append(Format(result.Latency.P99Milliseconds))
                .Append(" | ")
                .Append(Format(result.Throughput.OperationsPerSecond))
                .Append(" | ")
                .Append(FormatNullable(result.Throughput.PayloadMiBPerSecond))
                .Append(" | ")
                .Append(FormatNullable(result.Throughput.LogicalItemsPerSecond))
                .Append(" | ")
                .Append(Format(result.Allocation.MeanBytesPerOperation / 1024d))
                .AppendLine(" |");
        }

        builder.AppendLine();
        builder.AppendLine("## Provider breakdown");
        builder.AppendLine();

        foreach (var result in report.Scenarios) {
            builder.AppendLine($"### `{result.Scenario.Id}`");
            builder.AppendLine();
            builder.AppendLine($"- Workload: {result.Scenario.WorkloadDescription}");
            builder.AppendLine($"- LOH delta (mean/max bytes): `{Format(result.LohPressure.MeanDeltaBytes)}` / `{result.LohPressure.MaxDeltaBytes.ToString(CultureInfo.InvariantCulture)}`");
            builder.AppendLine($"- Temp-file churn (created/deleted/renamed mean): `{Format(result.TempFileChurn.MeanFilesCreated)}` / `{Format(result.TempFileChurn.MeanFilesDeleted)}` / `{Format(result.TempFileChurn.MeanFilesRenamed)}`");
            builder.AppendLine($"- Thread-pool pressure (max pending/thread delta): `{result.ThreadPool.MaxPendingWorkItemsDelta.ToString(CultureInfo.InvariantCulture)}` / `{result.ThreadPool.MaxThreadCountDelta.ToString(CultureInfo.InvariantCulture)}`");
            foreach (var provider in result.ProviderBreakdown) {
                builder.AppendLine($"  - `{provider.ProviderName}` mean/p95/p99 ms: `{Format(provider.MeanMilliseconds)}` / `{Format(provider.P95Milliseconds)}` / `{Format(provider.P99Milliseconds)}`");
            }

            builder.AppendLine();
        }

        builder.AppendLine("## Notes");
        builder.AppendLine();
        foreach (var note in report.Notes) {
            builder.AppendLine($"- {note}");
        }

        return builder.ToString();
    }

    private static string Format(double value)
    {
        return value.ToString("0.###", CultureInfo.InvariantCulture);
    }

    private static string FormatNullable(double? value)
    {
        return value.HasValue
            ? Format(value.Value)
            : "-";
    }
}

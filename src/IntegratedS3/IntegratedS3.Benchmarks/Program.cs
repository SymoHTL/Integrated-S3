using System.Globalization;

namespace IntegratedS3.Benchmarks;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var options = ParseArguments(args);
        if (options.ShowHelp) {
            PrintUsage();
            return;
        }

        if (options.ListScenarios) {
            foreach (var registration in HotPathBenchmarkScenarioCatalog.All) {
                Console.WriteLine($"{registration.Definition.Id} | {registration.Definition.HotPath} | {registration.Definition.Transport} | {registration.Definition.ProviderTopology}");
            }

            return;
        }

        var selectedScenarios = HotPathBenchmarkScenarioCatalog.Resolve(options.ScenarioIds);
        Console.WriteLine($"Running {selectedScenarios.Count} IntegratedS3 hot-path benchmark scenario(s)...");

        var runner = new HotPathBenchmarkRunner();
        var report = await runner.RunAsync(selectedScenarios, options, CancellationToken.None);

        HotPathBenchmarkReportWriter.WriteJson(report, options.JsonOutputPath);
        HotPathBenchmarkReportWriter.WriteMarkdown(report, options.MarkdownOutputPath);

        foreach (var result in report.Scenarios) {
            Console.WriteLine($"{result.Scenario.Id}: p50={result.Latency.P50Milliseconds.ToString("0.###", CultureInfo.InvariantCulture)} ms, p95={result.Latency.P95Milliseconds.ToString("0.###", CultureInfo.InvariantCulture)} ms, ops/s={result.Throughput.OperationsPerSecond.ToString("0.###", CultureInfo.InvariantCulture)}");
        }

        Console.WriteLine($"JSON baseline report: {options.JsonOutputPath}");
        Console.WriteLine($"Markdown baseline report: {options.MarkdownOutputPath}");
    }

    private static BenchmarkExecutionOptions ParseArguments(string[] args)
    {
        var warmupIterations = 2;
        var measuredIterations = 12;
        string? jsonOutput = null;
        string? markdownOutput = null;
        var scenarioIds = new List<string>();
        var listScenarios = false;

        for (var index = 0; index < args.Length; index++) {
            switch (args[index]) {
                case "--warmup":
                    warmupIterations = ParsePositiveInt(args, ++index, "--warmup");
                    break;
                case "--measured":
                    measuredIterations = ParsePositiveInt(args, ++index, "--measured");
                    break;
                case "--json-output":
                    jsonOutput = ParseRequiredValue(args, ++index, "--json-output");
                    break;
                case "--markdown-output":
                    markdownOutput = ParseRequiredValue(args, ++index, "--markdown-output");
                    break;
                case "--scenario":
                    scenarioIds.AddRange(ParseRequiredValue(args, ++index, "--scenario")
                        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
                    break;
                case "--list-scenarios":
                    listScenarios = true;
                    break;
                case "--help":
                case "-h":
                    return new BenchmarkExecutionOptions
                    {
                        ShowHelp = true
                    };
                default:
                    throw new ArgumentException($"Unknown argument '{args[index]}'.");
            }
        }

        return new BenchmarkExecutionOptions
        {
            WarmupIterations = warmupIterations,
            MeasuredIterations = measuredIterations,
            JsonOutputPath = Path.GetFullPath(jsonOutput ?? Path.Combine("docs", "benchmarks", "hot-path-baseline.json")),
            MarkdownOutputPath = Path.GetFullPath(markdownOutput ?? Path.Combine("docs", "benchmarks", "hot-path-baseline.md")),
            ScenarioIds = scenarioIds,
            ListScenarios = listScenarios
        };
    }

    private static int ParsePositiveInt(string[] args, int index, string argumentName)
    {
        var value = ParseRequiredValue(args, index, argumentName);
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0
            ? parsed
            : throw new ArgumentOutOfRangeException(argumentName, $"The value '{value}' is not a positive integer.");
    }

    private static string ParseRequiredValue(string[] args, int index, string argumentName)
    {
        return index < args.Length
            ? args[index]
            : throw new ArgumentException($"Missing value for '{argumentName}'.");
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Usage: dotnet run --project src\\IntegratedS3\\IntegratedS3.Benchmarks\\IntegratedS3.Benchmarks.csproj -- [options]");
        Console.WriteLine("  --warmup <count>           Warmup iterations per scenario (default: 2)");
        Console.WriteLine("  --measured <count>         Measured iterations per scenario (default: 12)");
        Console.WriteLine("  --json-output <path>       Output path for the JSON baseline report");
        Console.WriteLine("  --markdown-output <path>   Output path for the Markdown baseline report");
        Console.WriteLine("  --scenario <id[,id...]>    Limit execution to the listed scenario ids");
        Console.WriteLine("  --list-scenarios           Print the available scenario ids");
    }
}

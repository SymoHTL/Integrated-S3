namespace IntegratedS3.Benchmarks;

public sealed class BenchmarkExecutionOptions
{
    public int WarmupIterations { get; init; } = 2;

    public int MeasuredIterations { get; init; } = 12;

    public string JsonOutputPath { get; init; } = Path.GetFullPath(Path.Combine("docs", "benchmarks", "hot-path-baseline.json"));

    public string MarkdownOutputPath { get; init; } = Path.GetFullPath(Path.Combine("docs", "benchmarks", "hot-path-baseline.md"));

    public IReadOnlyList<string> ScenarioIds { get; init; } = [];

    public bool ListScenarios { get; init; }

    public bool ShowHelp { get; init; }
}

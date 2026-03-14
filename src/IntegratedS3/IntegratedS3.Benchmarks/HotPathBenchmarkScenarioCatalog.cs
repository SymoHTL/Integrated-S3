namespace IntegratedS3.Benchmarks;

public static class HotPathBenchmarkScenarioCatalog
{
    private static readonly BenchmarkScenarioRegistration[] Scenarios =
    [
        Create(static () => new HttpSigV4HeadBucketScenario()),
        Create(static () => new MetadataLookupScenario()),
        Create(static () => new DiskPutObjectScenario()),
        Create(static () => new DiskGetObjectScenario()),
        Create(static () => new MultipartPartUploadScenario()),
        Create(static () => new MultipartCompleteScenario()),
        Create(static () => new MirroredWriteScenario()),
        Create(static () => new ListObjectsScenario()),
        Create(static () => new PresignScenario()),
        Create(static () => new HttpPutObjectScenario()),
        Create(static () => new HttpGetObjectScenario()),
        Create(static () => new HttpListObjectsScenario())
    ];

    public static IReadOnlyList<BenchmarkScenarioRegistration> All => Scenarios;

    public static IReadOnlyList<BenchmarkScenarioRegistration> Resolve(IReadOnlyList<string> scenarioIds)
    {
        ArgumentNullException.ThrowIfNull(scenarioIds);

        if (scenarioIds.Count == 0) {
            return Scenarios;
        }

        var requestedIds = scenarioIds
            .Where(static id => !string.IsNullOrWhiteSpace(id))
            .Select(static id => id.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var resolved = requestedIds
            .Select(id => Scenarios.FirstOrDefault(registration => string.Equals(registration.Definition.Id, id, StringComparison.Ordinal))
                ?? throw new ArgumentException($"Unknown benchmark scenario id '{id}'.", nameof(scenarioIds)))
            .ToArray();

        return resolved;
    }

    private static BenchmarkScenarioRegistration Create(Func<IHotPathBenchmarkScenario> factory)
    {
        var scenario = factory();

        try
        {
            return new BenchmarkScenarioRegistration
            {
                Definition = scenario.Definition,
                CreateScenario = factory
            };
        }
        finally
        {
            if (scenario is IAsyncDisposable asyncDisposable)
            {
                asyncDisposable.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            else if (scenario is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }
}

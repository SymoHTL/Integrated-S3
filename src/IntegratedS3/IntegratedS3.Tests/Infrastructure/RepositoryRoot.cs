namespace IntegratedS3.Tests.Infrastructure;

/// <summary>
/// Locates the repository root from the test output directory, for tests that read files checked into the repo.
/// </summary>
internal static class RepositoryRoot
{
    public static string Get()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);

        while (directory is not null) {
            if (File.Exists(Path.Combine(directory.FullName, "LICENSE"))
                && Directory.Exists(Path.Combine(directory.FullName, "docs"))
                && Directory.Exists(Path.Combine(directory.FullName, "src"))) {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the repository root from the test output directory.");
    }

    public static string Combine(params string[] pathSegments)
    {
        return Path.Combine(Get(), Path.Combine(pathSegments));
    }
}

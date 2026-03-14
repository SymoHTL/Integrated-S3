using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Documentation and packaging guidance checks that keep the consumer-facing Track H docs discoverable.
/// </summary>
public sealed class DocumentationGuidanceTests
{
    [Fact]
    public void DirectoryBuildProps_EnablesPackageReadmeAndXmlDocs()
    {
        var content = ReadRepositoryFile("src", "IntegratedS3", "Directory.Build.props");

        Assert.Contains("<GenerateDocumentationFile", content, StringComparison.Ordinal);
        Assert.Contains("<PackageReadmeFile", content, StringComparison.Ordinal);
        Assert.Contains("README.md", content, StringComparison.Ordinal);
        Assert.Contains("https://github.com/SymoHTL/Intergrated-S3", content, StringComparison.Ordinal);
    }

    [Fact]
    public void RepositoryReadme_LinksToConsumerGuides()
    {
        var content = ReadRepositoryFile("README.md");

        Assert.Contains("IntegratedS3", content, StringComparison.Ordinal);
        Assert.Contains("docs/getting-started.md", content, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("docs/protocol-compatibility.md", content, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void GettingStartedGuide_DocumentsRecommendedHostComposition()
    {
        var content = ReadRepositoryFile("docs", "getting-started.md");

        Assert.Contains("AddIntegratedS3", content, StringComparison.Ordinal);
        Assert.Contains("AddDiskStorage", content, StringComparison.Ordinal);
        Assert.Contains("MapIntegratedS3Endpoints", content, StringComparison.Ordinal);
    }

    [Fact]
    public void ProtocolCompatibilityGuide_DocumentsVersionAlignmentAndCapabilities()
    {
        var content = ReadRepositoryFile("docs", "protocol-compatibility.md");

        Assert.Contains("matching versions", content, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Signature Version 4", content, StringComparison.Ordinal);
        Assert.Contains("/integrated-s3/capabilities", content, StringComparison.Ordinal);
    }

    [Fact]
    public void AotGuidance_DocumentsCurrentDiagnostics()
    {
        var content = ReadRepositoryFile("docs", "aot-trimming-guidance.md");

        Assert.Contains("RequiresUnreferencedCode", content, StringComparison.Ordinal);
        Assert.Contains("dotnet publish -c Release --self-contained src\\IntegratedS3\\WebUi\\WebUi.csproj", content, StringComparison.Ordinal);
    }

    private static string ReadRepositoryFile(params string[] pathSegments)
    {
        return File.ReadAllText(Path.Combine(GetRepositoryRoot(), Path.Combine(pathSegments)));
    }

    private static string GetRepositoryRoot()
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

        throw new DirectoryNotFoundException("Could not locate the repository root for documentation validation.");
    }
}

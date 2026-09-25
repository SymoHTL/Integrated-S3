using IntegratedS3.Tests.Infrastructure;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// The S3 object semantics in <c>src/IntegratedS3/Shared</c> are defined once and compiled into each project that
/// needs them as linked source. A private copy drifts from the original unseen: <c>Crc32Accumulator</c> had four,
/// one in each of four shipped packages. These names may be declared only under <c>Shared/</c>.
/// </summary>
public sealed class SharedSourceConventionTests
{
    private static readonly string[] SharedTypes = ["Crc32Accumulator"];

    private static readonly string[] SharedMethods = ["BuildCompositeChecksum", "NormalizeRange"];

    private static readonly CSharpParseOptions ParseOptions = new(LanguageVersion.Preview);

    // ponytail: parses with the Microsoft.CodeAnalysis.CSharp that BenchmarkDotNet brings in through the
    // IntegratedS3.Benchmarks reference; add a direct PackageReference if that ever goes away. A copy is found by its
    // name only, so a renamed copy passes.
    [Fact]
    public void SharedTypesAndMethods_AreDeclaredOnlyUnderShared()
    {
        var sourceRoot = RepositoryRoot.Combine("src", "IntegratedS3");
        var files = Directory.EnumerateFiles(sourceRoot, "*.cs", SearchOption.AllDirectories)
            .Select(path => Path.GetRelativePath(sourceRoot, path))
            .Where(static path => !IsExcluded(path.Split(Path.DirectorySeparatorChar)))
            .ToArray();

        var declarations = files
            .SelectMany(path => CSharpSyntaxTree.ParseText(File.ReadAllText(Path.Combine(sourceRoot, path)), ParseOptions)
                .GetRoot()
                .DescendantNodes()
                .Select(DeclaredSharedName)
                .OfType<string>()
                .Select(name => $"{path.Replace('\\', '/')}: {name}"))
            .ToArray();

        Assert.True(files.Length > 0, $"The scan of {sourceRoot} read no .cs files.");
        Assert.True(
            declarations.Length == 0,
            "Declared outside src/IntegratedS3/Shared, which holds the one definition; link the shared file into the "
            + "project instead of keeping a copy:" + Environment.NewLine + string.Join(Environment.NewLine, declarations));
    }

    private static bool IsExcluded(string[] segments)
    {
        return segments[0].Equals("Shared", StringComparison.OrdinalIgnoreCase)
            || segments.Any(static segment => segment.Equals("bin", StringComparison.OrdinalIgnoreCase)
                || segment.Equals("obj", StringComparison.OrdinalIgnoreCase));
    }

    private static string? DeclaredSharedName(SyntaxNode node)
    {
        return node switch
        {
            BaseTypeDeclarationSyntax type when SharedTypes.Contains(type.Identifier.ValueText) => $"type {type.Identifier.ValueText}",
            MethodDeclarationSyntax method when SharedMethods.Contains(method.Identifier.ValueText) => $"method {method.Identifier.ValueText}",
            LocalFunctionStatementSyntax function when SharedMethods.Contains(function.Identifier.ValueText) => $"local function {function.Identifier.ValueText}",
            _ => null
        };
    }
}

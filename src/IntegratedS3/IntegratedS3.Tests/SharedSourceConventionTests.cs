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
            .SelectMany(path => Roots(File.ReadAllText(Path.Combine(sourceRoot, path)))
                .SelectMany(static root => root.DescendantNodes())
                .Select(DeclaredSharedName)
                .OfType<string>()
                .Select(name => $"{path.Replace('\\', '/')}: {name}"))
            .ToArray();

        Assert.True(files.Length > 0, $"The scan of {sourceRoot} read no .cs files.");
        Assert.True(
            declarations.Length == 0,
            "Declared outside src/IntegratedS3/Shared, which holds the one definition; link the shared file into the "
            + "project instead of keeping a copy, or rename a declaration that means something else:" + Environment.NewLine + string.Join(Environment.NewLine, declarations));

        // The names above guard only while Shared/ still declares each of them, exactly once.
        var definitions = Directory.EnumerateFiles(Path.Combine(sourceRoot, "Shared"), "*.cs", SearchOption.AllDirectories)
            .SelectMany(path => Roots(File.ReadAllText(path)).SelectMany(static root => root.DescendantNodes()))
            .Select(DeclaredSharedName)
            .OfType<string>()
            .Order(StringComparer.Ordinal)
            .ToArray();
        string[] expected = ["method BuildCompositeChecksum", "method NormalizeRange", "type Crc32Accumulator"];
        Assert.Equal(expected, definitions);
    }

    // Only a project's own output folders are generated; a source folder named bin or obj deeper down still compiles.
    private static bool IsExcluded(string[] segments)
    {
        return segments[0].Equals("Shared", StringComparison.OrdinalIgnoreCase)
            || (segments.Length > 2 && segments[1] is "bin" or "obj");
    }

    // The code under an inactive #if is trivia to the parser, so it is parsed again on its own.
    private static IEnumerable<SyntaxNode> Roots(string text)
    {
        var root = CSharpSyntaxTree.ParseText(text, ParseOptions).GetRoot();
        return root.DescendantTrivia()
            .Where(static trivia => trivia.IsKind(SyntaxKind.DisabledTextTrivia))
            .Select(static trivia => CSharpSyntaxTree.ParseText(trivia.ToString(), ParseOptions).GetRoot())
            .Prepend(root);
    }

    private static string? DeclaredSharedName(SyntaxNode node)
    {
        return node switch
        {
            BaseTypeDeclarationSyntax type when SharedTypes.Contains(type.Identifier.ValueText) => $"type {type.Identifier.ValueText}",
            DelegateDeclarationSyntax type when SharedTypes.Contains(type.Identifier.ValueText) => $"delegate {type.Identifier.ValueText}",
            MethodDeclarationSyntax method when SharedMethods.Contains(method.Identifier.ValueText) => $"method {method.Identifier.ValueText}",
            LocalFunctionStatementSyntax function when SharedMethods.Contains(function.Identifier.ValueText) => $"local function {function.Identifier.ValueText}",
            PropertyDeclarationSyntax property when SharedMethods.Contains(property.Identifier.ValueText) => $"property {property.Identifier.ValueText}",
            VariableDeclaratorSyntax variable when SharedMethods.Contains(variable.Identifier.ValueText) => $"field or local {variable.Identifier.ValueText}",
            EventDeclarationSyntax @event when SharedMethods.Contains(@event.Identifier.ValueText) => $"event {@event.Identifier.ValueText}",
            ParameterSyntax parameter when SharedMethods.Contains(parameter.Identifier.ValueText) => $"parameter {parameter.Identifier.ValueText}",
            SingleVariableDesignationSyntax variable when SharedMethods.Contains(variable.Identifier.ValueText) => $"pattern or out variable {variable.Identifier.ValueText}",
            ForEachStatementSyntax loop when SharedMethods.Contains(loop.Identifier.ValueText) => $"foreach variable {loop.Identifier.ValueText}",
            TupleElementSyntax element when SharedMethods.Contains(element.Identifier.ValueText) => $"tuple element {element.Identifier.ValueText}",
            AnonymousObjectMemberDeclaratorSyntax { NameEquals: { } member } when SharedMethods.Contains(member.Name.Identifier.ValueText) => $"anonymous member {member.Name.Identifier.ValueText}",
            _ => null
        };
    }
}

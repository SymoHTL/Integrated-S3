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
    // Checked against every kind of declaration: a copy may be a class NormalizeRange or a method Crc32Accumulator.
    private static readonly string[] SharedNames = ["BuildCompositeChecksum", "Crc32Accumulator", "NormalizeRange"];

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
                .Concat(Views(path, File.ReadAllText(Path.Combine(sourceRoot, path))))
                .SelectMany(static root => root.DescendantNodes())
                .Select(DeclaredSharedName)
                .OfType<string>()
                .Distinct()
                .Select(name => $"{path.Replace('\\', '/')}: {name}"))
            .ToArray();

        Assert.True(files.Length > 0, $"The scan of {sourceRoot} read no .cs files.");
        Assert.True(
            declarations.Length == 0,
            "Declared outside src/IntegratedS3/Shared, which holds the one definition; link the shared file into the "
            + "project instead of keeping a copy, or rename a declaration that means something else:" + Environment.NewLine + string.Join(Environment.NewLine, declarations));

        // The names above guard only while Shared/ still declares each of them, exactly once, in every build's view.
        var definitions = Directory.EnumerateFiles(Path.Combine(sourceRoot, "Shared"), "*.cs", SearchOption.AllDirectories)
            .SelectMany(static path => Views(path, File.ReadAllText(path))
                .SelectMany(static view => view.DescendantNodes().Select(DeclaredSharedName).OfType<string>().CountBy(static name => name))
                .GroupBy(static count => count.Key, static count => count.Value)
                .SelectMany(static name => Enumerable.Repeat(name.Key, name.Max())))
            .Order(StringComparer.Ordinal)
            .ToArray();
        string[] expected = ["method BuildCompositeChecksum", "method NormalizeRange", "type Crc32Accumulator"];
        Assert.Equal(expected, definitions);
    }

    // Only a project's own output folders are generated; a source folder named bin or obj deeper down still compiles.
    private static bool IsExcluded(string[] segments)
    {
        return segments[0] == "Shared"
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

    // A build compiles one view of a file: the code its defined symbols make active. The file is parsed once for
    // each combination of the symbols its #if and #elif directives name, so every build's view is read whole.
    private static List<SyntaxNode> Views(string path, string text)
    {
        var symbols = new SortedSet<string>(StringComparer.Ordinal);
        List<SyntaxNode> views;
        int known;
        do
        {
            known = symbols.Count;
            Assert.True(known <= 12, $"{path} names {known} preprocessor symbols; the gate parses at most 2^12 views of a file.");
            var names = symbols.ToArray();
            views = Enumerable.Range(0, 1 << names.Length)
                .Select(mask => CSharpSyntaxTree.ParseText(text, ParseOptions.WithPreprocessorSymbols(names.Where((_, i) => (mask & (1 << i)) != 0))).GetRoot())
                .ToList();
            foreach (var view in views)
            {
                symbols.UnionWith(view.DescendantTrivia()
                    .Select(static trivia => trivia.GetStructure())
                    .OfType<ConditionalDirectiveTriviaSyntax>()
                    .SelectMany(static directive => directive.Condition.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>())
                    .Select(static name => name.Identifier.ValueText));
            }
        }
        while (symbols.Count != known);
        return views;
    }

    private static string? DeclaredSharedName(SyntaxNode node)
    {
        return node switch
        {
            BaseTypeDeclarationSyntax type when SharedNames.Contains(type.Identifier.ValueText) => $"type {type.Identifier.ValueText}",
            DelegateDeclarationSyntax type when SharedNames.Contains(type.Identifier.ValueText) => $"delegate {type.Identifier.ValueText}",
            MethodDeclarationSyntax method when SharedNames.Contains(method.Identifier.ValueText) => $"method {method.Identifier.ValueText}",
            LocalFunctionStatementSyntax function when SharedNames.Contains(function.Identifier.ValueText) => $"local function {function.Identifier.ValueText}",
            PropertyDeclarationSyntax property when SharedNames.Contains(property.Identifier.ValueText) => $"property {property.Identifier.ValueText}",
            VariableDeclaratorSyntax variable when SharedNames.Contains(variable.Identifier.ValueText) => $"field or local {variable.Identifier.ValueText}",
            EventDeclarationSyntax @event when SharedNames.Contains(@event.Identifier.ValueText) => $"event {@event.Identifier.ValueText}",
            ParameterSyntax parameter when SharedNames.Contains(parameter.Identifier.ValueText) => $"parameter {parameter.Identifier.ValueText}",
            SingleVariableDesignationSyntax variable when SharedNames.Contains(variable.Identifier.ValueText) => $"pattern or out variable {variable.Identifier.ValueText}",
            ForEachStatementSyntax loop when SharedNames.Contains(loop.Identifier.ValueText) => $"foreach variable {loop.Identifier.ValueText}",
            TupleElementSyntax element when SharedNames.Contains(element.Identifier.ValueText) => $"tuple element {element.Identifier.ValueText}",
            AnonymousObjectMemberDeclaratorSyntax { NameEquals: { } member } when SharedNames.Contains(member.Name.Identifier.ValueText) => $"anonymous member {member.Name.Identifier.ValueText}",
            ArgumentSyntax { NameColon: { } element, Parent: TupleExpressionSyntax } when SharedNames.Contains(element.Name.Identifier.ValueText) => $"tuple element {element.Name.Identifier.ValueText}",
            FromClauseSyntax clause when SharedNames.Contains(clause.Identifier.ValueText) => $"query variable {clause.Identifier.ValueText}",
            LetClauseSyntax clause when SharedNames.Contains(clause.Identifier.ValueText) => $"query variable {clause.Identifier.ValueText}",
            JoinClauseSyntax clause when SharedNames.Contains(clause.Identifier.ValueText) => $"query variable {clause.Identifier.ValueText}",
            JoinIntoClauseSyntax clause when SharedNames.Contains(clause.Identifier.ValueText) => $"query variable {clause.Identifier.ValueText}",
            QueryContinuationSyntax continuation when SharedNames.Contains(continuation.Identifier.ValueText) => $"query variable {continuation.Identifier.ValueText}",
            _ => null
        };
    }
}

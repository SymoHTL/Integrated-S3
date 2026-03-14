using System.Xml.Linq;
using Xunit;

namespace IntegratedS3.Tests;

internal static class S3XmlTestHelper
{
    internal const string CanonicalS3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/";

    private static readonly XNamespace S3XmlNamespace = CanonicalS3Namespace;

    internal static void AssertRoot(XDocument document, string localName)
    {
        ArgumentNullException.ThrowIfNull(document);
        ArgumentException.ThrowIfNullOrWhiteSpace(localName);

        Assert.Equal(localName, document.Root?.Name.LocalName);
        Assert.Equal(CanonicalS3Namespace, document.Root?.Name.NamespaceName);
    }

    internal static XElement? S3Element(this XContainer? container, string localName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localName);

        return container?.Element(S3XmlNamespace + localName);
    }

    internal static IEnumerable<XElement> S3Elements(this XContainer? container, string localName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localName);

        return container is null
            ? Array.Empty<XElement>()
            : container.Elements(S3XmlNamespace + localName);
    }
}

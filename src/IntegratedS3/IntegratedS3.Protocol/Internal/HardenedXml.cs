using System.Xml;
using System.Xml.Linq;

namespace IntegratedS3.Protocol.Internal;

/// <summary>
/// Central factory for loading untrusted S3 request XML with DTD processing and entity
/// expansion disabled, preventing "billion laughs" / entity-expansion denial-of-service and
/// XXE. All S3 XML request parsers must route through these helpers rather than calling
/// <see cref="XDocument.LoadAsync(Stream, LoadOptions, CancellationToken)"/> or
/// <see cref="XDocument.Parse(string)"/> directly on attacker-controlled input.
/// </summary>
public static class HardenedXml
{
    /// <summary>
    /// Builds <see cref="XmlReaderSettings"/> that reject any inline <c>DOCTYPE</c>
    /// (<see cref="DtdProcessing.Prohibit"/>), refuse to resolve external resources
    /// (<see cref="XmlResolver"/> = <see langword="null"/>), and forbid entity-expansion
    /// amplification (<c>MaxCharactersFromEntities = 1</c>). This matches AWS S3, which rejects
    /// request XML containing a DTD.
    /// </summary>
    private static XmlReaderSettings CreateSettings(bool async) => new()
    {
        Async = async,
        DtdProcessing = DtdProcessing.Prohibit,
        XmlResolver = null,
        MaxCharactersFromEntities = 1,
        CloseInput = false,
    };

    /// <summary>Asynchronously loads an <see cref="XDocument"/> from a stream with hardened settings.</summary>
    /// <param name="content">The stream containing the untrusted XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The parsed document.</returns>
    public static async Task<XDocument> LoadAsync(Stream content, CancellationToken cancellationToken = default)
    {
        using var reader = XmlReader.Create(content, CreateSettings(async: true));
        return await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken);
    }

    /// <summary>Parses an <see cref="XDocument"/> from a string with hardened settings.</summary>
    /// <param name="xml">The untrusted XML payload.</param>
    /// <returns>The parsed document.</returns>
    public static XDocument Parse(string xml)
    {
        using var stringReader = new StringReader(xml);
        using var reader = XmlReader.Create(stringReader, CreateSettings(async: false));
        return XDocument.Load(reader, LoadOptions.None);
    }
}

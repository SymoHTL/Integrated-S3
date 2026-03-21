namespace IntegratedS3.Protocol;

/// <summary>
/// Parses raw query strings into ordered key-value pairs with proper URI decoding,
/// suitable for canonical query string construction during SigV4 signing.
/// </summary>
public static class S3SigV4QueryStringParser
{
    /// <summary>
    /// Parses a raw query string into an ordered list of decoded key-value pairs.
    /// </summary>
    /// <param name="rawQuery">The raw query string, optionally prefixed with <c>?</c>. May be <see langword="null"/> or empty.</param>
    /// <returns>An ordered list of decoded query parameter key-value pairs.</returns>
    public static IReadOnlyList<KeyValuePair<string, string?>> Parse(string? rawQuery)
    {
        if (string.IsNullOrEmpty(rawQuery) || string.Equals(rawQuery, "?", StringComparison.Ordinal)) {
            return Array.Empty<KeyValuePair<string, string?>>();
        }

        var query = rawQuery.AsSpan();
        if (query[0] == '?') {
            query = query[1..];
        }

        var parameters = new List<KeyValuePair<string, string?>>();
        while (!query.IsEmpty) {
            var separatorIndex = query.IndexOf('&');
            ReadOnlySpan<char> segment;
            if (separatorIndex < 0) {
                segment = query;
                query = ReadOnlySpan<char>.Empty;
            }
            else {
                segment = query[..separatorIndex];
                query = query[(separatorIndex + 1)..];
            }

            if (segment.IsEmpty) {
                continue;
            }

            var assignmentIndex = segment.IndexOf('=');
            var rawKey = assignmentIndex < 0 ? segment : segment[..assignmentIndex];
            var rawValue = assignmentIndex < 0 ? ReadOnlySpan<char>.Empty : segment[(assignmentIndex + 1)..];
            parameters.Add(new KeyValuePair<string, string?>(DecodeComponent(rawKey), DecodeComponent(rawValue)));
        }

        return parameters;
    }

    private static string DecodeComponent(ReadOnlySpan<char> component)
    {
        return component.IsEmpty
            ? string.Empty
            : Uri.UnescapeDataString(component.ToString());
    }
}

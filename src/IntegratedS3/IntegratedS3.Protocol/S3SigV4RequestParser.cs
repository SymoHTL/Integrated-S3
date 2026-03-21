using System.Globalization;

namespace IntegratedS3.Protocol;

/// <summary>
/// Parses incoming S3 request Authorization headers and presigned query parameters to extract
/// SigV4 and SigV4a signing information such as credentials, signed headers, and signatures.
/// </summary>
public static class S3SigV4RequestParser
{
    private const string AlgorithmName = "AWS4-HMAC-SHA256";
    private const string SigV4aAlgorithmName = "AWS4-ECDSA-P256-SHA256";

    /// <summary>
    /// Determines whether a request uses SigV4a signing by inspecting the Authorization header
    /// or query-string algorithm parameter for the <c>AWS4-ECDSA-P256-SHA256</c> algorithm.
    /// </summary>
    /// <param name="authorizationHeader">The value of the Authorization header, or <see langword="null"/> if absent.</param>
    /// <param name="queryAlgorithm">The value of the <c>X-Amz-Algorithm</c> query parameter, or <see langword="null"/> if absent.</param>
    /// <returns><see langword="true"/> if the request uses SigV4a; otherwise, <see langword="false"/>.</returns>
    public static bool IsSigV4aRequest(string? authorizationHeader, string? queryAlgorithm)
    {
        var normalizedAuthorizationHeader = authorizationHeader?.Trim();
        if (!string.IsNullOrEmpty(normalizedAuthorizationHeader) &&
            normalizedAuthorizationHeader.StartsWith(SigV4aAlgorithmName, StringComparison.Ordinal))
            return true;

        var normalizedQueryAlgorithm = queryAlgorithm?.Trim();
        if (!string.IsNullOrEmpty(normalizedQueryAlgorithm) &&
            string.Equals(normalizedQueryAlgorithm, SigV4aAlgorithmName, StringComparison.Ordinal))
            return true;

        return false;
    }

    /// <summary>
    /// Attempts to parse a SigV4 or SigV4a Authorization header value without request headers context.
    /// </summary>
    /// <param name="authorizationHeader">The raw Authorization header value.</param>
    /// <param name="authorization">When successful, the parsed authorization data; otherwise, <see langword="null"/>.</param>
    /// <param name="error">A description of the parse failure, or <see langword="null"/> if parsing succeeded or the header was absent.</param>
    /// <returns>
    /// <see langword="true"/> if the header was recognized (even if malformed — check <paramref name="error"/>);
    /// <see langword="false"/> if no Authorization header was present.
    /// </returns>
    public static bool TryParseAuthorizationHeader(string? authorizationHeader, out S3SigV4AuthorizationHeader? authorization, out string? error)
    {
        return TryParseAuthorizationHeader(authorizationHeader, [], out authorization, out error);
    }

    /// <summary>
    /// Attempts to parse a SigV4 or SigV4a Authorization header value, also extracting the security token
    /// and region set from request headers when present.
    /// </summary>
    /// <param name="authorizationHeader">The raw Authorization header value.</param>
    /// <param name="requestHeaders">The request headers used to extract <c>x-amz-security-token</c> and <c>x-amz-region-set</c>.</param>
    /// <param name="authorization">When successful, the parsed authorization data; otherwise, <see langword="null"/>.</param>
    /// <param name="error">A description of the parse failure, or <see langword="null"/> if parsing succeeded or the header was absent.</param>
    /// <returns>
    /// <see langword="true"/> if the header was recognized (even if malformed — check <paramref name="error"/>);
    /// <see langword="false"/> if no Authorization header was present.
    /// </returns>
    public static bool TryParseAuthorizationHeader(
        string? authorizationHeader,
        IEnumerable<KeyValuePair<string, string?>> requestHeaders,
        out S3SigV4AuthorizationHeader? authorization,
        out string? error)
    {
        ArgumentNullException.ThrowIfNull(requestHeaders);

        authorization = null;
        error = null;

        if (string.IsNullOrWhiteSpace(authorizationHeader)) {
            return false;
        }

        var trimmed = authorizationHeader.Trim();

        if (trimmed.StartsWith(SigV4aAlgorithmName, StringComparison.Ordinal)) {
            return TryParseSigV4aAuthorizationHeader(trimmed, requestHeaders, out authorization, out error);
        }

        if (!trimmed.StartsWith(AlgorithmName, StringComparison.Ordinal)) {
            error = "Only AWS4-HMAC-SHA256 authorization headers are supported.";
            return true;
        }

        var parameterSection = trimmed[AlgorithmName.Length..].TrimStart();
        var parameters = parameterSection.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(static part => part.Split('=', 2, StringSplitOptions.TrimEntries))
            .ToDictionary(
                static parts => parts[0],
                static parts => parts.Length > 1 ? parts[1] : string.Empty,
                StringComparer.Ordinal);

        if (!parameters.TryGetValue("Credential", out var credential)
            || !TryParseCredentialScope(credential, out var credentialScope, out error)) {
            error ??= "The authorization header must include a valid Credential component.";
            return true;
        }

        if (!parameters.TryGetValue("SignedHeaders", out var signedHeadersText)) {
            error = "The authorization header must include SignedHeaders.";
            return true;
        }

        var signedHeaders = ParseSignedHeaders(signedHeadersText);
        if (signedHeaders.Count == 0) {
            error = "The authorization header must include at least one signed header.";
            return true;
        }

        if (!parameters.TryGetValue("Signature", out var signature) || string.IsNullOrWhiteSpace(signature)) {
            error = "The authorization header must include Signature.";
            return true;
        }

        string? securityToken = null;
        foreach (var header in requestHeaders) {
            if (string.Equals(header.Key, "x-amz-security-token", StringComparison.OrdinalIgnoreCase)) {
                securityToken = header.Value;
                break;
            }
        }

        authorization = new S3SigV4AuthorizationHeader
        {
            Algorithm = AlgorithmName,
            CredentialScope = credentialScope!,
            SignedHeaders = signedHeaders,
            Signature = signature.Trim(),
            SecurityToken = NormalizeOptionalValue(securityToken)
        };

        return true;
    }

    /// <summary>
    /// Attempts to parse SigV4 or SigV4a presigned request parameters from query string key-value pairs.
    /// </summary>
    /// <param name="queryParameters">The query string parameters from the request URL.</param>
    /// <param name="presignedRequest">When successful, the parsed presigned request data; otherwise, <see langword="null"/>.</param>
    /// <param name="error">A description of the parse failure, or <see langword="null"/> if parsing succeeded or no presigned parameters were present.</param>
    /// <returns>
    /// <see langword="true"/> if presigned parameters were recognized (even if malformed — check <paramref name="error"/>);
    /// <see langword="false"/> if no <c>X-Amz-Algorithm</c> query parameter was present.
    /// </returns>
    public static bool TryParsePresignedRequest(IEnumerable<KeyValuePair<string, string?>> queryParameters, out S3SigV4PresignedRequest? presignedRequest, out string? error)
    {
        ArgumentNullException.ThrowIfNull(queryParameters);

        presignedRequest = null;
        error = null;

        var query = queryParameters
            .GroupBy(static parameter => parameter.Key, StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                static group => group.Select(static parameter => parameter.Value).FirstOrDefault(),
                StringComparer.Ordinal);

        if (!TryGetQueryValue(query, "X-Amz-Algorithm", out var algorithm)) {
            return false;
        }

        if (!string.Equals(algorithm, AlgorithmName, StringComparison.Ordinal)) {
            if (string.Equals(algorithm, SigV4aAlgorithmName, StringComparison.Ordinal)) {
                return TryParseSigV4aPresignedRequest(query, out presignedRequest, out error);
            }

            error = "Only AWS4-HMAC-SHA256 presigned requests are supported.";
            return true;
        }

        if (!TryGetQueryValue(query, "X-Amz-Credential", out var credential)
            || !TryParseCredentialScope(credential, out var credentialScope, out error)) {
            error ??= "The presigned request must include a valid X-Amz-Credential value.";
            return true;
        }

        if (!TryGetQueryValue(query, "X-Amz-Date", out var dateText)
            || !DateTimeOffset.TryParseExact(dateText, "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var signedAtUtc)) {
            error = "The presigned request must include a valid X-Amz-Date value.";
            return true;
        }

        if (!TryGetQueryValue(query, "X-Amz-Expires", out var expiresText)
            || !int.TryParse(expiresText, NumberStyles.None, CultureInfo.InvariantCulture, out var expiresSeconds)
            || expiresSeconds <= 0) {
            error = "The presigned request must include a valid X-Amz-Expires value.";
            return true;
        }

        if (!TryGetQueryValue(query, "X-Amz-SignedHeaders", out var signedHeadersText)) {
            error = "The presigned request must include X-Amz-SignedHeaders.";
            return true;
        }

        var signedHeaders = ParseSignedHeaders(signedHeadersText);
        if (signedHeaders.Count == 0) {
            error = "The presigned request must include at least one signed header.";
            return true;
        }

        if (!TryGetQueryValue(query, "X-Amz-Signature", out var signature) || string.IsNullOrWhiteSpace(signature)) {
            error = "The presigned request must include X-Amz-Signature.";
            return true;
        }

        query.TryGetValue("X-Amz-Security-Token", out var securityTokenValues);

        presignedRequest = new S3SigV4PresignedRequest
        {
            Algorithm = AlgorithmName,
            CredentialScope = credentialScope!,
            SignedAtUtc = signedAtUtc,
            ExpiresSeconds = expiresSeconds,
            SignedHeaders = signedHeaders,
            Signature = signature.Trim(),
            SecurityToken = NormalizeOptionalValue(securityTokenValues)
        };

        return true;
    }

    private static bool TryParseCredentialScope(string? value, out S3SigV4CredentialScope? credentialScope, out string? error)
    {
        credentialScope = null;
        error = null;

        if (string.IsNullOrWhiteSpace(value)) {
            error = "The credential scope is required.";
            return false;
        }

        var segments = value.Split('/', StringSplitOptions.TrimEntries);
        if (segments.Length != 5) {
            error = "The credential scope must be in the form '<access-key>/<date>/<region>/<service>/aws4_request'.";
            return false;
        }

        credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = segments[0],
            DateStamp = segments[1],
            Region = segments[2],
            Service = segments[3],
            Terminator = segments[4]
        };

        return true;
    }

    private static IReadOnlyList<string> ParseSignedHeaders(string signedHeadersText)
    {
        return signedHeadersText.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(static header => header.ToLowerInvariant())
            .Distinct(StringComparer.Ordinal)
            .ToArray();
    }

    private static bool TryGetQueryValue(IReadOnlyDictionary<string, string?> query, string key, out string value)
    {
        if (query.TryGetValue(key, out var rawValue) && !string.IsNullOrWhiteSpace(rawValue)) {
            value = rawValue;
            return true;
        }

        value = string.Empty;
        return false;
    }

    private static string? NormalizeOptionalValue(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private static bool TryParseSigV4aAuthorizationHeader(
        string trimmed,
        IEnumerable<KeyValuePair<string, string?>> requestHeaders,
        out S3SigV4AuthorizationHeader? authorization,
        out string? error)
    {
        authorization = null;
        error = null;

        var parameterSection = trimmed[SigV4aAlgorithmName.Length..].TrimStart();
        var parameters = parameterSection.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(static part => part.Split('=', 2, StringSplitOptions.TrimEntries))
            .ToDictionary(
                static parts => parts[0],
                static parts => parts.Length > 1 ? parts[1] : string.Empty,
                StringComparer.Ordinal);

        if (!parameters.TryGetValue("Credential", out var credential)
            || !TryParseSigV4aCredentialScope(credential, out var credentialScope, out error)) {
            error ??= "The authorization header must include a valid Credential component.";
            return true;
        }

        if (!parameters.TryGetValue("SignedHeaders", out var signedHeadersText)) {
            error = "The authorization header must include SignedHeaders.";
            return true;
        }

        var signedHeaders = ParseSignedHeaders(signedHeadersText);
        if (signedHeaders.Count == 0) {
            error = "The authorization header must include at least one signed header.";
            return true;
        }

        if (!parameters.TryGetValue("Signature", out var signature) || string.IsNullOrWhiteSpace(signature)) {
            error = "The authorization header must include Signature.";
            return true;
        }

        string? regionSetHeader = null;
        string? securityToken = null;
        foreach (var header in requestHeaders) {
            if (string.Equals(header.Key, "x-amz-region-set", StringComparison.OrdinalIgnoreCase)) {
                regionSetHeader = header.Value;
            }
            if (string.Equals(header.Key, "x-amz-security-token", StringComparison.OrdinalIgnoreCase)) {
                securityToken = header.Value;
            }
        }

        authorization = new S3SigV4AuthorizationHeader
        {
            Algorithm = SigV4aAlgorithmName,
            CredentialScope = credentialScope!,
            SignedHeaders = signedHeaders,
            Signature = signature.Trim(),
            SecurityToken = NormalizeOptionalValue(securityToken),
            RegionSet = ParseRegionSet(regionSetHeader)
        };

        return true;
    }

    private static bool TryParseSigV4aPresignedRequest(
        IReadOnlyDictionary<string, string?> query,
        out S3SigV4PresignedRequest? presignedRequest,
        out string? error)
    {
        presignedRequest = null;
        error = null;

        if (!TryGetQueryValue(query, "X-Amz-Credential", out var credential)
            || !TryParseSigV4aCredentialScope(credential, out var credentialScope, out error)) {
            error ??= "The presigned request must include a valid X-Amz-Credential value.";
            return true;
        }

        if (!TryGetQueryValue(query, "X-Amz-Date", out var dateText)
            || !DateTimeOffset.TryParseExact(dateText, "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var signedAtUtc)) {
            error = "The presigned request must include a valid X-Amz-Date value.";
            return true;
        }

        if (!TryGetQueryValue(query, "X-Amz-Expires", out var expiresText)
            || !int.TryParse(expiresText, NumberStyles.None, CultureInfo.InvariantCulture, out var expiresSeconds)
            || expiresSeconds <= 0) {
            error = "The presigned request must include a valid X-Amz-Expires value.";
            return true;
        }

        if (!TryGetQueryValue(query, "X-Amz-SignedHeaders", out var signedHeadersText)) {
            error = "The presigned request must include X-Amz-SignedHeaders.";
            return true;
        }

        var signedHeaders = ParseSignedHeaders(signedHeadersText);
        if (signedHeaders.Count == 0) {
            error = "The presigned request must include at least one signed header.";
            return true;
        }

        if (!TryGetQueryValue(query, "X-Amz-Signature", out var signature) || string.IsNullOrWhiteSpace(signature)) {
            error = "The presigned request must include X-Amz-Signature.";
            return true;
        }

        query.TryGetValue("X-Amz-Security-Token", out var securityTokenValues);
        query.TryGetValue("X-Amz-Region-Set", out var regionSetValue);

        presignedRequest = new S3SigV4PresignedRequest
        {
            Algorithm = SigV4aAlgorithmName,
            CredentialScope = credentialScope!,
            SignedAtUtc = signedAtUtc,
            ExpiresSeconds = expiresSeconds,
            SignedHeaders = signedHeaders,
            Signature = signature.Trim(),
            SecurityToken = NormalizeOptionalValue(securityTokenValues),
            RegionSet = ParseRegionSet(regionSetValue)
        };

        return true;
    }

    private static bool TryParseSigV4aCredentialScope(string? value, out S3SigV4CredentialScope? credentialScope, out string? error)
    {
        credentialScope = null;
        error = null;

        if (string.IsNullOrWhiteSpace(value)) {
            error = "The credential scope is required.";
            return false;
        }

        var segments = value.Split('/', StringSplitOptions.TrimEntries);
        if (segments.Length != 4) {
            error = "The SigV4a credential scope must be in the form '<access-key>/<date>/<service>/aws4_request'.";
            return false;
        }

        credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = segments[0],
            DateStamp = segments[1],
            Region = "*",
            Service = segments[2],
            Terminator = segments[3]
        };

        return true;
    }

    private static IReadOnlyList<string>? ParseRegionSet(string? regionSetValue)
    {
        if (string.IsNullOrWhiteSpace(regionSetValue)) {
            return null;
        }

        return regionSetValue.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }
}

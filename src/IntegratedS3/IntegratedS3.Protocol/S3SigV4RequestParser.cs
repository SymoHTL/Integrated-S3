using System.Globalization;

namespace IntegratedS3.Protocol;

public static class S3SigV4RequestParser
{
    private const string AlgorithmName = "AWS4-HMAC-SHA256";

    public static bool TryParseAuthorizationHeader(string? authorizationHeader, out S3SigV4AuthorizationHeader? authorization, out string? error)
    {
        return TryParseAuthorizationHeader(authorizationHeader, [], out authorization, out error);
    }

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
            || expiresSeconds < 0) {
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
}

using System.Security.Cryptography;
using System.Text;

namespace IntegratedS3.Protocol;

public static class S3SigV4Signer
{
    public static S3SigV4CanonicalRequest BuildCanonicalRequest(
        string httpMethod,
        string path,
        IEnumerable<KeyValuePair<string, string?>> queryParameters,
        IEnumerable<KeyValuePair<string, string?>> headers,
        IReadOnlyList<string> signedHeaders,
        string payloadHash,
        string? unsignedQueryKey = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(httpMethod);
        ArgumentNullException.ThrowIfNull(queryParameters);
        ArgumentNullException.ThrowIfNull(headers);
        ArgumentNullException.ThrowIfNull(signedHeaders);
        ArgumentException.ThrowIfNullOrWhiteSpace(payloadHash);

        var canonicalUri = BuildCanonicalUri(path);
        var canonicalQueryString = BuildCanonicalQueryString(queryParameters, unsignedQueryKey);
        var canonicalHeaders = BuildCanonicalHeaders(headers, signedHeaders);
        var normalizedSignedHeaders = string.Join(';', signedHeaders.Select(static header => header.ToLowerInvariant()).OrderBy(static header => header, StringComparer.Ordinal));
        var canonicalRequest = string.Join('\n', [
            httpMethod.ToUpperInvariant(),
            canonicalUri,
            canonicalQueryString,
            canonicalHeaders,
            normalizedSignedHeaders,
            payloadHash
        ]);

        return new S3SigV4CanonicalRequest
        {
            CanonicalRequest = canonicalRequest,
            CanonicalRequestHashHex = ComputeSha256Hex(canonicalRequest),
            CanonicalUri = canonicalUri,
            CanonicalQueryString = canonicalQueryString,
            CanonicalHeaders = canonicalHeaders,
            SignedHeaders = normalizedSignedHeaders,
            PayloadHash = payloadHash
        };
    }

    public static string BuildStringToSign(string algorithm, DateTimeOffset requestTimestampUtc, S3SigV4CredentialScope credentialScope, string canonicalRequestHashHex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(algorithm);
        ArgumentNullException.ThrowIfNull(credentialScope);
        ArgumentException.ThrowIfNullOrWhiteSpace(canonicalRequestHashHex);

        return string.Join('\n', [
            algorithm,
            requestTimestampUtc.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'"),
            credentialScope.Scope,
            canonicalRequestHashHex
        ]);
    }

    public static string ComputeSignature(string secretAccessKey, S3SigV4CredentialScope credentialScope, string stringToSign)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(secretAccessKey);
        ArgumentNullException.ThrowIfNull(credentialScope);
        ArgumentException.ThrowIfNullOrWhiteSpace(stringToSign);

        var kSecret = Encoding.UTF8.GetBytes($"AWS4{secretAccessKey}");
        var kDate = ComputeHmacSha256(kSecret, credentialScope.DateStamp);
        var kRegion = ComputeHmacSha256(kDate, credentialScope.Region);
        var kService = ComputeHmacSha256(kRegion, credentialScope.Service);
        var kSigning = ComputeHmacSha256(kService, credentialScope.Terminator);
        return Convert.ToHexStringLower(ComputeHmacSha256(kSigning, stringToSign));
    }

    public static string ComputeSha256Hex(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
    }

    public static string ComputeSha256Hex(ReadOnlySpan<byte> value)
    {
        return Convert.ToHexStringLower(SHA256.HashData(value));
    }

    private static byte[] ComputeHmacSha256(byte[] key, string value)
    {
        using var hmac = new HMACSHA256(key);
        return hmac.ComputeHash(Encoding.UTF8.GetBytes(value));
    }

    private static string BuildCanonicalUri(string path)
    {
        var normalized = string.IsNullOrWhiteSpace(path) ? "/" : path;
        if (!normalized.StartsWith('/')) {
            normalized = $"/{normalized}";
        }

        var segments = normalized.Split('/');
        return string.Join('/', segments.Select(AwsEncodePathSegment));
    }

    private static string BuildCanonicalQueryString(IEnumerable<KeyValuePair<string, string?>> queryParameters, string? unsignedQueryKey)
    {
        return string.Join("&", queryParameters
            .Where(parameter => unsignedQueryKey is null || !string.Equals(parameter.Key, unsignedQueryKey, StringComparison.Ordinal))
            .Select(parameter => new KeyValuePair<string, string>(AwsEncodeQueryComponent(parameter.Key), AwsEncodeQueryComponent(parameter.Value ?? string.Empty)))
            .OrderBy(static parameter => parameter.Key, StringComparer.Ordinal)
            .ThenBy(static parameter => parameter.Value, StringComparer.Ordinal)
            .Select(static parameter => $"{parameter.Key}={parameter.Value}"));
    }

    private static string BuildCanonicalHeaders(IEnumerable<KeyValuePair<string, string?>> headers, IReadOnlyList<string> signedHeaders)
    {
        var signedHeaderSet = signedHeaders.Select(static header => header.ToLowerInvariant()).ToHashSet(StringComparer.Ordinal);
        var normalizedHeaders = headers
            .Select(static header => new KeyValuePair<string, string>(header.Key.ToLowerInvariant(), NormalizeHeaderValue(header.Value)))
            .Where(header => signedHeaderSet.Contains(header.Key))
            .GroupBy(static header => header.Key, StringComparer.Ordinal)
            .OrderBy(static group => group.Key, StringComparer.Ordinal)
            .Select(static group => $"{group.Key}:{string.Join(',', group.Select(static header => header.Value))}");

        return string.Join('\n', normalizedHeaders) + "\n";
    }

    private static string NormalizeHeaderValue(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) {
            return string.Empty;
        }

        return string.Join(' ', value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
    }

    private static string AwsEncodePathSegment(string segment)
    {
        if (segment.Length == 0) {
            return string.Empty;
        }

        return AwsEncodeComponent(segment, encodeSlash: false);
    }

    private static string AwsEncodeQueryComponent(string value)
    {
        return AwsEncodeComponent(value, encodeSlash: true);
    }

    private static string AwsEncodeComponent(string value, bool encodeSlash)
    {
        var builder = new StringBuilder(value.Length * 2);
        var buffer = new byte[8];
        foreach (var rune in value.EnumerateRunes()) {
            if (IsUnreserved(rune)) {
                builder.Append(rune.ToString());
                continue;
            }

            if (!encodeSlash && rune.Value == '/') {
                builder.Append('/');
                continue;
            }

            var written = Encoding.UTF8.GetBytes(rune.ToString(), buffer);
            for (var index = 0; index < written; index++) {
                builder.Append('%');
                builder.Append(buffer[index].ToString("X2"));
            }
        }

        return builder.ToString();
    }

    private static bool IsUnreserved(Rune rune)
    {
        return rune.Value is >= 'A' and <= 'Z'
            or >= 'a' and <= 'z'
            or >= '0' and <= '9'
            or '-'
            or '_'
            or '.'
            or '~';
    }
}

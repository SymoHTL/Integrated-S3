using System.Security.Cryptography;
using System.Text;

namespace IntegratedS3.Protocol;

public static class S3SigV4Signer
{
    private const string TrailerSignatureHeaderName = "x-amz-trailer-signature";

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

    public static string BuildStreamingPayloadStringToSign(
        DateTimeOffset requestTimestampUtc,
        S3SigV4CredentialScope credentialScope,
        string previousSignature,
        string chunkContentHashHex)
    {
        ArgumentNullException.ThrowIfNull(credentialScope);
        ArgumentException.ThrowIfNullOrWhiteSpace(previousSignature);
        ArgumentException.ThrowIfNullOrWhiteSpace(chunkContentHashHex);

        return string.Join('\n', [
            "AWS4-HMAC-SHA256-PAYLOAD",
            requestTimestampUtc.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'"),
            credentialScope.Scope,
            previousSignature,
            ComputeSha256Hex(string.Empty),
            chunkContentHashHex
        ]);
    }

    public static string BuildStreamingTrailerStringToSign(
        DateTimeOffset requestTimestampUtc,
        S3SigV4CredentialScope credentialScope,
        string previousSignature,
        string trailerHeadersHashHex)
    {
        ArgumentNullException.ThrowIfNull(credentialScope);
        ArgumentException.ThrowIfNullOrWhiteSpace(previousSignature);
        ArgumentException.ThrowIfNullOrWhiteSpace(trailerHeadersHashHex);

        return string.Join('\n', [
            "AWS4-HMAC-SHA256-TRAILER",
            requestTimestampUtc.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'"),
            credentialScope.Scope,
            previousSignature,
            trailerHeadersHashHex
        ]);
    }

    public static string BuildCanonicalStreamingTrailerHeaders(IEnumerable<KeyValuePair<string, string>> trailerHeaders)
    {
        ArgumentNullException.ThrowIfNull(trailerHeaders);

        var canonicalTrailerHeaders = trailerHeaders
            .Where(header => !string.Equals(header.Key, TrailerSignatureHeaderName, StringComparison.OrdinalIgnoreCase))
            .Select(static header => new KeyValuePair<string, string>(header.Key.ToLowerInvariant(), NormalizeHeaderValue(header.Value)))
            .GroupBy(static header => header.Key, StringComparer.Ordinal)
            .OrderBy(static group => group.Key, StringComparer.Ordinal)
            .Select(static group => $"{group.Key}:{string.Join(',', group.Select(static header => header.Value))}\n");

        return string.Concat(canonicalTrailerHeaders);
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

        var builder = new StringBuilder(segment.Length * 2);
        var buffer = new byte[8];
        for (var index = 0; index < segment.Length;) {
            if (IsEncodedOctet(segment, index)) {
                builder.Append('%');
                builder.Append(char.ToUpperInvariant(segment[index + 1]));
                builder.Append(char.ToUpperInvariant(segment[index + 2]));
                index += 3;
                continue;
            }

            var rune = Rune.GetRuneAt(segment, index);
            index += rune.Utf16SequenceLength;
            AppendEncodedRune(builder, buffer, rune, encodeSlash: false);
        }

        return builder.ToString();
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
            AppendEncodedRune(builder, buffer, rune, encodeSlash);
        }

        return builder.ToString();
    }

    private static bool IsEncodedOctet(string value, int index)
    {
        return index <= value.Length - 3
            && value[index] == '%'
            && Uri.IsHexDigit(value[index + 1])
            && Uri.IsHexDigit(value[index + 2]);
    }

    private static void AppendEncodedRune(StringBuilder builder, byte[] buffer, Rune rune, bool encodeSlash)
    {
        if (IsUnreserved(rune)) {
            builder.Append(rune.ToString());
            return;
        }

        if (!encodeSlash && rune.Value == '/') {
            builder.Append('/');
            return;
        }

        var written = Encoding.UTF8.GetBytes(rune.ToString(), buffer);
        for (var index = 0; index < written; index++) {
            builder.Append('%');
            builder.Append(buffer[index].ToString("X2"));
        }
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

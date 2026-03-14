using IntegratedS3.Protocol;
using Microsoft.AspNetCore.WebUtilities;

namespace IntegratedS3.Benchmarks;

internal sealed class SigV4RequestTemplate
{
    private static readonly string EmptyPayloadHash = S3SigV4Signer.ComputeSha256Hex(ReadOnlySpan<byte>.Empty);

    private readonly HttpMethod _method;
    private readonly string _pathAndQuery;
    private readonly string _hostHeader;
    private readonly string _authorizationHeader;
    private readonly string _timestampHeader;
    private readonly string? _payloadHashHeader;
    private readonly byte[]? _contentBytes;
    private readonly string? _contentType;

    private SigV4RequestTemplate(
        HttpMethod method,
        string pathAndQuery,
        string hostHeader,
        string authorizationHeader,
        string timestampHeader,
        string? payloadHashHeader,
        byte[]? contentBytes,
        string? contentType)
    {
        _method = method;
        _pathAndQuery = pathAndQuery;
        _hostHeader = hostHeader;
        _authorizationHeader = authorizationHeader;
        _timestampHeader = timestampHeader;
        _payloadHashHeader = payloadHashHeader;
        _contentBytes = contentBytes;
        _contentType = contentType;
    }

    public static SigV4RequestTemplate CreateHeaderSigned(
        HttpMethod method,
        Uri baseAddress,
        string pathAndQuery,
        byte[]? contentBytes = null,
        string? contentType = null)
    {
        ArgumentNullException.ThrowIfNull(method);
        ArgumentNullException.ThrowIfNull(baseAddress);
        ArgumentException.ThrowIfNullOrWhiteSpace(pathAndQuery);

        var requestUri = new Uri(baseAddress, pathAndQuery);
        var hostHeader = requestUri.IsDefaultPort
            ? requestUri.Host
            : requestUri.Authority;
        var timestampUtc = DateTimeOffset.UtcNow;
        var timestampHeader = timestampUtc.ToString("yyyyMMdd'T'HHmmss'Z'");
        var payloadHash = contentBytes is null
            ? EmptyPayloadHash
            : S3SigV4Signer.ComputeSha256Hex(contentBytes);

        var headers = new List<KeyValuePair<string, string?>>
        {
            new("host", hostHeader),
            new("x-amz-date", timestampHeader)
        };

        var signedHeaders = new List<string> { "host", "x-amz-date" };
        if (contentBytes is not null) {
            headers.Add(new KeyValuePair<string, string?>("x-amz-content-sha256", payloadHash));
            signedHeaders.Insert(1, "x-amz-content-sha256");
        }

        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = BenchmarkDefaults.AccessKeyId,
            DateStamp = timestampUtc.ToString("yyyyMMdd"),
            Region = BenchmarkDefaults.Region,
            Service = BenchmarkDefaults.Service,
            Terminator = "aws4_request"
        };

        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            method.Method,
            requestUri.AbsolutePath,
            ParseQueryParameters(requestUri),
            headers,
            signedHeaders,
            payloadHash);
        var stringToSign = S3SigV4Signer.BuildStringToSign("AWS4-HMAC-SHA256", timestampUtc, credentialScope, canonicalRequest.CanonicalRequestHashHex);
        var signature = S3SigV4Signer.ComputeSignature(BenchmarkDefaults.SecretAccessKey, credentialScope, stringToSign);
        var authorizationHeader = $"AWS4-HMAC-SHA256 Credential={BenchmarkDefaults.AccessKeyId}/{credentialScope.Scope}, SignedHeaders={string.Join(';', signedHeaders)}, Signature={signature}";

        return new SigV4RequestTemplate(
            method,
            pathAndQuery,
            hostHeader,
            authorizationHeader,
            timestampHeader,
            contentBytes is null ? null : payloadHash,
            contentBytes,
            contentType);
    }

    public HttpRequestMessage CreateRequestMessage()
    {
        var request = new HttpRequestMessage(_method, _pathAndQuery);
        request.Headers.Host = _hostHeader;
        request.Headers.TryAddWithoutValidation("Authorization", _authorizationHeader);
        request.Headers.TryAddWithoutValidation("x-amz-date", _timestampHeader);

        if (_contentBytes is not null) {
            request.Headers.TryAddWithoutValidation("x-amz-content-sha256", _payloadHashHeader);
            request.Content = new ByteArrayContent(_contentBytes);
            if (!string.IsNullOrWhiteSpace(_contentType)) {
                request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(_contentType);
            }
        }

        return request;
    }

    private static IReadOnlyList<KeyValuePair<string, string?>> ParseQueryParameters(Uri requestUri)
    {
        var parsed = QueryHelpers.ParseQuery(requestUri.Query);
        var values = new List<KeyValuePair<string, string?>>();
        foreach (var (key, queryValues) in parsed) {
            if (queryValues.Count == 0) {
                values.Add(new KeyValuePair<string, string?>(key, string.Empty));
                continue;
            }

            foreach (var value in queryValues) {
                values.Add(new KeyValuePair<string, string?>(key, value));
            }
        }

        return values;
    }
}

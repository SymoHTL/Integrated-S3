using System.Globalization;

namespace IntegratedS3.Protocol;

/// <summary>
/// Generates SigV4 presigned URLs by computing a canonical request and HMAC-SHA256 signature
/// that is appended as the <c>X-Amz-Signature</c> query parameter.
/// </summary>
public static class S3SigV4Presigner
{
    private const string Algorithm = "AWS4-HMAC-SHA256";
    private const string Terminator = "aws4_request";
    private const string SignatureQueryKey = "X-Amz-Signature";

    /// <summary>
    /// Creates a SigV4 presigned request by computing the canonical request, string-to-sign,
    /// and HMAC-SHA256 signature from the supplied parameters.
    /// </summary>
    /// <param name="parameters">The presign parameters including credentials, region, expiry, and request details.</param>
    /// <returns>A <see cref="S3SigV4PresignedRequestData"/> containing the signature and signed query parameters.</returns>
    public static S3SigV4PresignedRequestData Presign(S3SigV4PresignParameters parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.HttpMethod);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.Path);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.AccessKeyId);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.SecretAccessKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.Region);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.Service);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.PayloadHash);

        if (parameters.ExpiresInSeconds <= 0) {
            throw new ArgumentOutOfRangeException(nameof(parameters.ExpiresInSeconds), parameters.ExpiresInSeconds, "The presign expiry must be a positive number of seconds.");
        }

        var signedHeaders = parameters.SignedHeaders
            .Select(static header => header.ToLowerInvariant())
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static header => header, StringComparer.Ordinal)
            .ToArray();

        if (signedHeaders.Length == 0) {
            throw new ArgumentException("At least one signed header is required for a presigned request.", nameof(parameters));
        }

        var signedAtUtc = parameters.SignedAtUtc.ToUniversalTime();
        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = parameters.AccessKeyId,
            DateStamp = signedAtUtc.ToString("yyyyMMdd", CultureInfo.InvariantCulture),
            Region = parameters.Region,
            Service = parameters.Service,
            Terminator = Terminator
        };

        var queryParameters = new List<KeyValuePair<string, string?>>(parameters.QueryParameters.Count + 6)
        {
            new("X-Amz-Algorithm", Algorithm),
            new("X-Amz-Credential", $"{parameters.AccessKeyId}/{credentialScope.Scope}"),
            new("X-Amz-Date", signedAtUtc.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture)),
            new("X-Amz-Expires", parameters.ExpiresInSeconds.ToString(CultureInfo.InvariantCulture)),
            new("X-Amz-SignedHeaders", string.Join(';', signedHeaders))
        };

        if (!string.IsNullOrWhiteSpace(parameters.SecurityToken)) {
            queryParameters.Add(new KeyValuePair<string, string?>("X-Amz-Security-Token", parameters.SecurityToken));
        }

        queryParameters.AddRange(parameters.QueryParameters);

        var canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            parameters.HttpMethod,
            parameters.Path,
            queryParameters,
            parameters.Headers,
            signedHeaders,
            parameters.PayloadHash,
            unsignedQueryKey: SignatureQueryKey);

        var stringToSign = S3SigV4Signer.BuildStringToSign(
            Algorithm,
            signedAtUtc,
            credentialScope,
            canonicalRequest.CanonicalRequestHashHex);

        var signature = S3SigV4Signer.ComputeSignature(parameters.SecretAccessKey, credentialScope, stringToSign);
        queryParameters.Add(new KeyValuePair<string, string?>(SignatureQueryKey, signature));

        return new S3SigV4PresignedRequestData
        {
            CredentialScope = credentialScope,
            SignedAtUtc = signedAtUtc,
            ExpiresAtUtc = signedAtUtc.AddSeconds(parameters.ExpiresInSeconds),
            Signature = signature,
            CanonicalRequest = canonicalRequest,
            QueryParameters = queryParameters.ToArray()
        };
    }
}

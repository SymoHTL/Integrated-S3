using System.Globalization;

namespace IntegratedS3.Protocol;

/// <summary>
/// Generates SigV4a presigned URLs by computing a canonical request and ECDSA P-256 signature
/// that is appended as the <c>X-Amz-Signature</c> query parameter, supporting multi-region signing.
/// </summary>
public static class S3SigV4aPresigner
{
    private const string Terminator = "aws4_request";
    private const string SignatureQueryKey = "X-Amz-Signature";
    private const string RegionSetQueryKey = "X-Amz-Region-Set";

    /// <summary>
    /// Creates a SigV4a presigned request by computing the canonical request, string-to-sign,
    /// and ECDSA P-256 signature from the supplied parameters.
    /// </summary>
    /// <param name="parameters">The presign parameters including credentials, region set, expiry, and request details.</param>
    /// <returns>A <see cref="S3SigV4PresignedRequestData"/> containing the signature and signed query parameters.</returns>
    public static S3SigV4PresignedRequestData Presign(S3SigV4aPresignParameters parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.HttpMethod);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.Path);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.AccessKeyId);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.SecretAccessKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.Service);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameters.PayloadHash);

        if (parameters.RegionSet.Count == 0) {
            throw new ArgumentException("At least one region must be specified in RegionSet.", nameof(parameters));
        }

        if (parameters.ExpiresInSeconds <= 0) {
            throw new ArgumentOutOfRangeException(nameof(parameters), parameters.ExpiresInSeconds, "The presign expiry must be a positive number of seconds.");
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
        var dateStamp = signedAtUtc.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

        // SigV4a credential scope omits region: dateStamp/service/aws4_request
        var credentialScopeString = S3SigV4aSigner.BuildCredentialScopeString(dateStamp, parameters.Service);

        // Reuse S3SigV4CredentialScope for the result model; use "*" as sentinel region
        var credentialScope = new S3SigV4CredentialScope
        {
            AccessKeyId = parameters.AccessKeyId,
            DateStamp = dateStamp,
            Region = "*",
            Service = parameters.Service,
            Terminator = Terminator
        };

        var regionSetValue = string.Join(',', parameters.RegionSet);

        var queryParameters = new List<KeyValuePair<string, string?>>(parameters.QueryParameters.Count + 7)
        {
            new("X-Amz-Algorithm", S3SigV4aSigner.Algorithm),
            new("X-Amz-Credential", $"{parameters.AccessKeyId}/{credentialScopeString}"),
            new("X-Amz-Date", signedAtUtc.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture)),
            new("X-Amz-Expires", parameters.ExpiresInSeconds.ToString(CultureInfo.InvariantCulture)),
            new("X-Amz-SignedHeaders", string.Join(';', signedHeaders)),
            new(RegionSetQueryKey, regionSetValue)
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

        var stringToSign = S3SigV4aSigner.BuildStringToSign(
            signedAtUtc,
            credentialScopeString,
            canonicalRequest.CanonicalRequestHashHex);

        using var ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(parameters.SecretAccessKey, parameters.AccessKeyId);
        var signature = S3SigV4aSigner.ComputeSignature(ecdsaKey, stringToSign);
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

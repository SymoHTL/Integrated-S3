using System.Diagnostics;
using System.Globalization;
using System.Security.Claims;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Protocol;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IntegratedS3.AspNetCore.Services;

internal sealed class AwsSignatureV4RequestAuthenticator(
    IOptions<IntegratedS3Options> options,
    ILogger<AwsSignatureV4RequestAuthenticator> logger) : IIntegratedS3RequestAuthenticator
{
    private const string Algorithm = "AWS4-HMAC-SHA256";
    private const string AwsContentSha256HeaderName = "x-amz-content-sha256";
    private const string AwsDateHeaderName = "x-amz-date";
    private const string PresignedSignatureQueryKey = "X-Amz-Signature";
    private const string UnsignedPayload = "UNSIGNED-PAYLOAD";
    private const string EmptyPayloadSha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    public ValueTask<IntegratedS3RequestAuthenticationResult> AuthenticateAsync(HttpContext httpContext, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        cancellationToken.ThrowIfCancellationRequested();

        var settings = options.Value;
        if (!settings.EnableAwsSignatureV4Authentication) {
            return ValueTask.FromResult(IntegratedS3RequestAuthenticationResult.NoResult());
        }

        var correlationId = IntegratedS3AspNetCoreTelemetry.GetOrCreateCorrelationId(httpContext);
        var authorizationHeader = httpContext.Request.Headers.Authorization.ToString();
        if (S3SigV4RequestParser.TryParseAuthorizationHeader(authorizationHeader, out var headerAuthorization, out var headerError)) {
            using var activity = StartAuthenticationActivity(httpContext, correlationId, "sigv4-header", headerAuthorization?.CredentialScope.AccessKeyId);
            var result = ValidateHeaderAuthorization(httpContext, settings, headerAuthorization, headerError);
            ObserveAuthenticationResult(httpContext, activity, "sigv4-header", headerAuthorization?.CredentialScope.AccessKeyId, result);
            return ValueTask.FromResult(result);
        }

        if (S3SigV4RequestParser.TryParsePresignedRequest(EnumerateQueryParameters(httpContext.Request), out var presignedRequest, out var queryError)) {
            using var activity = StartAuthenticationActivity(httpContext, correlationId, "sigv4-presigned", presignedRequest?.CredentialScope.AccessKeyId);
            var result = ValidatePresignedRequest(httpContext, settings, presignedRequest, queryError);
            ObserveAuthenticationResult(httpContext, activity, "sigv4-presigned", presignedRequest?.CredentialScope.AccessKeyId, result);
            return ValueTask.FromResult(result);
        }

        return ValueTask.FromResult(IntegratedS3RequestAuthenticationResult.NoResult());
    }

    private Activity? StartAuthenticationActivity(HttpContext httpContext, string correlationId, string authType, string? accessKeyId)
    {
        var activity = IntegratedS3Observability.ActivitySource.StartActivity("IntegratedS3.Authenticate", ActivityKind.Internal);
        if (activity is null) {
            return null;
        }

        activity.SetTag(IntegratedS3Observability.Tags.AuthType, authType);
        activity.SetTag(IntegratedS3Observability.Tags.CorrelationId, correlationId);
        activity.SetTag(IntegratedS3Observability.Tags.RequestId, httpContext.TraceIdentifier);
        activity.SetTag("integrateds3.access_key_id", accessKeyId);
        activity.SetTag("http.request.method", httpContext.Request.Method);
        activity.SetTag("url.path", httpContext.Request.Path.ToString());
        return activity;
    }

    private void ObserveAuthenticationResult(
        HttpContext httpContext,
        Activity? activity,
        string authType,
        string? accessKeyId,
        IntegratedS3RequestAuthenticationResult result)
    {
        if (!result.HasAttemptedAuthentication) {
            return;
        }

        if (result.Succeeded) {
            IntegratedS3AspNetCoreTelemetry.MarkSuccess(activity, authType);
            logger.LogDebug(
                "IntegratedS3 authentication succeeded for {AuthType} access key {AccessKeyId}.",
                authType,
                accessKeyId);
            return;
        }

        IntegratedS3AspNetCoreTelemetry.RecordAuthenticationFailure("authentication", authType, result.ErrorCode ?? "AccessDenied");
        IntegratedS3AspNetCoreTelemetry.MarkFailure(activity, authType, result.ErrorCode ?? "AccessDenied", result.ErrorMessage);
        logger.LogWarning(
            "IntegratedS3 authentication failed for {AuthType}. AccessKeyId {AccessKeyId}. ErrorCode {ErrorCode}. RequestPath {RequestPath}.",
            authType,
            accessKeyId,
            result.ErrorCode,
            httpContext.Request.Path);
    }

    private static IntegratedS3RequestAuthenticationResult ValidateHeaderAuthorization(
        HttpContext httpContext,
        IntegratedS3Options settings,
        S3SigV4AuthorizationHeader? authorization,
        string? parseError)
    {
        if (authorization is null) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationHeaderMalformed", parseError ?? "The Authorization header is malformed.", statusCode: 400);
        }

        if (!string.Equals(authorization.Algorithm, Algorithm, StringComparison.Ordinal)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidRequest", "Only AWS4-HMAC-SHA256 authorization is supported.", statusCode: 400);
        }

        if (!TryValidateCredentialScope(authorization.CredentialScope, settings, out var scopeError, out var statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationHeaderMalformed", scopeError!, statusCode);
        }

        if (!TryResolveCredential(settings, authorization.CredentialScope.AccessKeyId, out var credential)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidAccessKeyId", $"The AWS access key id '{authorization.CredentialScope.AccessKeyId}' does not exist in this service.");
        }

        if (!authorization.SignedHeaders.Contains("host", StringComparer.Ordinal)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationHeaderMalformed", "The authorization header must sign the 'host' header.", statusCode: 400);
        }

        if (!TryParseHeaderTimestamp(httpContext.Request.Headers[AwsDateHeaderName].ToString(), out var requestTimestampUtc)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AccessDenied", "The request must include a valid x-amz-date header.");
        }

        if (IsOutsideAllowedClockSkew(requestTimestampUtc, settings)) {
            return IntegratedS3RequestAuthenticationResult.Failure("RequestTimeTooSkewed", "The difference between the request time and the server time is too large.");
        }

        if (!TryResolvePayloadHash(httpContext.Request, isPresigned: false, out var payloadHash, out var payloadHashError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidRequest", payloadHashError!, statusCode);
        }

        if (!TryBuildCanonicalRequest(httpContext.Request, authorization.SignedHeaders, payloadHash!, PresignedSignatureQueryKey, out var canonicalRequest, out var canonicalError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", canonicalError!, statusCode);
        }

        var stringToSign = S3SigV4Signer.BuildStringToSign(authorization.Algorithm, requestTimestampUtc, authorization.CredentialScope, canonicalRequest!.CanonicalRequestHashHex);
        var expectedSignature = S3SigV4Signer.ComputeSignature(credential!.SecretAccessKey, authorization.CredentialScope, stringToSign);
        if (!string.Equals(expectedSignature, authorization.Signature, StringComparison.OrdinalIgnoreCase)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.");
        }

        return IntegratedS3RequestAuthenticationResult.Success(CreatePrincipal(credential));
    }

    private static IntegratedS3RequestAuthenticationResult ValidatePresignedRequest(
        HttpContext httpContext,
        IntegratedS3Options settings,
        S3SigV4PresignedRequest? presignedRequest,
        string? parseError)
    {
        if (presignedRequest is null) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationQueryParametersError", parseError ?? "The query-string authorization parameters are malformed.", statusCode: 400);
        }

        if (!TryValidateCredentialScope(presignedRequest.CredentialScope, settings, out var scopeError, out var statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationQueryParametersError", scopeError!, statusCode);
        }

        if (presignedRequest.ExpiresSeconds > settings.MaximumPresignedUrlExpirySeconds) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationQueryParametersError", $"Presigned URL expiry exceeds the configured maximum of {settings.MaximumPresignedUrlExpirySeconds} seconds.", statusCode: 400);
        }

        var nowUtc = DateTimeOffset.UtcNow;
        if (presignedRequest.SignedAtUtc - nowUtc > TimeSpan.FromMinutes(settings.AllowedSignatureClockSkewMinutes)) {
            return IntegratedS3RequestAuthenticationResult.Failure("RequestTimeTooSkewed", "The presigned request time is too far in the future.");
        }

        if (nowUtc - presignedRequest.SignedAtUtc > TimeSpan.FromSeconds(presignedRequest.ExpiresSeconds) + TimeSpan.FromMinutes(settings.AllowedSignatureClockSkewMinutes)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AccessDenied", "The presigned request has expired.");
        }

        if (!TryResolveCredential(settings, presignedRequest.CredentialScope.AccessKeyId, out var credential)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidAccessKeyId", $"The AWS access key id '{presignedRequest.CredentialScope.AccessKeyId}' does not exist in this service.");
        }

        if (!TryResolvePayloadHash(httpContext.Request, isPresigned: true, out var payloadHash, out var payloadHashError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidRequest", payloadHashError!, statusCode);
        }

        if (!TryBuildCanonicalRequest(httpContext.Request, presignedRequest.SignedHeaders, payloadHash!, PresignedSignatureQueryKey, out var canonicalRequest, out var canonicalError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", canonicalError!, statusCode);
        }

        var stringToSign = S3SigV4Signer.BuildStringToSign(presignedRequest.Algorithm, presignedRequest.SignedAtUtc, presignedRequest.CredentialScope, canonicalRequest!.CanonicalRequestHashHex);
        var expectedSignature = S3SigV4Signer.ComputeSignature(credential!.SecretAccessKey, presignedRequest.CredentialScope, stringToSign);
        if (!string.Equals(expectedSignature, presignedRequest.Signature, StringComparison.OrdinalIgnoreCase)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", "The presigned request signature does not match the expected signature.");
        }

        return IntegratedS3RequestAuthenticationResult.Success(CreatePrincipal(credential));
    }

    private static ClaimsPrincipal CreatePrincipal(IntegratedS3AccessKeyCredential credential)
    {
        var claims = new List<Claim>
        {
            new(ClaimTypes.NameIdentifier, credential.AccessKeyId),
            new(ClaimTypes.Name, string.IsNullOrWhiteSpace(credential.DisplayName) ? credential.AccessKeyId : credential.DisplayName),
            new("integrateds3:access-key-id", credential.AccessKeyId),
            new("integrateds3:auth-type", "sigv4")
        };

        claims.AddRange(credential.Scopes.Where(static scope => !string.IsNullOrWhiteSpace(scope)).Select(static scope => new Claim("scope", scope)));
        return new ClaimsPrincipal(new ClaimsIdentity(claims, authenticationType: "IntegratedS3SigV4"));
    }

    private static bool TryResolveCredential(IntegratedS3Options settings, string accessKeyId, out IntegratedS3AccessKeyCredential? credential)
    {
        credential = settings.AccessKeyCredentials.FirstOrDefault(candidate => string.Equals(candidate.AccessKeyId, accessKeyId, StringComparison.Ordinal));
        return credential is not null;
    }

    private static bool TryValidateCredentialScope(S3SigV4CredentialScope credentialScope, IntegratedS3Options settings, out string? error, out int statusCode)
    {
        statusCode = 400;
        if (!string.Equals(credentialScope.Terminator, "aws4_request", StringComparison.Ordinal)) {
            error = "The credential scope must end with 'aws4_request'.";
            return false;
        }

        if (!string.Equals(credentialScope.Region, settings.SignatureAuthenticationRegion, StringComparison.Ordinal)) {
            error = $"Credential scope region '{credentialScope.Region}' does not match configured region '{settings.SignatureAuthenticationRegion}'.";
            return false;
        }

        if (!string.Equals(credentialScope.Service, settings.SignatureAuthenticationService, StringComparison.Ordinal)) {
            error = $"Credential scope service '{credentialScope.Service}' does not match configured service '{settings.SignatureAuthenticationService}'.";
            return false;
        }

        error = null;
        statusCode = 200;
        return true;
    }

    private static bool TryParseHeaderTimestamp(string? rawValue, out DateTimeOffset requestTimestampUtc)
    {
        return DateTimeOffset.TryParseExact(rawValue, "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out requestTimestampUtc);
    }

    private static bool IsOutsideAllowedClockSkew(DateTimeOffset requestTimestampUtc, IntegratedS3Options settings)
    {
        var clockSkew = TimeSpan.FromMinutes(settings.AllowedSignatureClockSkewMinutes);
        return (DateTimeOffset.UtcNow - requestTimestampUtc).Duration() > clockSkew;
    }

    private static bool TryResolvePayloadHash(HttpRequest request, bool isPresigned, out string? payloadHash, out string? error, out int statusCode)
    {
        var headerValue = request.Headers[AwsContentSha256HeaderName].ToString();
        if (!string.IsNullOrWhiteSpace(headerValue)) {
            payloadHash = headerValue.Trim();
            error = null;
            statusCode = 200;
            return true;
        }

        if (isPresigned) {
            payloadHash = UnsignedPayload;
            error = null;
            statusCode = 200;
            return true;
        }

        if (HttpMethods.IsGet(request.Method) || HttpMethods.IsHead(request.Method) || HttpMethods.IsDelete(request.Method)) {
            payloadHash = EmptyPayloadSha256;
            error = null;
            statusCode = 200;
            return true;
        }

        payloadHash = null;
        error = "The request must include the x-amz-content-sha256 header for signed payloads.";
        statusCode = 400;
        return false;
    }

    private static bool TryBuildCanonicalRequest(
        HttpRequest request,
        IReadOnlyList<string> signedHeaders,
        string payloadHash,
        string unsignedQueryKey,
        out S3SigV4CanonicalRequest? canonicalRequest,
        out string? error,
        out int statusCode)
    {
        if (!TryEnumerateSignedHeaders(request, signedHeaders, out var headers, out error)) {
            canonicalRequest = null;
            statusCode = 400;
            return false;
        }

        canonicalRequest = S3SigV4Signer.BuildCanonicalRequest(
            request.Method,
            request.PathBase.Add(request.Path).ToUriComponent(),
            EnumerateQueryParameters(request),
            headers!,
            signedHeaders,
            payloadHash,
            unsignedQueryKey);
        error = null;
        statusCode = 200;
        return true;
    }

    private static bool TryEnumerateSignedHeaders(HttpRequest request, IReadOnlyList<string> signedHeaders, out IReadOnlyList<KeyValuePair<string, string?>>? headers, out string? error)
    {
        var result = new List<KeyValuePair<string, string?>>(signedHeaders.Count);
        foreach (var signedHeader in signedHeaders) {
            if (string.Equals(signedHeader, "host", StringComparison.Ordinal)) {
                if (!request.Host.HasValue) {
                    headers = null;
                    error = "The request must include a Host header.";
                    return false;
                }

                result.Add(new KeyValuePair<string, string?>("host", request.Host.Value));
                continue;
            }

            if (!request.Headers.TryGetValue(signedHeader, out var value)) {
                headers = null;
                error = $"The signed header '{signedHeader}' is missing from the request.";
                return false;
            }

            result.Add(new KeyValuePair<string, string?>(signedHeader, value.ToString()));
        }

        headers = result;
        error = null;
        return true;
    }

    private static IEnumerable<KeyValuePair<string, string?>> EnumerateQueryParameters(HttpRequest request)
    {
        foreach (var pair in request.Query) {
            if (pair.Value.Count == 0) {
                yield return new KeyValuePair<string, string?>(pair.Key, string.Empty);
                continue;
            }

            foreach (var value in pair.Value) {
                yield return new KeyValuePair<string, string?>(pair.Key, value);
            }
        }
    }
}

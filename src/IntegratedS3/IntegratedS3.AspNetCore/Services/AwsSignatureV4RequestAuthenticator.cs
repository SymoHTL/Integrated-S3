using System.Diagnostics;
using System.Globalization;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
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
    private const string SigV4aAlgorithm = "AWS4-ECDSA-P256-SHA256";
    private const string AwsContentSha256HeaderName = "x-amz-content-sha256";
    private const string AwsDateHeaderName = "x-amz-date";
    private const string AwsSecurityTokenHeaderName = "x-amz-security-token";
    private const string AwsSecurityTokenQueryKey = "X-Amz-Security-Token";
    private const string AwsTrailerHeaderName = "x-amz-trailer";
    private const string PresignedSignatureQueryKey = "X-Amz-Signature";
    private const string UnsignedPayload = "UNSIGNED-PAYLOAD";
    private const string EmptyPayloadSha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private const string StreamingAws4HmacSha256PayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
    private const string StreamingSigV4aPayloadTrailer = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER";
    private const string StreamingUnsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

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
        if (S3SigV4RequestParser.TryParseAuthorizationHeader(authorizationHeader, EnumerateHeaders(httpContext.Request), out var headerAuthorization, out var headerError)) {
            var headerAuthType = string.Equals(headerAuthorization?.Algorithm, SigV4aAlgorithm, StringComparison.Ordinal) ? "sigv4a-header" : "sigv4-header";
            using var activity = StartAuthenticationActivity(httpContext, correlationId, headerAuthType, headerAuthorization?.CredentialScope.AccessKeyId);
            var result = ValidateHeaderAuthorization(httpContext, settings, headerAuthorization, headerError);
            ObserveAuthenticationResult(httpContext, activity, headerAuthType, headerAuthorization?.CredentialScope.AccessKeyId, result);
            return ValueTask.FromResult(result);
        }

        if (S3SigV4RequestParser.TryParsePresignedRequest(EnumerateQueryParameters(httpContext.Request), out var presignedRequest, out var queryError)) {
            var presignedAuthType = string.Equals(presignedRequest?.Algorithm, SigV4aAlgorithm, StringComparison.Ordinal) ? "sigv4a-presigned" : "sigv4-presigned";
            using var activity = StartAuthenticationActivity(httpContext, correlationId, presignedAuthType, presignedRequest?.CredentialScope.AccessKeyId);
            var result = ValidatePresignedRequest(httpContext, settings, presignedRequest, queryError);
            ObserveAuthenticationResult(httpContext, activity, presignedAuthType, presignedRequest?.CredentialScope.AccessKeyId, result);
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

        if (string.Equals(authorization.Algorithm, SigV4aAlgorithm, StringComparison.Ordinal)) {
            return ValidateSigV4aHeaderAuthorization(httpContext, settings, authorization);
        }

        if (!string.Equals(authorization.Algorithm, Algorithm, StringComparison.Ordinal)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidRequest", "Only AWS4-HMAC-SHA256 and AWS4-ECDSA-P256-SHA256 authorization is supported.", statusCode: 400);
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

        if (!TryValidateTrailerBackedStreamingHeaders(httpContext.Request, isPresigned: false, authorization.SignedHeaders, out var trailerHeaderErrorCode, out var trailerHeaderError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure(trailerHeaderErrorCode!, trailerHeaderError!, statusCode);
        }

        if (!TryValidateHeaderSecurityToken(authorization, credential!, out var securityTokenErrorCode, out var securityTokenError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure(securityTokenErrorCode!, securityTokenError!, statusCode);
        }

        if (!TryParseHeaderTimestamp(httpContext.Request.Headers[AwsDateHeaderName].ToString(), out var requestTimestampUtc)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AccessDenied", "The request must include a valid x-amz-date header.");
        }

        if (IsOutsideAllowedClockSkew(requestTimestampUtc, settings)) {
            return IntegratedS3RequestAuthenticationResult.Failure("RequestTimeTooSkewed", "The difference between the request time and the server time is too large.");
        }

        if (!TryResolvePayloadHash(httpContext.Request, isPresigned: false, signedHeaders: authorization.SignedHeaders, out var payloadHash, out var payloadHashError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidRequest", payloadHashError!, statusCode);
        }

        if (!TryBuildCanonicalRequest(httpContext.Request, authorization.SignedHeaders, payloadHash!, PresignedSignatureQueryKey, out var canonicalRequest, out var canonicalError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", canonicalError!, statusCode);
        }

        var stringToSign = S3SigV4Signer.BuildStringToSign(authorization.Algorithm, requestTimestampUtc, authorization.CredentialScope, canonicalRequest!.CanonicalRequestHashHex);
        var expectedSignature = S3SigV4Signer.ComputeSignature(credential!.SecretAccessKey, authorization.CredentialScope, stringToSign);
        if (!FixedTimeEqualsOrdinalIgnoreCase(expectedSignature, authorization.Signature)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.");
        }

        StoreAwsChunkedTrailerSigningContext(httpContext, payloadHash!, credential, authorization.CredentialScope, requestTimestampUtc);
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

        if (string.Equals(presignedRequest.Algorithm, SigV4aAlgorithm, StringComparison.Ordinal)) {
            return ValidateSigV4aPresignedRequest(httpContext, settings, presignedRequest);
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

        if (!presignedRequest.SignedHeaders.Contains("host", StringComparer.Ordinal)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationQueryParametersError", "The presigned request must sign the 'host' header.", statusCode: 400);
        }

        if (!TryValidateTrailerBackedStreamingHeaders(httpContext.Request, isPresigned: true, presignedRequest.SignedHeaders, out var trailerHeaderErrorCode, out var trailerHeaderError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure(trailerHeaderErrorCode!, trailerHeaderError!, statusCode);
        }

        if (!TryValidatePresignedSecurityToken(presignedRequest, credential!, out var securityTokenErrorCode, out var securityTokenError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure(securityTokenErrorCode!, securityTokenError!, statusCode);
        }

        if (!TryResolvePayloadHash(httpContext.Request, isPresigned: true, signedHeaders: presignedRequest.SignedHeaders, out var payloadHash, out var payloadHashError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidRequest", payloadHashError!, statusCode);
        }

        if (!TryBuildCanonicalRequest(httpContext.Request, presignedRequest.SignedHeaders, payloadHash!, PresignedSignatureQueryKey, out var canonicalRequest, out var canonicalError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", canonicalError!, statusCode);
        }

        var stringToSign = S3SigV4Signer.BuildStringToSign(presignedRequest.Algorithm, presignedRequest.SignedAtUtc, presignedRequest.CredentialScope, canonicalRequest!.CanonicalRequestHashHex);
        var expectedSignature = S3SigV4Signer.ComputeSignature(credential!.SecretAccessKey, presignedRequest.CredentialScope, stringToSign);
        if (!FixedTimeEqualsOrdinalIgnoreCase(expectedSignature, presignedRequest.Signature)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", "The presigned request signature does not match the expected signature.");
        }

        StoreAwsChunkedTrailerSigningContext(httpContext, payloadHash!, credential, presignedRequest.CredentialScope, presignedRequest.SignedAtUtc);
        return IntegratedS3RequestAuthenticationResult.Success(CreatePrincipal(credential));
    }

    private static bool TryValidateHeaderSecurityToken(
        S3SigV4AuthorizationHeader authorization,
        IntegratedS3AccessKeyCredential credential,
        out string? errorCode,
        out string? error,
        out int statusCode)
    {
        if (string.IsNullOrWhiteSpace(credential.SessionToken)) {
            errorCode = null;
            error = null;
            statusCode = 200;
            return true;
        }

        if (!authorization.SignedHeaders.Contains(AwsSecurityTokenHeaderName, StringComparer.Ordinal)) {
            errorCode = "AuthorizationHeaderMalformed";
            error = $"The authorization header must sign the '{AwsSecurityTokenHeaderName}' header when temporary credentials are used.";
            statusCode = 400;
            return false;
        }

        if (string.IsNullOrWhiteSpace(authorization.SecurityToken)) {
            errorCode = "AuthorizationHeaderMalformed";
            error = $"The request must include the {AwsSecurityTokenHeaderName} header when temporary credentials are used.";
            statusCode = 400;
            return false;
        }

        if (!FixedTimeEqualsOrdinal(credential.SessionToken, authorization.SecurityToken)) {
            errorCode = "AccessDenied";
            error = $"The provided {AwsSecurityTokenHeaderName} is invalid.";
            statusCode = 403;
            return false;
        }

        errorCode = null;
        error = null;
        statusCode = 200;
        return true;
    }

    private static bool TryValidatePresignedSecurityToken(
        S3SigV4PresignedRequest presignedRequest,
        IntegratedS3AccessKeyCredential credential,
        out string? errorCode,
        out string? error,
        out int statusCode)
    {
        if (string.IsNullOrWhiteSpace(credential.SessionToken)) {
            errorCode = null;
            error = null;
            statusCode = 200;
            return true;
        }

        if (string.IsNullOrWhiteSpace(presignedRequest.SecurityToken)) {
            errorCode = "AuthorizationQueryParametersError";
            error = $"The presigned request must include {AwsSecurityTokenQueryKey} when temporary credentials are used.";
            statusCode = 400;
            return false;
        }

        if (!FixedTimeEqualsOrdinal(credential.SessionToken, presignedRequest.SecurityToken)) {
            errorCode = "AccessDenied";
            error = $"The provided {AwsSecurityTokenQueryKey} is invalid.";
            statusCode = 403;
            return false;
        }

        errorCode = null;
        error = null;
        statusCode = 200;
        return true;
    }

    private static bool TryValidateTrailerBackedStreamingHeaders(
        HttpRequest request,
        bool isPresigned,
        IReadOnlyList<string> signedHeaders,
        out string? errorCode,
        out string? error,
        out int statusCode)
    {
        var payloadHashHeaderValue = request.Headers[AwsContentSha256HeaderName].ToString().Trim();
        if (!IsTrailerBackedStreamingPayloadHash(payloadHashHeaderValue)) {
            errorCode = null;
            error = null;
            statusCode = 200;
            return true;
        }

        if (signedHeaders.Contains(AwsTrailerHeaderName, StringComparer.Ordinal)) {
            errorCode = null;
            error = null;
            statusCode = 200;
            return true;
        }

        errorCode = isPresigned ? "AuthorizationQueryParametersError" : "AuthorizationHeaderMalformed";
        error = isPresigned
            ? $"The presigned request must sign the '{AwsTrailerHeaderName}' header when '{payloadHashHeaderValue}' is used."
            : $"The authorization header must sign the '{AwsTrailerHeaderName}' header when '{payloadHashHeaderValue}' is used.";
        statusCode = 400;
        return false;
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

    // ── SigV4a (ECDSA P-256) ──────────────────────────────────────────────

    private static IntegratedS3RequestAuthenticationResult ValidateSigV4aHeaderAuthorization(
        HttpContext httpContext,
        IntegratedS3Options settings,
        S3SigV4AuthorizationHeader authorization)
    {
        if (!TryValidateSigV4aCredentialScope(authorization.CredentialScope, settings, out var scopeError, out var statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationHeaderMalformed", scopeError!, statusCode);
        }

        if (!TryValidateSigV4aRegionSet(authorization.RegionSet, settings, out var regionError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationHeaderMalformed", regionError!, statusCode);
        }

        if (!TryResolveCredential(settings, authorization.CredentialScope.AccessKeyId, out var credential)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidAccessKeyId", $"The AWS access key id '{authorization.CredentialScope.AccessKeyId}' does not exist in this service.");
        }

        if (!authorization.SignedHeaders.Contains("host", StringComparer.Ordinal)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationHeaderMalformed", "The authorization header must sign the 'host' header.", statusCode: 400);
        }

        if (!TryValidateTrailerBackedStreamingHeaders(httpContext.Request, isPresigned: false, authorization.SignedHeaders, out var trailerErrorCode, out var trailerError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure(trailerErrorCode!, trailerError!, statusCode);
        }

        if (!TryValidateHeaderSecurityToken(authorization, credential!, out var securityTokenErrorCode, out var securityTokenError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure(securityTokenErrorCode!, securityTokenError!, statusCode);
        }

        if (!TryParseHeaderTimestamp(httpContext.Request.Headers[AwsDateHeaderName].ToString(), out var requestTimestampUtc)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AccessDenied", "The request must include a valid x-amz-date header.");
        }

        if (IsOutsideAllowedClockSkew(requestTimestampUtc, settings)) {
            return IntegratedS3RequestAuthenticationResult.Failure("RequestTimeTooSkewed", "The difference between the request time and the server time is too large.");
        }

        if (!TryResolvePayloadHash(httpContext.Request, isPresigned: false, signedHeaders: authorization.SignedHeaders, out var payloadHash, out var payloadHashError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidRequest", payloadHashError!, statusCode);
        }

        if (!TryBuildCanonicalRequest(httpContext.Request, authorization.SignedHeaders, payloadHash!, PresignedSignatureQueryKey, out var canonicalRequest, out var canonicalError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", canonicalError!, statusCode);
        }

        var credentialScopeString = S3SigV4aSigner.BuildCredentialScopeString(
            authorization.CredentialScope.DateStamp,
            authorization.CredentialScope.Service);
        var stringToSign = S3SigV4aSigner.BuildStringToSign(requestTimestampUtc, credentialScopeString, canonicalRequest!.CanonicalRequestHashHex);

        using var ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(credential!.SecretAccessKey, credential.AccessKeyId);
        if (!S3SigV4aSigner.VerifySignature(ecdsaKey, stringToSign, authorization.Signature)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.");
        }

        StoreSigV4aChunkedTrailerSigningContext(httpContext, payloadHash!, credential, authorization.CredentialScope, requestTimestampUtc);
        return IntegratedS3RequestAuthenticationResult.Success(CreateSigV4aPrincipal(credential));
    }

    private static IntegratedS3RequestAuthenticationResult ValidateSigV4aPresignedRequest(
        HttpContext httpContext,
        IntegratedS3Options settings,
        S3SigV4PresignedRequest presignedRequest)
    {
        if (!TryValidateSigV4aCredentialScope(presignedRequest.CredentialScope, settings, out var scopeError, out var statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationQueryParametersError", scopeError!, statusCode);
        }

        if (!TryValidateSigV4aRegionSet(presignedRequest.RegionSet, settings, out var regionError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationQueryParametersError", regionError!, statusCode);
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

        if (!presignedRequest.SignedHeaders.Contains("host", StringComparer.Ordinal)) {
            return IntegratedS3RequestAuthenticationResult.Failure("AuthorizationQueryParametersError", "The presigned request must sign the 'host' header.", statusCode: 400);
        }

        if (!TryValidateTrailerBackedStreamingHeaders(httpContext.Request, isPresigned: true, presignedRequest.SignedHeaders, out var trailerErrorCode, out var trailerError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure(trailerErrorCode!, trailerError!, statusCode);
        }

        if (!TryValidatePresignedSecurityToken(presignedRequest, credential!, out var securityTokenErrorCode, out var securityTokenError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure(securityTokenErrorCode!, securityTokenError!, statusCode);
        }

        if (!TryResolvePayloadHash(httpContext.Request, isPresigned: true, signedHeaders: presignedRequest.SignedHeaders, out var payloadHash, out var payloadHashError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("InvalidRequest", payloadHashError!, statusCode);
        }

        if (!TryBuildCanonicalRequest(httpContext.Request, presignedRequest.SignedHeaders, payloadHash!, PresignedSignatureQueryKey, out var canonicalRequest, out var canonicalError, out statusCode)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", canonicalError!, statusCode);
        }

        var credentialScopeString = S3SigV4aSigner.BuildCredentialScopeString(
            presignedRequest.CredentialScope.DateStamp,
            presignedRequest.CredentialScope.Service);
        var stringToSign = S3SigV4aSigner.BuildStringToSign(presignedRequest.SignedAtUtc, credentialScopeString, canonicalRequest!.CanonicalRequestHashHex);

        using var ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(credential!.SecretAccessKey, credential.AccessKeyId);
        if (!S3SigV4aSigner.VerifySignature(ecdsaKey, stringToSign, presignedRequest.Signature)) {
            return IntegratedS3RequestAuthenticationResult.Failure("SignatureDoesNotMatch", "The presigned request signature does not match the expected signature.");
        }

        StoreSigV4aChunkedTrailerSigningContext(httpContext, payloadHash!, credential, presignedRequest.CredentialScope, presignedRequest.SignedAtUtc);
        return IntegratedS3RequestAuthenticationResult.Success(CreateSigV4aPrincipal(credential));
    }

    private static bool TryValidateSigV4aCredentialScope(S3SigV4CredentialScope credentialScope, IntegratedS3Options settings, out string? error, out int statusCode)
    {
        statusCode = 400;
        if (!string.Equals(credentialScope.Terminator, "aws4_request", StringComparison.Ordinal)) {
            error = "The credential scope must end with 'aws4_request'.";
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

    private static bool TryValidateSigV4aRegionSet(IReadOnlyList<string>? regionSet, IntegratedS3Options settings, out string? error, out int statusCode)
    {
        statusCode = 400;
        if (regionSet is null || regionSet.Count == 0) {
            error = "SigV4a requests must include a valid region set.";
            return false;
        }

        // Accept wildcard
        if (regionSet.Any(r => string.Equals(r, "*", StringComparison.Ordinal))) {
            error = null;
            statusCode = 200;
            return true;
        }

        // Accept when the configured region is in the set
        if (regionSet.Any(r => string.Equals(r, settings.SignatureAuthenticationRegion, StringComparison.Ordinal))) {
            error = null;
            statusCode = 200;
            return true;
        }

        error = $"The region set '{string.Join(",", regionSet)}' does not include the configured region '{settings.SignatureAuthenticationRegion}'.";
        return false;
    }

    private static ClaimsPrincipal CreateSigV4aPrincipal(IntegratedS3AccessKeyCredential credential)
    {
        var claims = new List<Claim>
        {
            new(ClaimTypes.NameIdentifier, credential.AccessKeyId),
            new(ClaimTypes.Name, string.IsNullOrWhiteSpace(credential.DisplayName) ? credential.AccessKeyId : credential.DisplayName),
            new("integrateds3:access-key-id", credential.AccessKeyId),
            new("integrateds3:auth-type", "sigv4a")
        };

        claims.AddRange(credential.Scopes.Where(static scope => !string.IsNullOrWhiteSpace(scope)).Select(static scope => new Claim("scope", scope)));
        return new ClaimsPrincipal(new ClaimsIdentity(claims, authenticationType: "IntegratedS3SigV4a"));
    }

    private static void StoreSigV4aChunkedTrailerSigningContext(
        HttpContext httpContext,
        string payloadHash,
        IntegratedS3AccessKeyCredential credential,
        S3SigV4CredentialScope credentialScope,
        DateTimeOffset requestTimestampUtc)
    {
        if (!IsSignedTrailerBackedPayloadHash(payloadHash)) {
            return;
        }

        AwsChunkedTrailerSigningContextStore.Set(httpContext, new AwsChunkedTrailerSigningContext
        {
            CredentialScope = credentialScope,
            SignedAtUtc = requestTimestampUtc,
            SecretAccessKey = credential.SecretAccessKey,
            IsSigV4a = true,
            AccessKeyId = credential.AccessKeyId
        });
    }

    // ── End SigV4a ─────────────────────────────────────────────────────────

    private static bool TryResolveCredential(IntegratedS3Options settings, string accessKeyId, out IntegratedS3AccessKeyCredential? credential)
    {
        credential = settings.AccessKeyCredentials.FirstOrDefault(candidate => string.Equals(candidate.AccessKeyId, accessKeyId, StringComparison.Ordinal));
        return credential is not null;
    }

    private static bool FixedTimeEqualsOrdinal(string expected, string actual)
    {
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(expected),
            Encoding.UTF8.GetBytes(actual));
    }

    private static bool FixedTimeEqualsOrdinalIgnoreCase(string expected, string actual)
    {
        return FixedTimeEqualsOrdinal(expected.ToUpperInvariant(), actual.ToUpperInvariant());
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

    private static bool TryResolvePayloadHash(
        HttpRequest request,
        bool isPresigned,
        IReadOnlyList<string> signedHeaders,
        out string? payloadHash,
        out string? error,
        out int statusCode)
    {
        var headerValue = request.Headers[AwsContentSha256HeaderName].ToString().Trim();
        var signsPayloadHashHeader = signedHeaders.Contains(AwsContentSha256HeaderName, StringComparer.Ordinal);
        var usesTrailerBackedPayloadHash = IsTrailerBackedStreamingPayloadHash(headerValue);

        if (usesTrailerBackedPayloadHash) {
            if (!IsAwsChunkedContent(request)) {
                payloadHash = null;
                error = $"The '{headerValue}' payload hash requires the 'aws-chunked' content encoding.";
                statusCode = 400;
                return false;
            }

            if (string.IsNullOrWhiteSpace(request.Headers[AwsTrailerHeaderName].ToString())) {
                payloadHash = null;
                error = $"The request must include the '{AwsTrailerHeaderName}' header when '{headerValue}' is used.";
                statusCode = 400;
                return false;
            }
        }

        if (isPresigned) {
            if (usesTrailerBackedPayloadHash && !signsPayloadHashHeader) {
                payloadHash = null;
                error = $"The presigned request must sign the '{AwsContentSha256HeaderName}' header when '{headerValue}' is used.";
                statusCode = 400;
                return false;
            }

            if (signsPayloadHashHeader) {
                if (!string.IsNullOrWhiteSpace(headerValue)) {
                    payloadHash = headerValue.Trim();
                    error = null;
                    statusCode = 200;
                    return true;
                }

                payloadHash = null;
                error = "The presigned request must include the signed x-amz-content-sha256 header.";
                statusCode = 400;
                return false;
            }

            payloadHash = UnsignedPayload;
            error = null;
            statusCode = 200;
            return true;
        }

        if (!string.IsNullOrWhiteSpace(headerValue)) {
            payloadHash = headerValue.Trim();
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

    private static bool IsTrailerBackedStreamingPayloadHash(string? payloadHash)
    {
        return string.Equals(payloadHash, StreamingAws4HmacSha256PayloadTrailer, StringComparison.OrdinalIgnoreCase)
            || string.Equals(payloadHash, StreamingSigV4aPayloadTrailer, StringComparison.OrdinalIgnoreCase)
            || string.Equals(payloadHash, StreamingUnsignedPayloadTrailer, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsSignedTrailerBackedPayloadHash(string? payloadHash)
    {
        return string.Equals(payloadHash, StreamingAws4HmacSha256PayloadTrailer, StringComparison.OrdinalIgnoreCase)
            || string.Equals(payloadHash, StreamingSigV4aPayloadTrailer, StringComparison.OrdinalIgnoreCase);
    }

    private static void StoreAwsChunkedTrailerSigningContext(
        HttpContext httpContext,
        string payloadHash,
        IntegratedS3AccessKeyCredential credential,
        S3SigV4CredentialScope credentialScope,
        DateTimeOffset requestTimestampUtc)
    {
        if (!IsSignedTrailerBackedPayloadHash(payloadHash)) {
            return;
        }

        AwsChunkedTrailerSigningContextStore.Set(httpContext, new AwsChunkedTrailerSigningContext
        {
            CredentialScope = credentialScope,
            SignedAtUtc = requestTimestampUtc,
            SecretAccessKey = credential.SecretAccessKey
        });
    }

    private static bool IsAwsChunkedContent(HttpRequest request)
    {
        if (!request.Headers.TryGetValue("content-encoding", out var contentEncodingValues)) {
            return false;
        }

        return contentEncodingValues
            .Where(static value => value is not null)
            .SelectMany(static value => value!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .Any(static value => string.Equals(value, "aws-chunked", StringComparison.OrdinalIgnoreCase));
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
        return S3SigV4QueryStringParser.Parse(request.QueryString.Value);
    }

    private static IEnumerable<KeyValuePair<string, string?>> EnumerateHeaders(HttpRequest request)
    {
        return request.Headers.Select(static header => new KeyValuePair<string, string?>(header.Key, header.Value.ToString()));
    }
}

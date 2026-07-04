using IntegratedS3.Abstractions.Observability;
using IntegratedS3.AspNetCore.Services;
using IntegratedS3.Protocol;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace IntegratedS3.AspNetCore.Endpoints;

internal sealed class IntegratedS3RequestAuthenticationEndpointFilter(
    IIntegratedS3RequestAuthenticator authenticator,
    IOptionsMonitor<IntegratedS3Options> options,
    ILogger<IntegratedS3RequestAuthenticationEndpointFilter> logger) : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        var httpContext = context.HttpContext;
        var correlationId = IntegratedS3AspNetCoreTelemetry.GetOrCreateCorrelationId(httpContext);
        using var scope = IntegratedS3AspNetCoreTelemetry.BeginRequestScope(logger, httpContext, correlationId);
        using var activity = IntegratedS3AspNetCoreTelemetry.StartRequestActivity(httpContext, correlationId);

        try {
            if (httpContext.User.Identity?.IsAuthenticated != true) {
                var authenticationResult = await authenticator.AuthenticateAsync(httpContext, httpContext.RequestAborted);
                if (authenticationResult.HasAttemptedAuthentication) {
                    if (!authenticationResult.Succeeded) {
                        activity?.SetStatus(ActivityStatusCode.Error, authenticationResult.ErrorMessage);
                        activity?.SetTag(IntegratedS3Observability.Tags.Result, "failure");
                        activity?.SetTag(IntegratedS3Observability.Tags.ErrorCode, authenticationResult.ErrorCode);

                        return new XmlErrorResult(
                            authenticationResult.StatusCode,
                            S3XmlResponseWriter.WriteError(new S3ErrorResponse
                            {
                                Code = authenticationResult.ErrorCode ?? "AccessDenied",
                                Message = authenticationResult.ErrorMessage ?? "Request authentication failed.",
                                Resource = httpContext.Request.PathBase.Add(httpContext.Request.Path).Value,
                                RequestId = httpContext.TraceIdentifier,
                                HostId = httpContext.TraceIdentifier
                            }));
                    }

                    activity?.SetTag(IntegratedS3Observability.Tags.Result, "success");
                    httpContext.User = authenticationResult.Principal!;
                }
                else if (IsAuthenticationRequired(httpContext)) {
                    // Fail closed: authentication is required but the request presented no credentials
                    // (no Authorization header and no presigned X-Amz-Signature). Reject as AccessDenied
                    // instead of falling through to the endpoint as an anonymous principal.
                    activity?.SetStatus(ActivityStatusCode.Error, "Anonymous request rejected.");
                    activity?.SetTag(IntegratedS3Observability.Tags.Result, "failure");
                    activity?.SetTag(IntegratedS3Observability.Tags.ErrorCode, "AccessDenied");

                    return new XmlAuthenticationFailureResult(
                        403,
                        S3XmlResponseWriter.WriteError(new S3ErrorResponse
                        {
                            Code = "AccessDenied",
                            Message = "Anonymous access is not permitted. Requests must be authenticated.",
                            Resource = httpContext.Request.PathBase.Add(httpContext.Request.Path).Value,
                            RequestId = httpContext.TraceIdentifier
                        }));
                }
            }

            return await next(context);
        }
        catch (OperationCanceledException) when (httpContext.RequestAborted.IsCancellationRequested) {
            activity?.SetStatus(ActivityStatusCode.Error, "Cancelled");
            activity?.SetTag(IntegratedS3Observability.Tags.Result, "cancelled");
            throw;
        }
        catch (Exception exception) {
            var (statusCode, errorCode, message) = MapException(exception);
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            activity?.SetTag(IntegratedS3Observability.Tags.Result, "failure");
            activity?.SetTag(IntegratedS3Observability.Tags.ErrorCode, errorCode);
            logger.LogError(exception, "IntegratedS3 request handling failed unexpectedly.");

            // If the response has already begun streaming we can no longer emit a clean error
            // body; rethrow and let the host abort the connection.
            if (httpContext.Response.HasStarted) {
                throw;
            }

            // Translate any otherwise-unhandled exception into a well-formed S3 <Error> XML
            // response so SDK XML error parsers can read it (and retry on 5xx), instead of
            // ASP.NET's default non-S3 error (empty/text body, no x-amz-request-id).
            return new XmlErrorResult(
                statusCode,
                S3XmlResponseWriter.WriteError(new S3ErrorResponse
                {
                    Code = errorCode,
                    Message = message,
                    Resource = httpContext.Request.PathBase.Add(httpContext.Request.Path).Value,
                    RequestId = httpContext.TraceIdentifier,
                    HostId = httpContext.TraceIdentifier
                }));
        }
    }

    /// <summary>
    /// Determines whether an unauthenticated request should be rejected. Authentication is required when
    /// either SigV4 authentication or <see cref="IntegratedS3Options.RequireAuthenticatedRequests"/> is enabled,
    /// unless anonymous access has been explicitly allowed — globally via
    /// <see cref="IntegratedS3Options.AllowAnonymousRequests"/> or per-route via an <see cref="IAllowAnonymous"/>
    /// convention (for example <c>RouteGroupBuilder.AllowAnonymous()</c>).
    /// </summary>
    private bool IsAuthenticationRequired(HttpContext httpContext)
    {
        var settings = options.CurrentValue;
        if (settings.AllowAnonymousRequests) {
            return false;
        }

        if (!settings.EnableAwsSignatureV4Authentication && !settings.RequireAuthenticatedRequests) {
            return false;
        }

        return httpContext.GetEndpoint()?.Metadata.GetMetadata<IAllowAnonymous>() is null;
    }

    /// <summary>
    /// Maps an otherwise-unhandled exception to the S3 status code, error code and message.
    /// Framework request errors (e.g. Kestrel body-size limit exceeded) carry their own
    /// intended HTTP status via <see cref="BadHttpRequestException"/>; everything else is a
    /// server-side <c>InternalError</c> 500.
    /// </summary>
    private static (int StatusCode, string Code, string Message) MapException(Exception exception)
    {
        if (exception is ContentSha256MismatchException) {
            return (
                StatusCodes.Status400BadRequest,
                "XAmzContentSHA256Mismatch",
                "The provided 'x-amz-content-sha256' header does not match what was computed.");
        }

        if (exception is ChunkSignatureMismatchException) {
            return (
                StatusCodes.Status403Forbidden,
                "SignatureDoesNotMatch",
                "The request signature we calculated does not match the signature you provided.");
        }

        if (exception is BadHttpRequestException badRequest) {
            return badRequest.StatusCode switch
            {
                StatusCodes.Status413PayloadTooLarge => (
                    StatusCodes.Status413PayloadTooLarge,
                    "EntityTooLarge",
                    "Your proposed upload exceeds the maximum allowed object size."),
                StatusCodes.Status400BadRequest => (
                    StatusCodes.Status400BadRequest,
                    "InvalidRequest",
                    "The request could not be understood by the server."),
                _ => (
                    badRequest.StatusCode,
                    "InvalidRequest",
                    "The request could not be understood by the server."),
            };
        }

        return (
            StatusCodes.Status500InternalServerError,
            InternalErrorCode,
            "We encountered an internal error. Please try again.");
    }

    private const string InternalErrorCode = "InternalError";

    private sealed class XmlAuthenticationFailureResult(int statusCode, string content) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);
            httpContext.Response.StatusCode = statusCode;
            httpContext.Response.ContentType = "application/xml";
            httpContext.Response.Headers["x-amz-request-id"] = httpContext.TraceIdentifier;
            httpContext.Response.Headers["x-amz-id-2"] = httpContext.TraceIdentifier;
            await httpContext.Response.WriteAsync(content, httpContext.RequestAborted);
        }
    }

    private sealed class XmlErrorResult(int statusCode, string content) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);
            httpContext.Response.StatusCode = statusCode;
            httpContext.Response.ContentType = "application/xml";
            httpContext.Response.Headers["x-amz-request-id"] = httpContext.TraceIdentifier;
            httpContext.Response.Headers["x-amz-id-2"] = httpContext.TraceIdentifier;
            await httpContext.Response.WriteAsync(content, httpContext.RequestAborted);
        }
    }
}

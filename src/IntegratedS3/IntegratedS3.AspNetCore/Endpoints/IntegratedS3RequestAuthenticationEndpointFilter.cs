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

                        return new XmlAuthenticationFailureResult(
                            authenticationResult.StatusCode,
                            S3XmlResponseWriter.WriteError(new S3ErrorResponse
                            {
                                Code = authenticationResult.ErrorCode ?? "AccessDenied",
                                Message = authenticationResult.ErrorMessage ?? "Request authentication failed.",
                                Resource = httpContext.Request.PathBase.Add(httpContext.Request.Path).Value,
                                RequestId = httpContext.TraceIdentifier
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
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            activity?.SetTag(IntegratedS3Observability.Tags.Result, "failure");
            logger.LogError(exception, "IntegratedS3 request handling failed unexpectedly.");
            throw;
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

    private sealed class XmlAuthenticationFailureResult(int statusCode, string content) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);
            httpContext.Response.StatusCode = statusCode;
            httpContext.Response.ContentType = "application/xml";
            await httpContext.Response.WriteAsync(content, httpContext.RequestAborted);
        }
    }
}

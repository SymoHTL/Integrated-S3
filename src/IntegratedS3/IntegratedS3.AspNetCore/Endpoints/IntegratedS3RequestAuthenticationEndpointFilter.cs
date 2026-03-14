using IntegratedS3.Abstractions.Observability;
using IntegratedS3.AspNetCore.Services;
using IntegratedS3.Protocol;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace IntegratedS3.AspNetCore.Endpoints;

internal sealed class IntegratedS3RequestAuthenticationEndpointFilter(
    IIntegratedS3RequestAuthenticator authenticator,
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

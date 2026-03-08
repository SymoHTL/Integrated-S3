using IntegratedS3.Protocol;
using IntegratedS3.AspNetCore.Services;
using Microsoft.AspNetCore.Http;

namespace IntegratedS3.AspNetCore.Endpoints;

internal sealed class IntegratedS3RequestAuthenticationEndpointFilter(IIntegratedS3RequestAuthenticator authenticator) : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        var httpContext = context.HttpContext;
        if (httpContext.User.Identity?.IsAuthenticated != true) {
            var authenticationResult = await authenticator.AuthenticateAsync(httpContext, httpContext.RequestAborted);
            if (authenticationResult.HasAttemptedAuthentication) {
                if (!authenticationResult.Succeeded) {
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

                httpContext.User = authenticationResult.Principal!;
            }
        }

        return await next(context);
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

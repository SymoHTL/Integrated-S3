using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Tests.Infrastructure;

internal sealed class TestHeaderAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Headers.Authorization.Any()) {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        if (!Request.Headers.TryGetValue("Authorization", out var authorizationValues)) {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        var authorizationValue = authorizationValues.ToString();
        const string prefix = "TestHeader ";
        if (!authorizationValue.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        var permissions = authorizationValue[prefix.Length..]
            .Split([',', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var claims = permissions.Select(static permission => new Claim("scope", permission)).ToList();
        claims.Add(new Claim(ClaimTypes.Name, "test-user"));

        var identity = new ClaimsIdentity(claims, Scheme.Name);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, Scheme.Name);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}
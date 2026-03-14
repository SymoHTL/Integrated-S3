using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace IntegratedS3.AspNetCore.Endpoints;

public static class IntegratedS3HealthEndpointRouteBuilderExtensions
{
    private const string DefaultLivenessPattern = "/health/live";
    private const string DefaultReadinessPattern = "/health/ready";

    public static IEndpointConventionBuilder MapIntegratedS3LivenessHealthCheck(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        return endpoints.MapIntegratedS3LivenessHealthCheck(DefaultLivenessPattern);
    }

    public static IEndpointConventionBuilder MapIntegratedS3LivenessHealthCheck(this IEndpointRouteBuilder endpoints, string pattern)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        if (string.IsNullOrWhiteSpace(pattern)) {
            throw new ArgumentException("Health check route pattern is required.", nameof(pattern));
        }

        return endpoints.MapHealthChecks(pattern, new HealthCheckOptions
        {
            Predicate = static _ => false
        }).ExcludeFromDescription();
    }

    public static IEndpointConventionBuilder MapIntegratedS3ReadinessHealthCheck(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        return endpoints.MapIntegratedS3ReadinessHealthCheck(DefaultReadinessPattern);
    }

    public static IEndpointConventionBuilder MapIntegratedS3ReadinessHealthCheck(this IEndpointRouteBuilder endpoints, string pattern)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        if (string.IsNullOrWhiteSpace(pattern)) {
            throw new ArgumentException("Health check route pattern is required.", nameof(pattern));
        }

        return endpoints.MapHealthChecks(pattern, new HealthCheckOptions
        {
            Predicate = static registration => registration.Tags.Contains(IntegratedS3HealthCheckTags.Readiness),
            ResultStatusCodes =
            {
                [HealthStatus.Healthy] = StatusCodes.Status200OK,
                [HealthStatus.Degraded] = StatusCodes.Status503ServiceUnavailable,
                [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable
            }
        }).ExcludeFromDescription();
    }

    public static IEndpointRouteBuilder MapIntegratedS3HealthEndpoints(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        return endpoints.MapIntegratedS3HealthEndpoints(DefaultLivenessPattern, DefaultReadinessPattern);
    }

    public static IEndpointRouteBuilder MapIntegratedS3HealthEndpoints(this IEndpointRouteBuilder endpoints, string livenessPattern, string readinessPattern)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        endpoints.MapIntegratedS3LivenessHealthCheck(livenessPattern);
        endpoints.MapIntegratedS3ReadinessHealthCheck(readinessPattern);
        return endpoints;
    }
}

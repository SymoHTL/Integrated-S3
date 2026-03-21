using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace IntegratedS3.AspNetCore.Endpoints;

/// <summary>
/// Extension methods for mapping IntegratedS3 health check endpoints.
/// </summary>
public static class IntegratedS3HealthEndpointRouteBuilderExtensions
{
    private const string DefaultLivenessPattern = "/health/live";
    private const string DefaultReadinessPattern = "/health/ready";

    /// <summary>
    /// Maps a liveness probe endpoint at the default pattern <c>/health/live</c>.
    /// The liveness check always returns healthy; no health checks are evaluated.
    /// </summary>
    /// <param name="endpoints">The <see cref="IEndpointRouteBuilder"/> to add the endpoint to.</param>
    /// <returns>An <see cref="IEndpointConventionBuilder"/> that can be used to further customize the endpoint.</returns>
    public static IEndpointConventionBuilder MapIntegratedS3LivenessHealthCheck(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        return endpoints.MapIntegratedS3LivenessHealthCheck(DefaultLivenessPattern);
    }

    /// <summary>
    /// Maps a liveness probe endpoint at the specified route pattern.
    /// The liveness check always returns healthy; no health checks are evaluated.
    /// </summary>
    /// <param name="endpoints">The <see cref="IEndpointRouteBuilder"/> to add the endpoint to.</param>
    /// <param name="pattern">The route pattern for the liveness endpoint.</param>
    /// <returns>An <see cref="IEndpointConventionBuilder"/> that can be used to further customize the endpoint.</returns>
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

    /// <summary>
    /// Maps a readiness probe endpoint at the default pattern <c>/health/ready</c>.
    /// Evaluates health checks tagged with <see cref="IntegratedS3HealthCheckTags.Readiness"/>.
    /// Returns HTTP 503 for degraded or unhealthy status.
    /// </summary>
    /// <param name="endpoints">The <see cref="IEndpointRouteBuilder"/> to add the endpoint to.</param>
    /// <returns>An <see cref="IEndpointConventionBuilder"/> that can be used to further customize the endpoint.</returns>
    public static IEndpointConventionBuilder MapIntegratedS3ReadinessHealthCheck(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        return endpoints.MapIntegratedS3ReadinessHealthCheck(DefaultReadinessPattern);
    }

    /// <summary>
    /// Maps a readiness probe endpoint at the specified route pattern.
    /// Evaluates health checks tagged with <see cref="IntegratedS3HealthCheckTags.Readiness"/>.
    /// Returns HTTP 503 for degraded or unhealthy status.
    /// </summary>
    /// <param name="endpoints">The <see cref="IEndpointRouteBuilder"/> to add the endpoint to.</param>
    /// <param name="pattern">The route pattern for the readiness endpoint.</param>
    /// <returns>An <see cref="IEndpointConventionBuilder"/> that can be used to further customize the endpoint.</returns>
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

    /// <summary>
    /// Maps both liveness and readiness health check endpoints using the default patterns
    /// <c>/health/live</c> and <c>/health/ready</c>.
    /// </summary>
    /// <param name="endpoints">The <see cref="IEndpointRouteBuilder"/> to add the endpoints to.</param>
    /// <returns>The <see cref="IEndpointRouteBuilder"/> so that additional calls can be chained.</returns>
    public static IEndpointRouteBuilder MapIntegratedS3HealthEndpoints(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        return endpoints.MapIntegratedS3HealthEndpoints(DefaultLivenessPattern, DefaultReadinessPattern);
    }

    /// <summary>
    /// Maps both liveness and readiness health check endpoints using the specified route patterns.
    /// </summary>
    /// <param name="endpoints">The <see cref="IEndpointRouteBuilder"/> to add the endpoints to.</param>
    /// <param name="livenessPattern">The route pattern for the liveness endpoint.</param>
    /// <param name="readinessPattern">The route pattern for the readiness endpoint.</param>
    /// <returns>The <see cref="IEndpointRouteBuilder"/> so that additional calls can be chained.</returns>
    public static IEndpointRouteBuilder MapIntegratedS3HealthEndpoints(this IEndpointRouteBuilder endpoints, string livenessPattern, string readinessPattern)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        endpoints.MapIntegratedS3LivenessHealthCheck(livenessPattern);
        endpoints.MapIntegratedS3ReadinessHealthCheck(readinessPattern);
        return endpoints;
    }
}

using IntegratedS3.AspNetCore.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace IntegratedS3.AspNetCore.DependencyInjection;

/// <summary>
/// Extension methods for registering IntegratedS3 backend health checks
/// with an <see cref="IHealthChecksBuilder"/>.
/// </summary>
public static class IntegratedS3HealthChecksBuilderExtensions
{
    private const string DefaultHealthCheckName = "integrated-s3-backends";

    /// <summary>
    /// Registers an IntegratedS3 backend health check with the default name
    /// <c>"integrated-s3-backends"</c>.
    /// </summary>
    /// <param name="builder">The <see cref="IHealthChecksBuilder"/> to add the check to.</param>
    /// <returns>The <see cref="IHealthChecksBuilder"/> so that additional calls can be chained.</returns>
    public static IHealthChecksBuilder AddIntegratedS3BackendHealthCheck(this IHealthChecksBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddIntegratedS3BackendHealthCheck(DefaultHealthCheckName);
    }

    /// <summary>
    /// Registers an IntegratedS3 backend health check with a custom name, failure status, tags, and timeout.
    /// </summary>
    /// <param name="builder">The <see cref="IHealthChecksBuilder"/> to add the check to.</param>
    /// <param name="name">The name of the health check registration.</param>
    /// <param name="failureStatus">
    /// The <see cref="HealthStatus"/> to report when the health check fails.
    /// If <see langword="null"/>, the default failure status is used.
    /// </param>
    /// <param name="tags">
    /// Tags to apply to the health check. If <see langword="null"/>, the
    /// <see cref="IntegratedS3HealthCheckTags.Readiness"/> tag is applied by default.
    /// </param>
    /// <param name="timeout">
    /// The timeout for the health check. If <see langword="null"/>, no timeout is applied.
    /// </param>
    /// <returns>The <see cref="IHealthChecksBuilder"/> so that additional calls can be chained.</returns>
    public static IHealthChecksBuilder AddIntegratedS3BackendHealthCheck(
        this IHealthChecksBuilder builder,
        string name,
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (string.IsNullOrWhiteSpace(name)) {
            throw new ArgumentException("Health check name is required.", nameof(name));
        }

        var normalizedTags = tags ?? [IntegratedS3HealthCheckTags.Readiness];
        return builder.AddCheck<IntegratedS3BackendHealthCheck>(name, failureStatus, normalizedTags, timeout);
    }
}

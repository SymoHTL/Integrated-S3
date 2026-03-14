using IntegratedS3.AspNetCore.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace IntegratedS3.AspNetCore.DependencyInjection;

public static class IntegratedS3HealthChecksBuilderExtensions
{
    private const string DefaultHealthCheckName = "integrated-s3-backends";

    public static IHealthChecksBuilder AddIntegratedS3BackendHealthCheck(this IHealthChecksBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddIntegratedS3BackendHealthCheck(DefaultHealthCheckName);
    }

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

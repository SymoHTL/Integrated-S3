namespace IntegratedS3.AspNetCore;

/// <summary>
/// Well-known health check tag constants for IntegratedS3.
/// </summary>
public static class IntegratedS3HealthCheckTags
{
    /// <summary>
    /// Tag applied to IntegratedS3 readiness health checks.
    /// Use this tag to filter health check endpoints that verify backend storage availability.
    /// </summary>
    public const string Readiness = "integrated-s3-readiness";
}

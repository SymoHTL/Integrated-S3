namespace IntegratedS3.Core.Options;

/// <summary>
/// Configures the optional EF Core-backed catalog integration.
/// </summary>
public sealed class EntityFrameworkCatalogOptions
{
    /// <summary>Whether the integration should call <c>EnsureCreated</c> for the consumer-owned DbContext on startup.</summary>
    public bool EnsureCreated { get; set; } = true;
}

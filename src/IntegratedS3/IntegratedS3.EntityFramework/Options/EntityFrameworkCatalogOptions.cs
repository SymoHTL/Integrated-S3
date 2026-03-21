namespace IntegratedS3.Core.Options;

/// <summary>
/// Configuration options for the Entity Framework Core storage catalog integration.
/// </summary>
public sealed class EntityFrameworkCatalogOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether <c>Database.EnsureCreated</c> is called on startup
    /// to automatically create the IntegratedS3 catalog tables if they do not exist. Defaults to <see langword="true"/>.
    /// </summary>
    public bool EnsureCreated { get; set; } = true;
}
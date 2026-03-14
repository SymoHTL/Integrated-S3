namespace IntegratedS3.Client;

/// <summary>
/// Options for registering <see cref="IntegratedS3Client"/> through dependency injection.
/// </summary>
public sealed class IntegratedS3ClientOptions
{
    /// <summary>
    /// The default relative route prefix used by IntegratedS3 hosts.
    /// </summary>
    public const string DefaultRoutePrefix = "integrated-s3";

    /// <summary>
    /// The absolute base address of the IntegratedS3 host.
    /// When omitted, callers can still configure the named HttpClient manually via <c>IHttpClientBuilder</c>.
    /// </summary>
    public Uri? BaseAddress { get; set; }

    /// <summary>
    /// The relative route prefix used to reach the IntegratedS3 endpoint surface.
    /// </summary>
    public string RoutePrefix { get; set; } = DefaultRoutePrefix;
}

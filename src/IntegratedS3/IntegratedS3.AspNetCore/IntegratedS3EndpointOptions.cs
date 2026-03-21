using Microsoft.AspNetCore.Routing;

namespace IntegratedS3.AspNetCore;

/// <summary>
/// Code-driven endpoint configuration options for IntegratedS3. Use this when you need callback-based
/// route group customization (e.g., adding filters, metadata, or conventions).
/// </summary>
/// <remarks>
/// For AOT/trimming-safe configuration from <see cref="Microsoft.Extensions.Configuration.IConfiguration"/>,
/// prefer <see cref="DependencyInjection.IntegratedS3EndpointConfigurationOptions"/>.
/// </remarks>
public sealed class IntegratedS3EndpointOptions
{
    private readonly Dictionary<IntegratedS3EndpointFeature, Action<RouteGroupBuilder>?> featureRouteGroupConfigurations = [];

    /// <summary>
    /// Enables S3 service-level endpoints (ListBuckets, service discovery). Defaults to <see langword="true"/>.
    /// </summary>
    public bool EnableServiceEndpoints { get; set; } = true;

    /// <summary>
    /// Enables bucket-level endpoints (CreateBucket, DeleteBucket, ListObjects, etc.). Defaults to <see langword="true"/>.
    /// </summary>
    public bool EnableBucketEndpoints { get; set; } = true;

    /// <summary>
    /// Enables object-level endpoints (GetObject, PutObject, DeleteObject, HeadObject, CopyObject, etc.). Defaults to <see langword="true"/>.
    /// </summary>
    public bool EnableObjectEndpoints { get; set; } = true;

    /// <summary>
    /// Enables multipart upload endpoints (CreateMultipartUpload, UploadPart, CompleteMultipartUpload, etc.). Defaults to <see langword="true"/>.
    /// </summary>
    public bool EnableMultipartEndpoints { get; set; } = true;

    /// <summary>
    /// Enables administrative endpoints (diagnostics, repair operations). Defaults to <see langword="true"/>.
    /// </summary>
    public bool EnableAdminEndpoints { get; set; } = true;

    /// <summary>
    /// Authorization options applied to the top-level route group. Affects all endpoints.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? RouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for root-level GET endpoints (ListBuckets, service info).
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? RootRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for S3-compatibility endpoints that exist outside the standard route structure.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? CompatibilityRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for service-level endpoints.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? ServiceRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for bucket-level endpoints.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? BucketRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for object-level endpoints.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? ObjectRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for multipart upload endpoints.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? MultipartRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for administrative endpoints.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? AdminRouteAuthorization { get; set; }

    /// <summary>
    /// Callback to configure the top-level route group builder. Applied to all endpoints.
    /// </summary>
    public Action<RouteGroupBuilder>? ConfigureRouteGroup { get; set; }

    /// <summary>
    /// Callback to configure the root-level (GET /) route group.
    /// </summary>
    public Action<RouteGroupBuilder>? ConfigureRootRouteGroup { get; set; }

    /// <summary>
    /// Callback to configure the S3-compatibility route group.
    /// </summary>
    public Action<RouteGroupBuilder>? ConfigureCompatibilityRouteGroup { get; set; }

    /// <summary>
    /// Callback to configure the service endpoint route group.
    /// </summary>
    /// <remarks>
    /// Delegates to <see cref="GetFeatureRouteGroupConfiguration"/> and
    /// <see cref="SetFeatureRouteGroupConfiguration"/>.
    /// </remarks>
    public Action<RouteGroupBuilder>? ConfigureServiceRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Service);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Service, value);
    }

    /// <summary>
    /// Callback to configure the bucket endpoint route group.
    /// </summary>
    /// <remarks>
    /// Delegates to <see cref="GetFeatureRouteGroupConfiguration"/> and
    /// <see cref="SetFeatureRouteGroupConfiguration"/>.
    /// </remarks>
    public Action<RouteGroupBuilder>? ConfigureBucketRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Bucket);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Bucket, value);
    }

    /// <summary>
    /// Callback to configure the object endpoint route group.
    /// </summary>
    /// <remarks>
    /// Delegates to <see cref="GetFeatureRouteGroupConfiguration"/> and
    /// <see cref="SetFeatureRouteGroupConfiguration"/>.
    /// </remarks>
    public Action<RouteGroupBuilder>? ConfigureObjectRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Object);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Object, value);
    }

    /// <summary>
    /// Callback to configure the multipart endpoint route group.
    /// </summary>
    /// <remarks>
    /// Delegates to <see cref="GetFeatureRouteGroupConfiguration"/> and
    /// <see cref="SetFeatureRouteGroupConfiguration"/>.
    /// </remarks>
    public Action<RouteGroupBuilder>? ConfigureMultipartRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Multipart);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Multipart, value);
    }

    /// <summary>
    /// Callback to configure the admin endpoint route group.
    /// </summary>
    /// <remarks>
    /// Delegates to <see cref="GetFeatureRouteGroupConfiguration"/> and
    /// <see cref="SetFeatureRouteGroupConfiguration"/>.
    /// </remarks>
    public Action<RouteGroupBuilder>? ConfigureAdminRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Admin);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Admin, value);
    }

    /// <summary>
    /// Gets the route group configuration callback for the specified endpoint feature.
    /// </summary>
    /// <param name="feature">The endpoint feature to retrieve the configuration for.</param>
    /// <returns>The configuration callback, or <see langword="null"/> if none has been set.</returns>
    public Action<RouteGroupBuilder>? GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature feature)
    {
        ValidateFeature(feature);

        return featureRouteGroupConfigurations.TryGetValue(feature, out var configuration)
            ? configuration
            : null;
    }

    /// <summary>
    /// Sets or removes the route group configuration callback for the specified endpoint feature.
    /// </summary>
    /// <param name="feature">The endpoint feature to configure.</param>
    /// <param name="configuration">The configuration callback, or <see langword="null"/> to remove the existing configuration.</param>
    public void SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature feature, Action<RouteGroupBuilder>? configuration)
    {
        ValidateFeature(feature);

        if (configuration is null) {
            featureRouteGroupConfigurations.Remove(feature);
            return;
        }

        featureRouteGroupConfigurations[feature] = configuration;
    }

    internal IntegratedS3EndpointOptions Clone()
    {
        var clone = new IntegratedS3EndpointOptions
        {
            EnableServiceEndpoints = EnableServiceEndpoints,
            EnableBucketEndpoints = EnableBucketEndpoints,
            EnableObjectEndpoints = EnableObjectEndpoints,
            EnableMultipartEndpoints = EnableMultipartEndpoints,
            EnableAdminEndpoints = EnableAdminEndpoints,
            RouteAuthorization = RouteAuthorization?.Clone(),
            RootRouteAuthorization = RootRouteAuthorization?.Clone(),
            CompatibilityRouteAuthorization = CompatibilityRouteAuthorization?.Clone(),
            ServiceRouteAuthorization = ServiceRouteAuthorization?.Clone(),
            BucketRouteAuthorization = BucketRouteAuthorization?.Clone(),
            ObjectRouteAuthorization = ObjectRouteAuthorization?.Clone(),
            MultipartRouteAuthorization = MultipartRouteAuthorization?.Clone(),
            AdminRouteAuthorization = AdminRouteAuthorization?.Clone(),
            ConfigureRouteGroup = ConfigureRouteGroup,
            ConfigureRootRouteGroup = ConfigureRootRouteGroup,
            ConfigureCompatibilityRouteGroup = ConfigureCompatibilityRouteGroup
        };

        foreach (var (feature, configuration) in featureRouteGroupConfigurations) {
            clone.featureRouteGroupConfigurations[feature] = configuration;
        }

        return clone;
    }

    private static void ValidateFeature(IntegratedS3EndpointFeature feature)
    {
        if (!Enum.IsDefined(feature)) {
            throw new ArgumentOutOfRangeException(nameof(feature), feature, "Unknown Integrated S3 endpoint feature.");
        }
    }
}

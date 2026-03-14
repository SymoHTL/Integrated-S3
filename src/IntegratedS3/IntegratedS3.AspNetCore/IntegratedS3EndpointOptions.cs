using Microsoft.AspNetCore.Routing;

namespace IntegratedS3.AspNetCore;

/// <summary>
/// Configures which endpoint groups are mapped and how the root route groups should be customized.
/// </summary>
public sealed class IntegratedS3EndpointOptions
{
    private readonly Dictionary<IntegratedS3EndpointFeature, Action<RouteGroupBuilder>?> featureRouteGroupConfigurations = [];

    /// <summary>Whether service-document routes should be mapped.</summary>
    public bool EnableServiceEndpoints { get; set; } = true;

    /// <summary>Whether bucket routes should be mapped.</summary>
    public bool EnableBucketEndpoints { get; set; } = true;

    /// <summary>Whether object routes should be mapped.</summary>
    public bool EnableObjectEndpoints { get; set; } = true;

    /// <summary>Whether multipart routes should be mapped.</summary>
    public bool EnableMultipartEndpoints { get; set; } = true;

    /// <summary>Whether admin and capability routes should be mapped.</summary>
    public bool EnableAdminEndpoints { get; set; } = true;

    /// <summary>Authorization conventions applied to the main IntegratedS3 route group.</summary>
    public IntegratedS3EndpointAuthorizationOptions? RouteAuthorization { get; set; }

    /// <summary>Authorization conventions applied to the shared root GET route group.</summary>
    public IntegratedS3EndpointAuthorizationOptions? RootRouteAuthorization { get; set; }

    /// <summary>Authorization conventions applied to the shared S3-compatible compatibility route group.</summary>
    public IntegratedS3EndpointAuthorizationOptions? CompatibilityRouteAuthorization { get; set; }

    /// <summary>Authorization conventions applied to the service route group.</summary>
    public IntegratedS3EndpointAuthorizationOptions? ServiceRouteAuthorization { get; set; }

    /// <summary>Authorization conventions applied to the bucket route group.</summary>
    public IntegratedS3EndpointAuthorizationOptions? BucketRouteAuthorization { get; set; }

    /// <summary>Authorization conventions applied to the object route group.</summary>
    public IntegratedS3EndpointAuthorizationOptions? ObjectRouteAuthorization { get; set; }

    /// <summary>Authorization conventions applied to the multipart route group.</summary>
    public IntegratedS3EndpointAuthorizationOptions? MultipartRouteAuthorization { get; set; }

    /// <summary>Authorization conventions applied to the admin route group.</summary>
    public IntegratedS3EndpointAuthorizationOptions? AdminRouteAuthorization { get; set; }

    /// <summary>Applies conventions to the main IntegratedS3 route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureRouteGroup { get; set; }

    /// <summary>Applies conventions to the shared root GET route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureRootRouteGroup { get; set; }

    /// <summary>Applies conventions to the shared S3-compatible compatibility route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureCompatibilityRouteGroup { get; set; }

    /// <summary>Applies conventions to the service route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureServiceRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Service);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Service, value);
    }

    /// <summary>Applies conventions to the bucket route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureBucketRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Bucket);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Bucket, value);
    }

    /// <summary>Applies conventions to the object route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureObjectRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Object);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Object, value);
    }

    /// <summary>Applies conventions to the multipart route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureMultipartRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Multipart);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Multipart, value);
    }

    /// <summary>Applies conventions to the admin route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureAdminRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Admin);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Admin, value);
    }

    /// <summary>Gets the route-group convention delegate for a specific endpoint feature.</summary>
    public Action<RouteGroupBuilder>? GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature feature)
    {
        ValidateFeature(feature);

        return featureRouteGroupConfigurations.TryGetValue(feature, out var configuration)
            ? configuration
            : null;
    }

    /// <summary>Sets or clears the route-group convention delegate for a specific endpoint feature.</summary>
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

using Microsoft.AspNetCore.Routing;

namespace IntegratedS3.AspNetCore;

public sealed class IntegratedS3EndpointOptions
{
    private readonly Dictionary<IntegratedS3EndpointFeature, Action<RouteGroupBuilder>?> featureRouteGroupConfigurations = [];

    public bool EnableServiceEndpoints { get; set; } = true;

    public bool EnableBucketEndpoints { get; set; } = true;

    public bool EnableObjectEndpoints { get; set; } = true;

    public bool EnableMultipartEndpoints { get; set; } = true;

    public bool EnableAdminEndpoints { get; set; } = true;

    public IntegratedS3EndpointAuthorizationOptions? RouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? RootRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? CompatibilityRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? ServiceRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? BucketRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? ObjectRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? MultipartRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? AdminRouteAuthorization { get; set; }

    public Action<RouteGroupBuilder>? ConfigureRouteGroup { get; set; }

    public Action<RouteGroupBuilder>? ConfigureRootRouteGroup { get; set; }

    public Action<RouteGroupBuilder>? ConfigureCompatibilityRouteGroup { get; set; }

    public Action<RouteGroupBuilder>? ConfigureServiceRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Service);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Service, value);
    }

    public Action<RouteGroupBuilder>? ConfigureBucketRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Bucket);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Bucket, value);
    }

    public Action<RouteGroupBuilder>? ConfigureObjectRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Object);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Object, value);
    }

    public Action<RouteGroupBuilder>? ConfigureMultipartRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Multipart);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Multipart, value);
    }

    public Action<RouteGroupBuilder>? ConfigureAdminRouteGroup
    {
        get => GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Admin);
        set => SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Admin, value);
    }

    public Action<RouteGroupBuilder>? GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature feature)
    {
        ValidateFeature(feature);

        return featureRouteGroupConfigurations.TryGetValue(feature, out var configuration)
            ? configuration
            : null;
    }

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

using Microsoft.AspNetCore.Routing;

namespace IntegratedS3.AspNetCore;

public sealed class IntegratedS3EndpointOptions
{
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

    public Action<RouteGroupBuilder>? ConfigureServiceRouteGroup { get; set; }

    public Action<RouteGroupBuilder>? ConfigureBucketRouteGroup { get; set; }

    public Action<RouteGroupBuilder>? ConfigureObjectRouteGroup { get; set; }

    public Action<RouteGroupBuilder>? ConfigureMultipartRouteGroup { get; set; }

    public Action<RouteGroupBuilder>? ConfigureAdminRouteGroup { get; set; }

    internal IntegratedS3EndpointOptions Clone()
    {
        return new IntegratedS3EndpointOptions
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
            ConfigureCompatibilityRouteGroup = ConfigureCompatibilityRouteGroup,
            ConfigureServiceRouteGroup = ConfigureServiceRouteGroup,
            ConfigureBucketRouteGroup = ConfigureBucketRouteGroup,
            ConfigureObjectRouteGroup = ConfigureObjectRouteGroup,
            ConfigureMultipartRouteGroup = ConfigureMultipartRouteGroup,
            ConfigureAdminRouteGroup = ConfigureAdminRouteGroup
        };
    }
}

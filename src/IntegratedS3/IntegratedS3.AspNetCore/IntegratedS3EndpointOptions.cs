using Microsoft.AspNetCore.Routing;

namespace IntegratedS3.AspNetCore;

public sealed class IntegratedS3EndpointOptions
{
    public bool EnableServiceEndpoints { get; set; } = true;

    public bool EnableBucketEndpoints { get; set; } = true;

    public bool EnableObjectEndpoints { get; set; } = true;

    public bool EnableMultipartEndpoints { get; set; } = true;

    public bool EnableAdminEndpoints { get; set; } = true;

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

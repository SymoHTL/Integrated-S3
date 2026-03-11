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

    internal IntegratedS3EndpointOptions Clone()
    {
        return new IntegratedS3EndpointOptions
        {
            EnableServiceEndpoints = EnableServiceEndpoints,
            EnableBucketEndpoints = EnableBucketEndpoints,
            EnableObjectEndpoints = EnableObjectEndpoints,
            EnableMultipartEndpoints = EnableMultipartEndpoints,
            EnableAdminEndpoints = EnableAdminEndpoints,
            ConfigureRouteGroup = ConfigureRouteGroup
        };
    }
}

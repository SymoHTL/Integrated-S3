using IntegratedS3.AspNetCore;

namespace IntegratedS3.AspNetCore.DependencyInjection;

public sealed class IntegratedS3EndpointConfigurationOptions
{
    public bool? EnableServiceEndpoints { get; set; }

    public bool? EnableBucketEndpoints { get; set; }

    public bool? EnableObjectEndpoints { get; set; }

    public bool? EnableMultipartEndpoints { get; set; }

    public bool? EnableAdminEndpoints { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? RouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? RootRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? CompatibilityRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? ServiceRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? BucketRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? ObjectRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? MultipartRouteAuthorization { get; set; }

    public IntegratedS3EndpointAuthorizationOptions? AdminRouteAuthorization { get; set; }

    internal IntegratedS3EndpointConfigurationOptions Clone()
    {
        return new IntegratedS3EndpointConfigurationOptions
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
            AdminRouteAuthorization = AdminRouteAuthorization?.Clone()
        };
    }

    internal void ApplyTo(IntegratedS3EndpointOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (EnableServiceEndpoints is bool enableServiceEndpoints) {
            options.EnableServiceEndpoints = enableServiceEndpoints;
        }

        if (EnableBucketEndpoints is bool enableBucketEndpoints) {
            options.EnableBucketEndpoints = enableBucketEndpoints;
        }

        if (EnableObjectEndpoints is bool enableObjectEndpoints) {
            options.EnableObjectEndpoints = enableObjectEndpoints;
        }

        if (EnableMultipartEndpoints is bool enableMultipartEndpoints) {
            options.EnableMultipartEndpoints = enableMultipartEndpoints;
        }

        if (EnableAdminEndpoints is bool enableAdminEndpoints) {
            options.EnableAdminEndpoints = enableAdminEndpoints;
        }

        if (RouteAuthorization is not null) {
            options.RouteAuthorization = RouteAuthorization.Clone();
        }

        if (RootRouteAuthorization is not null) {
            options.RootRouteAuthorization = RootRouteAuthorization.Clone();
        }

        if (CompatibilityRouteAuthorization is not null) {
            options.CompatibilityRouteAuthorization = CompatibilityRouteAuthorization.Clone();
        }

        if (ServiceRouteAuthorization is not null) {
            options.ServiceRouteAuthorization = ServiceRouteAuthorization.Clone();
        }

        if (BucketRouteAuthorization is not null) {
            options.BucketRouteAuthorization = BucketRouteAuthorization.Clone();
        }

        if (ObjectRouteAuthorization is not null) {
            options.ObjectRouteAuthorization = ObjectRouteAuthorization.Clone();
        }

        if (MultipartRouteAuthorization is not null) {
            options.MultipartRouteAuthorization = MultipartRouteAuthorization.Clone();
        }

        if (AdminRouteAuthorization is not null) {
            options.AdminRouteAuthorization = AdminRouteAuthorization.Clone();
        }
    }
}

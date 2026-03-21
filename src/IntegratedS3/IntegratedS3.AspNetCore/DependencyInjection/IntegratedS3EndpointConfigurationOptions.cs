using IntegratedS3.AspNetCore;

namespace IntegratedS3.AspNetCore.DependencyInjection;

/// <summary>
/// Configuration-bindable (AOT/trimming-safe) endpoint options for IntegratedS3.
/// This is the <see cref="Microsoft.Extensions.Configuration.IConfiguration"/>-friendly counterpart
/// to <see cref="IntegratedS3EndpointOptions"/>.
/// </summary>
/// <remarks>
/// Nullable properties allow partial configuration — only non-<see langword="null"/> values are applied
/// when merged into <see cref="IntegratedS3EndpointOptions"/>. Prefer this type when endpoint
/// configuration comes from <c>appsettings.json</c> or similar
/// <see cref="Microsoft.Extensions.Configuration.IConfiguration"/> sources, as it avoids code callbacks
/// that can break AOT compilation.
/// </remarks>
public sealed class IntegratedS3EndpointConfigurationOptions
{
    /// <summary>
    /// Enables or disables S3 service-level endpoints (ListBuckets, service discovery).
    /// <see langword="null"/> means not configured; defers to the default in <see cref="IntegratedS3EndpointOptions.EnableServiceEndpoints"/>.
    /// </summary>
    public bool? EnableServiceEndpoints { get; set; }

    /// <summary>
    /// Enables or disables bucket-level endpoints (CreateBucket, DeleteBucket, ListObjects, etc.).
    /// <see langword="null"/> means not configured; defers to the default in <see cref="IntegratedS3EndpointOptions.EnableBucketEndpoints"/>.
    /// </summary>
    public bool? EnableBucketEndpoints { get; set; }

    /// <summary>
    /// Enables or disables object-level endpoints (GetObject, PutObject, DeleteObject, HeadObject, CopyObject, etc.).
    /// <see langword="null"/> means not configured; defers to the default in <see cref="IntegratedS3EndpointOptions.EnableObjectEndpoints"/>.
    /// </summary>
    public bool? EnableObjectEndpoints { get; set; }

    /// <summary>
    /// Enables or disables multipart upload endpoints (CreateMultipartUpload, UploadPart, CompleteMultipartUpload, etc.).
    /// <see langword="null"/> means not configured; defers to the default in <see cref="IntegratedS3EndpointOptions.EnableMultipartEndpoints"/>.
    /// </summary>
    public bool? EnableMultipartEndpoints { get; set; }

    /// <summary>
    /// Enables or disables administrative endpoints (diagnostics, repair operations).
    /// <see langword="null"/> means not configured; defers to the default in <see cref="IntegratedS3EndpointOptions.EnableAdminEndpoints"/>.
    /// </summary>
    public bool? EnableAdminEndpoints { get; set; }

    /// <summary>
    /// Authorization options applied to the top-level route group. Affects all endpoints.
    /// <see langword="null"/> means not configured; defers to the value in <see cref="IntegratedS3EndpointOptions.RouteAuthorization"/>.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? RouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for root-level GET endpoints (ListBuckets, service info).
    /// <see langword="null"/> means not configured; defers to the value in <see cref="IntegratedS3EndpointOptions.RootRouteAuthorization"/>.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? RootRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for S3-compatibility endpoints that exist outside the standard route structure.
    /// <see langword="null"/> means not configured; defers to the value in <see cref="IntegratedS3EndpointOptions.CompatibilityRouteAuthorization"/>.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? CompatibilityRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for service-level endpoints.
    /// <see langword="null"/> means not configured; defers to the value in <see cref="IntegratedS3EndpointOptions.ServiceRouteAuthorization"/>.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? ServiceRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for bucket-level endpoints.
    /// <see langword="null"/> means not configured; defers to the value in <see cref="IntegratedS3EndpointOptions.BucketRouteAuthorization"/>.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? BucketRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for object-level endpoints.
    /// <see langword="null"/> means not configured; defers to the value in <see cref="IntegratedS3EndpointOptions.ObjectRouteAuthorization"/>.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? ObjectRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for multipart upload endpoints.
    /// <see langword="null"/> means not configured; defers to the value in <see cref="IntegratedS3EndpointOptions.MultipartRouteAuthorization"/>.
    /// </summary>
    public IntegratedS3EndpointAuthorizationOptions? MultipartRouteAuthorization { get; set; }

    /// <summary>
    /// Authorization options for administrative endpoints.
    /// <see langword="null"/> means not configured; defers to the value in <see cref="IntegratedS3EndpointOptions.AdminRouteAuthorization"/>.
    /// </summary>
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

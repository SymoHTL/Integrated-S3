namespace IntegratedS3.Client;

internal static class IntegratedS3ClientPathUtilities
{
    public static Uri NormalizeBaseAddress(Uri baseAddress)
    {
        ArgumentNullException.ThrowIfNull(baseAddress);

        if (!baseAddress.IsAbsoluteUri) {
            throw new ArgumentException("The IntegratedS3 client base address must be absolute.", nameof(baseAddress));
        }

        var builder = new UriBuilder(baseAddress);
        if (!builder.Path.EndsWith("/", StringComparison.Ordinal)) {
            builder.Path += "/";
        }

        return builder.Uri;
    }

    public static string NormalizeRoutePrefix(string? routePrefix)
    {
        if (string.IsNullOrWhiteSpace(routePrefix)) {
            return IntegratedS3ClientOptions.DefaultRoutePrefix;
        }

        return routePrefix.Trim().Trim('/');
    }
}

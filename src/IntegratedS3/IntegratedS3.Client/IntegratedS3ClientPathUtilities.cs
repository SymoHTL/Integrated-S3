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
        var trimmed = routePrefix?.Trim();
        if (string.IsNullOrWhiteSpace(trimmed)) {
            return IntegratedS3ClientOptions.DefaultRoutePrefix;
        }

        return trimmed.Trim('/');
    }
}

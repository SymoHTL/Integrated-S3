using IntegratedS3.AspNetCore.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace IntegratedS3.AspNetCore.DependencyInjection;

internal sealed class IntegratedS3OptionsValidator(IServiceProvider? serviceProvider = null) : IValidateOptions<IntegratedS3Options>
{
    public ValidateOptionsResult Validate(string? name, IntegratedS3Options options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();

        if (!string.IsNullOrEmpty(options.RoutePrefix) && !options.RoutePrefix.StartsWith('/'))
        {
            failures.Add(
                $"RoutePrefix must start with '/' when non-empty, but was '{options.RoutePrefix}'. " +
                "Set RoutePrefix to a path like '/integrated-s3' or leave it empty for root-level mapping.");
        }

        if (options.AllowedSignatureClockSkewMinutes <= 0)
        {
            failures.Add(
                $"AllowedSignatureClockSkewMinutes must be a positive integer, but was {options.AllowedSignatureClockSkewMinutes}. " +
                "Set a value like 5 (minutes) to allow reasonable clock drift for signature verification.");
        }

        if (options.MaximumPresignedUrlExpirySeconds <= 0)
        {
            failures.Add(
                $"MaximumPresignedUrlExpirySeconds must be a positive integer, but was {options.MaximumPresignedUrlExpirySeconds}. " +
                "Set a value like 3600 (1 hour) to control how long presigned URLs remain valid.");
        }

        if (options.MaxObjectSizeBytes is <= 0)
        {
            failures.Add(
                $"MaxObjectSizeBytes must be a positive number of bytes when set, but was {options.MaxObjectSizeBytes}. " +
                "Set a value like 5368709120 (5 GiB) to cap object uploads, or leave it null to accept uploads without a size limit.");
        }

        if (options.EnableAwsSignatureV4Authentication
            && UsesConfiguredCredentialResolver()
            && (options.AccessKeyCredentials is null || options.AccessKeyCredentials.Count == 0
                || !options.AccessKeyCredentials.Exists(static c =>
                    !string.IsNullOrWhiteSpace(c.AccessKeyId) && !string.IsNullOrWhiteSpace(c.SecretAccessKey))))
        {
            failures.Add(
                "EnableAwsSignatureV4Authentication is true but no valid AccessKeyCredentials are configured. " +
                "Add at least one entry with a non-empty AccessKeyId and SecretAccessKey to the AccessKeyCredentials list, " +
                "or register a custom IIntegratedS3CredentialResolver to resolve credentials from another source.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }

    /// <summary>
    /// Determines whether SigV4 credentials come from <see cref="IntegratedS3Options.AccessKeyCredentials"/>.
    /// The AccessKeyCredentials requirement only applies to the built-in options-backed resolver; a custom
    /// <see cref="IIntegratedS3CredentialResolver"/> may source credentials elsewhere (for example a database).
    /// The resolver is looked up lazily here rather than injected because the options pipeline that
    /// constructs this validator is itself a dependency of the default resolver.
    /// </summary>
    private bool UsesConfiguredCredentialResolver()
    {
        if (serviceProvider is null) {
            return true;
        }

        var credentialResolver = serviceProvider.GetService<IIntegratedS3CredentialResolver>();
        return credentialResolver is null or ConfiguredIntegratedS3CredentialResolver;
    }
}

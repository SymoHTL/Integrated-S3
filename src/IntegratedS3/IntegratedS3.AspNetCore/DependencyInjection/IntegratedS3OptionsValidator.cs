using Microsoft.Extensions.Options;

namespace IntegratedS3.AspNetCore.DependencyInjection;

internal sealed class IntegratedS3OptionsValidator : IValidateOptions<IntegratedS3Options>
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

        if (options.EnableAwsSignatureV4Authentication
            && (options.AccessKeyCredentials is null || options.AccessKeyCredentials.Count == 0
                || !options.AccessKeyCredentials.Exists(static c =>
                    !string.IsNullOrWhiteSpace(c.AccessKeyId) && !string.IsNullOrWhiteSpace(c.SecretAccessKey))))
        {
            failures.Add(
                "EnableAwsSignatureV4Authentication is true but no valid AccessKeyCredentials are configured. " +
                "Add at least one entry with a non-empty AccessKeyId and SecretAccessKey to the AccessKeyCredentials list.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}

using Microsoft.Extensions.Options;

namespace IntegratedS3.AspNetCore.Services;

/// <summary>
/// Default <see cref="IIntegratedS3CredentialResolver"/> that resolves credentials from
/// <see cref="IntegratedS3Options.AccessKeyCredentials"/>. Reading through
/// <see cref="IOptionsMonitor{TOptions}"/> means credential changes from reloadable
/// configuration sources are picked up on the next request without an application restart.
/// </summary>
internal sealed class ConfiguredIntegratedS3CredentialResolver(IOptionsMonitor<IntegratedS3Options> options) : IIntegratedS3CredentialResolver
{
    public ValueTask<IntegratedS3AccessKeyCredential?> ResolveAsync(string accessKeyId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(accessKeyId);
        cancellationToken.ThrowIfCancellationRequested();

        var credential = options.CurrentValue.AccessKeyCredentials.FirstOrDefault(candidate =>
            string.Equals(candidate.AccessKeyId, accessKeyId, StringComparison.Ordinal));

        return ValueTask.FromResult(credential);
    }
}

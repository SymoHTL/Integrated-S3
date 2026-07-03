namespace IntegratedS3.AspNetCore;

/// <summary>
/// Resolves the access key credential used to verify an incoming AWS Signature V4/V4a request.
/// The default implementation resolves from <see cref="IntegratedS3Options.AccessKeyCredentials"/>
/// through <c>IOptionsMonitor&lt;IntegratedS3Options&gt;</c>, so keys added, rotated, or revoked via a
/// reloadable configuration source take effect without an application restart.
/// Register a custom implementation to back credentials with a database, secret manager,
/// or any other dynamic source.
/// </summary>
public interface IIntegratedS3CredentialResolver
{
    /// <summary>
    /// Resolves the credential for the given access key id.
    /// </summary>
    /// <param name="accessKeyId">The access key id presented by the request's credential scope.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// The matching <see cref="IntegratedS3AccessKeyCredential"/>, or <see langword="null"/> when the
    /// access key id is unknown or revoked (the request is then rejected with <c>InvalidAccessKeyId</c>).
    /// </returns>
    ValueTask<IntegratedS3AccessKeyCredential?> ResolveAsync(string accessKeyId, CancellationToken cancellationToken = default);
}

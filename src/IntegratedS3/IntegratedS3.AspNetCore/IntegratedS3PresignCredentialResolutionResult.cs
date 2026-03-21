namespace IntegratedS3.AspNetCore;

/// <summary>
/// Result of a presign credential resolution attempt.
/// Use the <see cref="Success"/> and <see cref="Failure"/> factory methods to create instances.
/// </summary>
public sealed class IntegratedS3PresignCredentialResolutionResult
{
    private IntegratedS3PresignCredentialResolutionResult(
        bool succeeded,
        IntegratedS3AccessKeyCredential? credential,
        string? errorMessage)
    {
        Succeeded = succeeded;
        Credential = credential;
        ErrorMessage = errorMessage;
    }

    /// <summary>
    /// Gets a value indicating whether a credential was successfully resolved.
    /// </summary>
    public bool Succeeded { get; }

    /// <summary>
    /// Gets the resolved credential, or <see langword="null"/> when resolution failed.
    /// </summary>
    public IntegratedS3AccessKeyCredential? Credential { get; }

    /// <summary>
    /// Gets the error message when resolution fails, or <see langword="null"/> on success.
    /// </summary>
    public string? ErrorMessage { get; }

    /// <summary>
    /// Creates a successful result with the given credential.
    /// </summary>
    /// <param name="credential">The resolved <see cref="IntegratedS3AccessKeyCredential"/>.</param>
    /// <returns>A successful <see cref="IntegratedS3PresignCredentialResolutionResult"/>.</returns>
    public static IntegratedS3PresignCredentialResolutionResult Success(IntegratedS3AccessKeyCredential credential)
    {
        ArgumentNullException.ThrowIfNull(credential);
        return new IntegratedS3PresignCredentialResolutionResult(true, credential, errorMessage: null);
    }

    /// <summary>
    /// Creates a failed result with the given error message.
    /// </summary>
    /// <param name="errorMessage">A message describing why credential resolution failed.</param>
    /// <returns>A failed <see cref="IntegratedS3PresignCredentialResolutionResult"/>.</returns>
    public static IntegratedS3PresignCredentialResolutionResult Failure(string errorMessage)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorMessage);
        return new IntegratedS3PresignCredentialResolutionResult(false, credential: null, errorMessage);
    }
}

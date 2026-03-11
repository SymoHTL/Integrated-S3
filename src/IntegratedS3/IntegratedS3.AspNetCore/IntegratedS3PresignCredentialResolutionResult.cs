namespace IntegratedS3.AspNetCore;

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

    public bool Succeeded { get; }

    public IntegratedS3AccessKeyCredential? Credential { get; }

    public string? ErrorMessage { get; }

    public static IntegratedS3PresignCredentialResolutionResult Success(IntegratedS3AccessKeyCredential credential)
    {
        ArgumentNullException.ThrowIfNull(credential);
        return new IntegratedS3PresignCredentialResolutionResult(true, credential, errorMessage: null);
    }

    public static IntegratedS3PresignCredentialResolutionResult Failure(string errorMessage)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorMessage);
        return new IntegratedS3PresignCredentialResolutionResult(false, credential: null, errorMessage);
    }
}

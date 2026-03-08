using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Core.Services;

public sealed class StorageAuthorizationException : Exception
{
    public StorageAuthorizationException(StorageError error)
        : base(error?.Message)
    {
        Error = error ?? throw new ArgumentNullException(nameof(error));
    }

    public StorageError Error { get; }
}
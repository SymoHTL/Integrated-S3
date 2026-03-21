using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Exception thrown when a storage operation is denied by the authorization layer.
/// </summary>
public sealed class StorageAuthorizationException : Exception
{
    /// <summary>
    /// Initializes a new instance of <see cref="StorageAuthorizationException"/> with the specified authorization error.
    /// </summary>
    /// <param name="error">The <see cref="StorageError"/> describing the authorization failure.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="error"/> is <see langword="null"/>.</exception>
    public StorageAuthorizationException(StorageError error)
        : base(error?.Message)
    {
        Error = error ?? throw new ArgumentNullException(nameof(error));
    }

    /// <summary>
    /// Gets the <see cref="StorageError"/> that describes the authorization failure.
    /// </summary>
    public StorageError Error { get; }
}
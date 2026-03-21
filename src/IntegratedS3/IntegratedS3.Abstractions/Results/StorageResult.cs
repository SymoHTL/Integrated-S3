using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Abstractions.Results;

/// <summary>
/// Represents the outcome of a storage operation that produces no value on success.
/// </summary>
public class StorageResult
{
    /// <summary>
    /// Initializes a new instance of <see cref="StorageResult"/>.
    /// </summary>
    protected StorageResult(bool isSuccess, StorageError? error)
    {
        IsSuccess = isSuccess;
        Error = error;
    }

    /// <summary>
    /// Gets a value indicating whether the storage operation succeeded.
    /// </summary>
    public bool IsSuccess { get; }

    /// <summary>
    /// Gets the error details when <see cref="IsSuccess"/> is <see langword="false"/>; otherwise <see langword="null"/>.
    /// </summary>
    public StorageError? Error { get; }

    /// <summary>
    /// Creates a successful <see cref="StorageResult"/> with no value.
    /// </summary>
    /// <returns>A <see cref="StorageResult"/> representing a successful operation.</returns>
    public static StorageResult Success()
    {
        return new StorageResult(true, null);
    }

    /// <summary>
    /// Creates a failed <see cref="StorageResult"/> from the specified error.
    /// </summary>
    /// <param name="error">The <see cref="StorageError"/> describing the failure.</param>
    /// <returns>A <see cref="StorageResult"/> representing a failed operation.</returns>
    public static StorageResult Failure(StorageError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new StorageResult(false, error);
    }
}

/// <summary>
/// Represents the outcome of a storage operation that carries a <typeparamref name="T"/> value on success.
/// </summary>
/// <typeparam name="T">The type of the result value.</typeparam>
public sealed class StorageResult<T> : StorageResult
{
    private StorageResult(bool isSuccess, T? value, StorageError? error)
        : base(isSuccess, error)
    {
        Value = value;
    }

    /// <summary>
    /// Gets the result value. Non-null when <see cref="StorageResult.IsSuccess"/> is <see langword="true"/>; otherwise the default of <typeparamref name="T"/>.
    /// </summary>
    public T? Value { get; }

    /// <summary>
    /// Creates a successful <see cref="StorageResult{T}"/> with the given value.
    /// </summary>
    /// <param name="value">The result value.</param>
    /// <returns>A <see cref="StorageResult{T}"/> representing a successful operation.</returns>
    public static StorageResult<T> Success(T value)
    {
        return new StorageResult<T>(true, value, null);
    }

    /// <summary>
    /// Creates a failed <see cref="StorageResult{T}"/> from the specified error.
    /// </summary>
    /// <param name="error">The <see cref="StorageError"/> describing the failure.</param>
    /// <returns>A <see cref="StorageResult{T}"/> representing a failed operation.</returns>
    public static new StorageResult<T> Failure(StorageError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new StorageResult<T>(false, default, error);
    }
}

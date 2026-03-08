using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Abstractions.Results;

public class StorageResult
{
    protected StorageResult(bool isSuccess, StorageError? error)
    {
        IsSuccess = isSuccess;
        Error = error;
    }

    public bool IsSuccess { get; }

    public StorageError? Error { get; }

    public static StorageResult Success()
    {
        return new StorageResult(true, null);
    }

    public static StorageResult Failure(StorageError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new StorageResult(false, error);
    }
}

public sealed class StorageResult<T> : StorageResult
{
    private StorageResult(bool isSuccess, T? value, StorageError? error)
        : base(isSuccess, error)
    {
        Value = value;
    }

    public T? Value { get; }

    public static StorageResult<T> Success(T value)
    {
        return new StorageResult<T>(true, value, null);
    }

    public static new StorageResult<T> Failure(StorageError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new StorageResult<T>(false, default, error);
    }
}

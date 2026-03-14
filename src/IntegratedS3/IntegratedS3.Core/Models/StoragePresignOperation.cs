namespace IntegratedS3.Core.Models;

/// <summary>
/// Identifies which object operation should be presigned.
/// </summary>
public enum StoragePresignOperation
{
    /// <summary>Presign an object download.</summary>
    GetObject,

    /// <summary>Presign an object upload.</summary>
    PutObject
}

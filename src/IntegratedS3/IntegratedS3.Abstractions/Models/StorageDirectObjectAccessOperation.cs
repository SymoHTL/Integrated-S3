namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Operations that support presigned (direct) object access.
/// </summary>
public enum StorageDirectObjectAccessOperation
{
    /// <summary>
    /// Presigned URL for downloading an object.
    /// </summary>
    GetObject,

    /// <summary>
    /// Presigned URL for uploading an object.
    /// </summary>
    PutObject
}

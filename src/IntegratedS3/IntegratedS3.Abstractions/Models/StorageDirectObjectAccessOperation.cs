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
    PutObject,

    /// <summary>
    /// Presigned URL for deleting an object.
    /// </summary>
    DeleteObject,

    /// <summary>
    /// Presigned URL for reading object metadata without downloading the body.
    /// </summary>
    HeadObject,

    /// <summary>
    /// Presigned URL for uploading a single part of a multipart upload.
    /// </summary>
    UploadPart
}

namespace IntegratedS3.Core.Models;

/// <summary>
/// Identifies the type of presign operation to generate a URL for.
/// </summary>
public enum StoragePresignOperation
{
    /// <summary>Generate a presigned URL for downloading (GET) an object.</summary>
    GetObject,

    /// <summary>Generate a presigned URL for uploading (PUT) an object.</summary>
    PutObject
}

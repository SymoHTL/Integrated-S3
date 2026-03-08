namespace IntegratedS3.Core.Models;

public enum StorageOperationType
{
    ListBuckets,
    CreateBucket,
    HeadBucket,
    DeleteBucket,
    ListObjects,
    GetObject,
    CopyObject,
    PutObject,
    InitiateMultipartUpload,
    UploadMultipartPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    HeadObject,
    DeleteObject
}
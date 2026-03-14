namespace IntegratedS3.Core.Models;

public enum StorageOperationType
{
    ListBuckets,
    CreateBucket,
    GetBucketLocation,
    GetBucketVersioning,
    PutBucketVersioning,
    GetBucketCors,
    PutBucketCors,
    DeleteBucketCors,
    HeadBucket,
    DeleteBucket,
    ListObjects,
    GetObject,
    PresignGetObject,
    GetObjectTags,
    CopyObject,
    PutObject,
    PresignPutObject,
    PutObjectTags,
    DeleteObjectTags,
    InitiateMultipartUpload,
    UploadMultipartPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    HeadObject,
    DeleteObject
}

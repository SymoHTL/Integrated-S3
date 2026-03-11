using System.Text.Json.Serialization;

namespace IntegratedS3.Provider.Disk.Internal;

[JsonSerializable(typeof(DiskObjectMetadata))]
[JsonSerializable(typeof(DiskBucketMetadata))]
[JsonSerializable(typeof(DiskBucketCorsConfiguration))]
[JsonSerializable(typeof(DiskBucketCorsRule))]
[JsonSerializable(typeof(DiskMultipartUploadState))]
internal partial class DiskStorageJsonSerializerContext : JsonSerializerContext
{
}

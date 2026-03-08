using System.Text.Json.Serialization;

namespace IntegratedS3.Provider.Disk.Internal;

[JsonSerializable(typeof(DiskObjectMetadata))]
[JsonSerializable(typeof(DiskMultipartUploadState))]
internal partial class DiskStorageJsonSerializerContext : JsonSerializerContext
{
}

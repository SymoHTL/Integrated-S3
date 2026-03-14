using System.Text.Json;
using System.Text.Json.Serialization;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(StoragePresignRequest))]
[JsonSerializable(typeof(StoragePresignedHeader))]
[JsonSerializable(typeof(StoragePresignedHeader[]))]
[JsonSerializable(typeof(StoragePresignedRequest))]
internal partial class IntegratedS3ClientJsonSerializerContext : JsonSerializerContext
{
}

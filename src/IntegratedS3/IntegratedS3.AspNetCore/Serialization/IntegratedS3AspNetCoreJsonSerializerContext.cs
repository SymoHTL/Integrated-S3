using System.Text.Json.Serialization;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.AspNetCore.Endpoints;
using Microsoft.AspNetCore.Mvc;

namespace IntegratedS3.AspNetCore.Serialization;

[JsonSerializable(typeof(ProblemDetails))]
[JsonSerializable(typeof(BucketInfo))]
[JsonSerializable(typeof(BucketInfo[]))]
[JsonSerializable(typeof(ObjectInfo))]
[JsonSerializable(typeof(ObjectInfo[]))]
[JsonSerializable(typeof(StorageCapabilities))]
[JsonSerializable(typeof(StorageServiceDocument))]
[JsonSerializable(typeof(StorageProviderDocument))]
[JsonSerializable(typeof(StorageProviderDocument[]))]
internal partial class IntegratedS3AspNetCoreJsonSerializerContext : JsonSerializerContext
{
}

using System.Text.Json.Serialization;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;
using Microsoft.AspNetCore.Mvc;

namespace IntegratedS3.AspNetCore.Serialization;

[JsonSerializable(typeof(ProblemDetails))]
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(BucketInfo))]
[JsonSerializable(typeof(BucketInfo[]))]
[JsonSerializable(typeof(ObjectInfo))]
[JsonSerializable(typeof(ObjectInfo[]))]
[JsonSerializable(typeof(ObjectServerSideEncryptionInfo))]
[JsonSerializable(typeof(StorageCapabilities))]
[JsonSerializable(typeof(StorageObjectLocationDescriptor))]
[JsonSerializable(typeof(StorageSupportStateDescriptor))]
[JsonSerializable(typeof(StorageServiceDocument))]
[JsonSerializable(typeof(StorageProviderDocument))]
[JsonSerializable(typeof(StorageProviderDocument[]))]
[JsonSerializable(typeof(StoragePresignRequest))]
[JsonSerializable(typeof(StoragePresignedHeader))]
[JsonSerializable(typeof(StoragePresignedHeader[]))]
[JsonSerializable(typeof(StoragePresignedRequest))]
[JsonSerializable(typeof(StorageAdminDiagnostics))]
[JsonSerializable(typeof(StorageAdminProviderDiagnostics))]
[JsonSerializable(typeof(StorageAdminProviderDiagnostics[]))]
[JsonSerializable(typeof(StorageAdminReplicaLagDiagnostics))]
[JsonSerializable(typeof(StorageAdminRepairDiagnostics))]
[JsonSerializable(typeof(StorageReplicaRepairEntry))]
[JsonSerializable(typeof(StorageReplicaRepairEntry[]))]
internal partial class IntegratedS3AspNetCoreJsonSerializerContext : JsonSerializerContext
{
}

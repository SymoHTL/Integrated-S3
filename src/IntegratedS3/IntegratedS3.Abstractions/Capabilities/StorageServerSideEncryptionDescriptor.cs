using System.Text.Json.Serialization;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Capabilities;

public sealed class StorageServerSideEncryptionDescriptor
{
    public IReadOnlyList<StorageServerSideEncryptionVariantDescriptor> Variants { get; set; } = [];
}

public sealed class StorageServerSideEncryptionVariantDescriptor
{
    public ObjectServerSideEncryptionAlgorithm Algorithm { get; set; }

    public StorageServerSideEncryptionRequestStyle RequestStyle { get; set; } = StorageServerSideEncryptionRequestStyle.Managed;

    public IReadOnlyList<StorageServerSideEncryptionRequestOperation> SupportedRequestOperations { get; set; } = [];

    public bool SupportsResponseMetadata { get; set; }

    public bool SupportsKeyId { get; set; }

    public bool SupportsContext { get; set; }
}

[JsonConverter(typeof(JsonStringEnumConverter<StorageServerSideEncryptionRequestStyle>))]
public enum StorageServerSideEncryptionRequestStyle
{
    Managed,
    CustomerProvidedKey
}

[JsonConverter(typeof(JsonStringEnumConverter<StorageServerSideEncryptionRequestOperation>))]
public enum StorageServerSideEncryptionRequestOperation
{
    PutObject,
    GetObject,
    HeadObject,
    CopyDestination,
    CopySource,
    InitiateMultipartUpload
}

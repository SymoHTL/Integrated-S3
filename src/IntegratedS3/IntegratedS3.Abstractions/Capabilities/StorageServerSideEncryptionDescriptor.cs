using System.Text.Json.Serialization;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Capabilities;

/// <summary>
/// Describes the server-side encryption variants supported by a storage provider.
/// </summary>
public sealed class StorageServerSideEncryptionDescriptor
{
    /// <summary>The list of supported SSE algorithm/style combinations.</summary>
    public IReadOnlyList<StorageServerSideEncryptionVariantDescriptor> Variants { get; set; } = [];
}

/// <summary>
/// Describes a single server-side encryption variant, including its algorithm, request style, and supported operations.
/// </summary>
public sealed class StorageServerSideEncryptionVariantDescriptor
{
    /// <summary>The encryption algorithm used by this variant.</summary>
    public ObjectServerSideEncryptionAlgorithm Algorithm { get; set; }

    /// <summary>The request style indicating how encryption keys are provided.</summary>
    public StorageServerSideEncryptionRequestStyle RequestStyle { get; set; } = StorageServerSideEncryptionRequestStyle.Managed;

    /// <summary>The S3 operations that accept SSE parameters for this variant.</summary>
    public IReadOnlyList<StorageServerSideEncryptionRequestOperation> SupportedRequestOperations { get; set; } = [];

    /// <summary>Whether the provider returns encryption metadata in responses.</summary>
    public bool SupportsResponseMetadata { get; set; }

    /// <summary>Whether the variant supports specifying an encryption key identifier.</summary>
    public bool SupportsKeyId { get; set; }

    /// <summary>Whether the variant supports an encryption context parameter.</summary>
    public bool SupportsContext { get; set; }
}

/// <summary>
/// Indicates how server-side encryption keys are managed for a request.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter<StorageServerSideEncryptionRequestStyle>))]
public enum StorageServerSideEncryptionRequestStyle
{
    /// <summary>The provider manages encryption keys.</summary>
    Managed,

    /// <summary>The caller supplies the encryption key (SSE-C).</summary>
    CustomerProvidedKey
}

/// <summary>
/// Identifies an S3 operation that accepts server-side encryption parameters.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter<StorageServerSideEncryptionRequestOperation>))]
public enum StorageServerSideEncryptionRequestOperation
{
    /// <summary>PutObject operation.</summary>
    PutObject,

    /// <summary>GetObject operation.</summary>
    GetObject,

    /// <summary>HeadObject operation.</summary>
    HeadObject,

    /// <summary>The destination side of a CopyObject operation.</summary>
    CopyDestination,

    /// <summary>The source side of a CopyObject operation.</summary>
    CopySource,

    /// <summary>InitiateMultipartUpload operation.</summary>
    InitiateMultipartUpload
}

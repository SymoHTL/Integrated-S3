using System.Text.Json.Serialization;

namespace IntegratedS3.Abstractions.Models;

public sealed class ObjectServerSideEncryptionSettings
{
    public required ObjectServerSideEncryptionAlgorithm Algorithm { get; init; }

    public string? KeyId { get; init; }

    public IReadOnlyDictionary<string, string>? Context { get; init; }
}

public sealed class ObjectServerSideEncryptionInfo
{
    public required ObjectServerSideEncryptionAlgorithm Algorithm { get; init; }

    public string? KeyId { get; init; }
}

[JsonConverter(typeof(JsonStringEnumConverter<ObjectServerSideEncryptionAlgorithm>))]
public enum ObjectServerSideEncryptionAlgorithm
{
    Aes256,
    Kms,
    KmsDsse
}

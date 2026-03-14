using System.Buffers;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

internal sealed class S3ServerSideEncryptionNotSupportedException(string message) : NotSupportedException(message);

internal static class S3ServerSideEncryptionMapper
{
    public static void ApplyTo(PutObjectRequest request, ObjectServerSideEncryptionSettings? settings)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (settings is null)
            return;

        ApplyToCore(
            settings,
            method => request.ServerSideEncryptionMethod = method,
            keyId => request.ServerSideEncryptionKeyManagementServiceKeyId = keyId,
            context => request.ServerSideEncryptionKeyManagementServiceEncryptionContext = context);
    }

    public static void ApplyTo(CopyObjectRequest request, ObjectServerSideEncryptionSettings? settings)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (settings is null)
            return;

        ApplyToCore(
            settings,
            method => request.ServerSideEncryptionMethod = method,
            keyId => request.ServerSideEncryptionKeyManagementServiceKeyId = keyId,
            context => request.ServerSideEncryptionKeyManagementServiceEncryptionContext = context);
    }

    public static void ApplyTo(InitiateMultipartUploadRequest request, ObjectServerSideEncryptionSettings? settings)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (settings is null)
            return;

        ApplyToCore(
            settings,
            method => request.ServerSideEncryptionMethod = method,
            keyId => request.ServerSideEncryptionKeyManagementServiceKeyId = keyId,
            context => request.ServerSideEncryptionKeyManagementServiceEncryptionContext = context);
    }

    public static ObjectServerSideEncryptionInfo? ToInfo(ServerSideEncryptionMethod? method, string? keyId)
    {
        var algorithm = NormalizeAlgorithm(method);
        if (algorithm is null)
            return null;

        if (algorithm == ObjectServerSideEncryptionAlgorithm.Aes256
            && !string.IsNullOrWhiteSpace(keyId))
        {
            throw new S3ServerSideEncryptionNotSupportedException(
                "S3 object metadata reported an unexpected KMS key identifier for AES256 server-side encryption.");
        }

        return new ObjectServerSideEncryptionInfo
        {
            Algorithm = algorithm.Value,
            KeyId = SupportsKmsKeyId(algorithm.Value) && !string.IsNullOrWhiteSpace(keyId)
                ? keyId
                : null
        };
    }

    private static void ApplyToCore(
        ObjectServerSideEncryptionSettings settings,
        Action<ServerSideEncryptionMethod> setMethod,
        Action<string?> setKeyId,
        Action<string?> setContext)
    {
        switch (settings.Algorithm)
        {
            case ObjectServerSideEncryptionAlgorithm.Aes256:
                if (settings.KeyId is not null || settings.Context is not null)
                {
                    throw new S3ServerSideEncryptionNotSupportedException(
                        "AES256 server-side encryption does not support key identifiers or encryption context in the native S3 provider.");
                }

                setMethod(ServerSideEncryptionMethod.AES256);
                break;

            case ObjectServerSideEncryptionAlgorithm.Kms:
            case ObjectServerSideEncryptionAlgorithm.KmsDsse:
                setMethod(settings.Algorithm == ObjectServerSideEncryptionAlgorithm.Kms
                    ? ServerSideEncryptionMethod.AWSKMS
                    : ServerSideEncryptionMethod.AWSKMSDSSE);

                if (!string.IsNullOrWhiteSpace(settings.KeyId))
                    setKeyId(settings.KeyId);

                if (settings.Context is not null)
                    setContext(EncodeKmsEncryptionContext(settings.Context));

                break;

            default:
                throw new S3ServerSideEncryptionNotSupportedException(
                    $"Server-side encryption algorithm '{settings.Algorithm}' is not supported by the native S3 provider.");
        }
    }

    private static string EncodeKmsEncryptionContext(IReadOnlyDictionary<string, string> context)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer);

        writer.WriteStartObject();

        foreach (var (key, value) in context)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(key);
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            writer.WriteString(key, value);
        }

        writer.WriteEndObject();
        writer.Flush();

        return Convert.ToBase64String(buffer.WrittenSpan);
    }

    private static ObjectServerSideEncryptionAlgorithm? NormalizeAlgorithm(ServerSideEncryptionMethod? method)
    {
        var value = method?.Value;
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return value switch
        {
            "AES256" => ObjectServerSideEncryptionAlgorithm.Aes256,
            "aws:kms" => ObjectServerSideEncryptionAlgorithm.Kms,
            "aws:kms:dsse" => ObjectServerSideEncryptionAlgorithm.KmsDsse,
            _ => throw new S3ServerSideEncryptionNotSupportedException(
                $"S3 object metadata reported unsupported server-side encryption algorithm '{value}'.")
        };
    }

    private static bool SupportsKmsKeyId(ObjectServerSideEncryptionAlgorithm algorithm)
        => algorithm is ObjectServerSideEncryptionAlgorithm.Kms or ObjectServerSideEncryptionAlgorithm.KmsDsse;
}

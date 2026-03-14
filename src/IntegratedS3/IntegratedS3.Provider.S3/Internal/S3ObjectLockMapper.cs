using Amazon.S3;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

internal sealed class S3ObjectLockNotSupportedException(string message) : NotSupportedException(message);

internal static class S3ObjectLockMapper
{
    public static ObjectRetentionMode? ToRetentionMode(ObjectLockMode? mode)
        => NormalizeRetentionMode(mode?.Value);

    public static ObjectRetentionMode? ToRetentionMode(ObjectLockRetentionMode? mode)
        => NormalizeRetentionMode(mode?.Value);

    public static ObjectLegalHoldStatus? ToLegalHoldStatus(ObjectLockLegalHoldStatus? status)
    {
        var value = status?.Value;
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        return value switch
        {
            "ON" => ObjectLegalHoldStatus.On,
            "OFF" => ObjectLegalHoldStatus.Off,
            _ => throw new S3ObjectLockNotSupportedException(
                $"S3 object metadata reported unsupported object legal-hold status '{value}'.")
        };
    }

    private static ObjectRetentionMode? NormalizeRetentionMode(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        return value switch
        {
            "GOVERNANCE" => ObjectRetentionMode.Governance,
            "COMPLIANCE" => ObjectRetentionMode.Compliance,
            _ => throw new S3ObjectLockNotSupportedException(
                $"S3 object metadata reported unsupported object retention mode '{value}'.")
        };
    }
}

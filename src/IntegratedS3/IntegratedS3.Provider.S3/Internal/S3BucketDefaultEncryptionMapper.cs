using Amazon.S3;
using Amazon.S3.Model;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

internal static class S3BucketDefaultEncryptionMapper
{
    public static BucketDefaultEncryptionConfiguration ToBucketDefaultEncryptionConfiguration(
        string bucketName,
        ServerSideEncryptionConfiguration? configuration)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        var rules = configuration?.ServerSideEncryptionRules;
        if (rules is null || rules.Count == 0) {
            throw new S3ServerSideEncryptionNotSupportedException(
                "S3 bucket default encryption metadata did not include any server-side encryption rules.");
        }

        if (rules.Count != 1) {
            throw new S3ServerSideEncryptionNotSupportedException(
                "S3 bucket default encryption metadata reported multiple server-side encryption rules, which are not currently supported.");
        }

        var rule = rules[0];
        if (rule.ServerSideEncryptionByDefault is null) {
            throw new S3ServerSideEncryptionNotSupportedException(
                "S3 bucket default encryption metadata did not include an ApplyServerSideEncryptionByDefault configuration.");
        }

        var serverSideEncryption = S3ServerSideEncryptionMapper.ToInfo(
            rule.ServerSideEncryptionByDefault.ServerSideEncryptionAlgorithm,
            rule.ServerSideEncryptionByDefault.ServerSideEncryptionKeyManagementServiceKeyId)
            ?? throw new S3ServerSideEncryptionNotSupportedException(
                "S3 bucket default encryption metadata did not include a supported server-side encryption algorithm.");

        return new BucketDefaultEncryptionConfiguration
        {
            BucketName = bucketName,
            Rule = new BucketDefaultEncryptionRule
            {
                Algorithm = serverSideEncryption.Algorithm,
                KeyId = serverSideEncryption.KeyId,
                BucketKeyEnabled = rule.BucketKeyEnabled == true
            }
        };
    }

    public static ServerSideEncryptionConfiguration ToServerSideEncryptionConfiguration(BucketDefaultEncryptionRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);

        var keyId = string.IsNullOrWhiteSpace(rule.KeyId)
            ? null
            : rule.KeyId.Trim();

        if (rule.KeyId is not null && keyId is null) {
            throw new S3ServerSideEncryptionNotSupportedException(
                "KMS-managed bucket default encryption key identifiers must not be empty in the native S3 provider.");
        }

        return new ServerSideEncryptionConfiguration
        {
            ServerSideEncryptionRules =
            [
                new ServerSideEncryptionRule
                {
                    BucketKeyEnabled = rule.BucketKeyEnabled,
                    ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault
                    {
                        ServerSideEncryptionAlgorithm = ToServerSideEncryptionMethod(rule.Algorithm, keyId),
                        ServerSideEncryptionKeyManagementServiceKeyId = keyId
                    }
                }
            ]
        };
    }

    private static ServerSideEncryptionMethod ToServerSideEncryptionMethod(
        ObjectServerSideEncryptionAlgorithm algorithm,
        string? keyId)
    {
        return algorithm switch
        {
            ObjectServerSideEncryptionAlgorithm.Aes256 when keyId is not null => throw new S3ServerSideEncryptionNotSupportedException(
                "AES256 bucket default encryption does not support KMS key identifiers in the native S3 provider."),
            ObjectServerSideEncryptionAlgorithm.Aes256 => ServerSideEncryptionMethod.AES256,
            ObjectServerSideEncryptionAlgorithm.Kms => ServerSideEncryptionMethod.AWSKMS,
            ObjectServerSideEncryptionAlgorithm.KmsDsse => ServerSideEncryptionMethod.AWSKMSDSSE,
            _ => throw new S3ServerSideEncryptionNotSupportedException(
                $"Bucket default encryption algorithm '{algorithm}' is not supported by the native S3 provider.")
        };
    }
}

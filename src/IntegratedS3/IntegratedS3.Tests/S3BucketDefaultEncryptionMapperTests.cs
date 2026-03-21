using Amazon.S3;
using Amazon.S3.Model;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Provider.S3.Internal;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class S3BucketDefaultEncryptionMapperTests
{
    [Fact]
    public void ToServerSideEncryptionConfiguration_MapsManagedKmsRules()
    {
        var configuration = S3BucketDefaultEncryptionMapper.ToServerSideEncryptionConfiguration(new BucketDefaultEncryptionRule
        {
            Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
            KeyId = "alias/test-key"
        });

        var rule = Assert.Single(configuration.ServerSideEncryptionRules);
        Assert.Equal(ServerSideEncryptionMethod.AWSKMSDSSE.Value, rule.ServerSideEncryptionByDefault.ServerSideEncryptionAlgorithm?.Value);
        Assert.Equal("alias/test-key", rule.ServerSideEncryptionByDefault.ServerSideEncryptionKeyManagementServiceKeyId);
        Assert.False(rule.BucketKeyEnabled);
    }

    [Fact]
    public void ToServerSideEncryptionConfiguration_ThrowsForAes256WithKeyId()
    {
        var exception = Assert.Throws<S3ServerSideEncryptionNotSupportedException>(() =>
            S3BucketDefaultEncryptionMapper.ToServerSideEncryptionConfiguration(new BucketDefaultEncryptionRule
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256,
                KeyId = "alias/invalid"
            }));

        Assert.Contains("AES256", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ToBucketDefaultEncryptionConfiguration_MapsSupportedAwsAlgorithms()
    {
        var configuration = new ServerSideEncryptionConfiguration
        {
            ServerSideEncryptionRules =
            [
                new ServerSideEncryptionRule
                {
                    ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault
                    {
                        ServerSideEncryptionAlgorithm = ServerSideEncryptionMethod.AWSKMS,
                        ServerSideEncryptionKeyManagementServiceKeyId = "alias/default-key"
                    }
                }
            ]
        };

        var result = S3BucketDefaultEncryptionMapper.ToBucketDefaultEncryptionConfiguration("my-bucket", configuration);

        Assert.Equal("my-bucket", result.BucketName);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Kms, result.Rule.Algorithm);
        Assert.Equal("alias/default-key", result.Rule.KeyId);
    }

    [Fact]
    public void ToBucketDefaultEncryptionConfiguration_MapsBucketKeyEnabledTrue()
    {
        var configuration = new ServerSideEncryptionConfiguration
        {
            ServerSideEncryptionRules =
            [
                new ServerSideEncryptionRule
                {
                    BucketKeyEnabled = true,
                    ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault
                    {
                        ServerSideEncryptionAlgorithm = ServerSideEncryptionMethod.AES256
                    }
                }
            ]
        };

        var result = S3BucketDefaultEncryptionMapper.ToBucketDefaultEncryptionConfiguration("my-bucket", configuration);

        Assert.Equal("my-bucket", result.BucketName);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Aes256, result.Rule.Algorithm);
        Assert.True(result.Rule.BucketKeyEnabled);
    }
}

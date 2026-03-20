using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Provider.S3.Internal;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class S3ServerSideEncryptionMapperTests
{
    [Fact]
    public void ApplyTo_PutObjectRequest_MapsAes256WithoutKmsFields()
    {
        var request = new PutObjectRequest();

        S3ServerSideEncryptionMapper.ApplyTo(
            request,
            new ObjectServerSideEncryptionSettings
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256
            });

        Assert.Equal(ServerSideEncryptionMethod.AES256.Value, request.ServerSideEncryptionMethod?.Value);
        Assert.Null(request.ServerSideEncryptionKeyManagementServiceKeyId);
        Assert.Null(request.ServerSideEncryptionKeyManagementServiceEncryptionContext);
    }

    [Theory]
    [InlineData(ObjectServerSideEncryptionAlgorithm.Kms, "aws:kms")]
    [InlineData(ObjectServerSideEncryptionAlgorithm.KmsDsse, "aws:kms:dsse")]
    public void ApplyTo_CopyAndInitiateMultipartRequests_MapsManagedKmsFieldsAndContext(
        ObjectServerSideEncryptionAlgorithm algorithm,
        string expectedMethod)
    {
        var settings = new ObjectServerSideEncryptionSettings
        {
            Algorithm = algorithm,
            KeyId = "kms-key-1",
            Context = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant"] = "alpha",
                ["scope"] = "tests"
            }
        };
        var copyRequest = new CopyObjectRequest();
        var multipartRequest = new InitiateMultipartUploadRequest();

        S3ServerSideEncryptionMapper.ApplyTo(copyRequest, settings);
        S3ServerSideEncryptionMapper.ApplyTo(multipartRequest, settings);

        Assert.Equal(expectedMethod, copyRequest.ServerSideEncryptionMethod?.Value);
        Assert.Equal("kms-key-1", copyRequest.ServerSideEncryptionKeyManagementServiceKeyId);
        Assert.Equal(expectedMethod, multipartRequest.ServerSideEncryptionMethod?.Value);
        Assert.Equal("kms-key-1", multipartRequest.ServerSideEncryptionKeyManagementServiceKeyId);
        AssertEncryptionContext(settings.Context!, copyRequest.ServerSideEncryptionKeyManagementServiceEncryptionContext);
        AssertEncryptionContext(settings.Context!, multipartRequest.ServerSideEncryptionKeyManagementServiceEncryptionContext);
    }

    [Fact]
    public void ToInfo_MapsSupportedAwsAlgorithms()
    {
        var aes256 = S3ServerSideEncryptionMapper.ToInfo(ServerSideEncryptionMethod.AES256, null);
        var kms = S3ServerSideEncryptionMapper.ToInfo(ServerSideEncryptionMethod.AWSKMS, "kms-key-1");
        var kmsDsse = S3ServerSideEncryptionMapper.ToInfo(ServerSideEncryptionMethod.AWSKMSDSSE, "kms-key-2");

        Assert.NotNull(aes256);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Aes256, aes256!.Algorithm);
        Assert.Null(aes256.KeyId);

        Assert.NotNull(kms);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Kms, kms!.Algorithm);
        Assert.Equal("kms-key-1", kms.KeyId);

        Assert.NotNull(kmsDsse);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.KmsDsse, kmsDsse!.Algorithm);
        Assert.Equal("kms-key-2", kmsDsse.KeyId);
    }

    [Fact]
    public void ToInfo_ThrowsForUnsupportedAwsAlgorithms()
    {
        var ex = Assert.Throws<S3ServerSideEncryptionNotSupportedException>(
            () => S3ServerSideEncryptionMapper.ToInfo(new ServerSideEncryptionMethod("aws:unsupported"), "kms-key-1"));

        Assert.Contains("aws:unsupported", ex.Message, StringComparison.Ordinal);
    }

    private static void AssertEncryptionContext(
        IReadOnlyDictionary<string, string> expected,
        string? encodedContext)
    {
        Assert.False(string.IsNullOrWhiteSpace(encodedContext));

        using var document = JsonDocument.Parse(Convert.FromBase64String(encodedContext!));
        Assert.Equal(expected.Count, document.RootElement.EnumerateObject().Count());

        foreach (var (key, value) in expected)
        {
            Assert.True(document.RootElement.TryGetProperty(key, out var property));
            Assert.Equal(value, property.GetString());
        }
    }

    // -----------------------------------------------------------------
    // SSE-C (customer-provided key) tests
    // -----------------------------------------------------------------

    private static ObjectCustomerEncryptionSettings CreateTestCustomerEncryption() => new()
    {
        Algorithm = "AES256",
        Key = Convert.ToBase64String(new byte[32]),
        KeyMd5 = Convert.ToBase64String(new byte[16])
    };

    [Fact]
    public void ApplyCustomerEncryption_PutObjectRequest_MapsAes256CustomerKey()
    {
        var request = new PutObjectRequest();
        var settings = CreateTestCustomerEncryption();

        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, settings);

        Assert.Equal(ServerSideEncryptionCustomerMethod.AES256, request.ServerSideEncryptionCustomerMethod);
        Assert.Equal(settings.Key, request.ServerSideEncryptionCustomerProvidedKey);
        Assert.Equal(settings.KeyMd5, request.ServerSideEncryptionCustomerProvidedKeyMD5);
    }

    [Fact]
    public void ApplyCustomerEncryption_GetObjectRequest_MapsCustomerKey()
    {
        var request = new Amazon.S3.Model.GetObjectRequest();
        var settings = CreateTestCustomerEncryption();

        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, settings);

        Assert.Equal(ServerSideEncryptionCustomerMethod.AES256, request.ServerSideEncryptionCustomerMethod);
        Assert.Equal(settings.Key, request.ServerSideEncryptionCustomerProvidedKey);
        Assert.Equal(settings.KeyMd5, request.ServerSideEncryptionCustomerProvidedKeyMD5);
    }

    [Fact]
    public void ApplyCustomerEncryption_HeadObjectRequest_MapsCustomerKey()
    {
        var request = new GetObjectMetadataRequest();
        var settings = CreateTestCustomerEncryption();

        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, settings);

        Assert.Equal(ServerSideEncryptionCustomerMethod.AES256, request.ServerSideEncryptionCustomerMethod);
        Assert.Equal(settings.Key, request.ServerSideEncryptionCustomerProvidedKey);
        Assert.Equal(settings.KeyMd5, request.ServerSideEncryptionCustomerProvidedKeyMD5);
    }

    [Fact]
    public void ApplyCustomerEncryption_InitiateMultipartUploadRequest_MapsCustomerKey()
    {
        var request = new InitiateMultipartUploadRequest();
        var settings = CreateTestCustomerEncryption();

        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, settings);

        Assert.Equal(ServerSideEncryptionCustomerMethod.AES256, request.ServerSideEncryptionCustomerMethod);
        Assert.Equal(settings.Key, request.ServerSideEncryptionCustomerProvidedKey);
        Assert.Equal(settings.KeyMd5, request.ServerSideEncryptionCustomerProvidedKeyMD5);
    }

    [Fact]
    public void ApplyCustomerEncryption_UploadPartRequest_MapsCustomerKey()
    {
        var request = new UploadPartRequest();
        var settings = CreateTestCustomerEncryption();

        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, settings);

        Assert.Equal(ServerSideEncryptionCustomerMethod.AES256, request.ServerSideEncryptionCustomerMethod);
        Assert.Equal(settings.Key, request.ServerSideEncryptionCustomerProvidedKey);
        Assert.Equal(settings.KeyMd5, request.ServerSideEncryptionCustomerProvidedKeyMD5);
    }

    [Fact]
    public void ApplyCustomerEncryption_CopyObjectRequest_MapsDestinationCustomerKey()
    {
        var request = new CopyObjectRequest();
        var settings = CreateTestCustomerEncryption();

        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, settings);

        Assert.Equal(ServerSideEncryptionCustomerMethod.AES256, request.ServerSideEncryptionCustomerMethod);
        Assert.Equal(settings.Key, request.ServerSideEncryptionCustomerProvidedKey);
        Assert.Equal(settings.KeyMd5, request.ServerSideEncryptionCustomerProvidedKeyMD5);
    }

    [Fact]
    public void ApplyCopySourceCustomerEncryption_CopyObjectRequest_MapsSourceCustomerKey()
    {
        var request = new CopyObjectRequest();
        var settings = CreateTestCustomerEncryption();

        S3ServerSideEncryptionMapper.ApplyCopySourceCustomerEncryption(request, settings);

        Assert.Equal(ServerSideEncryptionCustomerMethod.AES256, request.CopySourceServerSideEncryptionCustomerMethod);
        Assert.Equal(settings.Key, request.CopySourceServerSideEncryptionCustomerProvidedKey);
        Assert.Equal(settings.KeyMd5, request.CopySourceServerSideEncryptionCustomerProvidedKeyMD5);
    }

    [Fact]
    public void ApplyCopySourceCustomerEncryption_CopyPartRequest_MapsSourceCustomerKey()
    {
        var request = new CopyPartRequest();
        var settings = CreateTestCustomerEncryption();

        S3ServerSideEncryptionMapper.ApplyCopySourceCustomerEncryption(request, settings);

        Assert.Equal(ServerSideEncryptionCustomerMethod.AES256, request.CopySourceServerSideEncryptionCustomerMethod);
        Assert.Equal(settings.Key, request.CopySourceServerSideEncryptionCustomerProvidedKey);
        Assert.Equal(settings.KeyMd5, request.CopySourceServerSideEncryptionCustomerProvidedKeyMD5);
    }

    [Fact]
    public void ToCustomerEncryptionInfo_MapsAlgorithmAndKeyMd5()
    {
        var info = S3ServerSideEncryptionMapper.ToCustomerEncryptionInfo("AES256", "abc123==");

        Assert.NotNull(info);
        Assert.Equal("AES256", info!.Algorithm);
        Assert.Equal("abc123==", info.KeyMd5);
    }

    [Fact]
    public void ToCustomerEncryptionInfo_ReturnsNullWhenAlgorithmIsNull()
    {
        Assert.Null(S3ServerSideEncryptionMapper.ToCustomerEncryptionInfo(null, "abc123=="));
        Assert.Null(S3ServerSideEncryptionMapper.ToCustomerEncryptionInfo("", "abc123=="));
        Assert.Null(S3ServerSideEncryptionMapper.ToCustomerEncryptionInfo("  ", "abc123=="));
    }

    [Fact]
    public void ApplyCustomerEncryption_NullSettings_DoesNothing()
    {
        var putRequest = new PutObjectRequest();
        var getRequest = new Amazon.S3.Model.GetObjectRequest();
        var headRequest = new GetObjectMetadataRequest();

        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(putRequest, null);
        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(getRequest, null);
        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(headRequest, null);

        Assert.Null(putRequest.ServerSideEncryptionCustomerMethod);
        Assert.Null(putRequest.ServerSideEncryptionCustomerProvidedKey);
        Assert.Null(putRequest.ServerSideEncryptionCustomerProvidedKeyMD5);
        Assert.Null(getRequest.ServerSideEncryptionCustomerMethod);
        Assert.Null(getRequest.ServerSideEncryptionCustomerProvidedKey);
        Assert.Null(getRequest.ServerSideEncryptionCustomerProvidedKeyMD5);
        Assert.Null(headRequest.ServerSideEncryptionCustomerMethod);
        Assert.Null(headRequest.ServerSideEncryptionCustomerProvidedKey);
        Assert.Null(headRequest.ServerSideEncryptionCustomerProvidedKeyMD5);
    }
}

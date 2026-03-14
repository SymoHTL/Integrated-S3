using Amazon.Runtime;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Provider.S3;
using IntegratedS3.Provider.S3.DependencyInjection;
using IntegratedS3.Provider.S3.Internal;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Focused tests for runtime-aware capability reporting and credential/option
/// normalization introduced by the s3-provider-options-capabilities fix.
/// </summary>
public sealed class S3StorageOptionsCapabilitiesTests
{
    // ── Capability reporting ────────────────────────────────────────────────

    [Fact]
    public void CreateDefault_ForcePathStyleTrue_ReportsPathStyleNative_AndVirtualHostedUnsupported()
    {
        var options = new S3StorageOptions { ForcePathStyle = true };

        var caps = S3StorageCapabilities.CreateDefault(options);

        Assert.Equal(StorageCapabilitySupport.Native, caps.PathStyleAddressing);
        Assert.Equal(StorageCapabilitySupport.Unsupported, caps.VirtualHostedStyleAddressing);
        Assert.Equal(StorageCapabilitySupport.Native, caps.ObjectLock);
    }

    [Fact]
    public void CreateDefault_ForcePathStyleFalse_ReportsVirtualHostedNative_AndPathStyleUnsupported()
    {
        var options = new S3StorageOptions { ForcePathStyle = false };

        var caps = S3StorageCapabilities.CreateDefault(options);

        Assert.Equal(StorageCapabilitySupport.Unsupported, caps.PathStyleAddressing);
        Assert.Equal(StorageCapabilitySupport.Native, caps.VirtualHostedStyleAddressing);
    }

    [Fact]
    public void CreateDefault_BucketOperations_AlwaysNative_Regardless_OfPathStyle()
    {
        var capsFps = S3StorageCapabilities.CreateDefault(new S3StorageOptions { ForcePathStyle = true });
        var capsVhs = S3StorageCapabilities.CreateDefault(new S3StorageOptions { ForcePathStyle = false });

        Assert.Equal(StorageCapabilitySupport.Native, capsFps.BucketOperations);
        Assert.Equal(StorageCapabilitySupport.Native, capsVhs.BucketOperations);
    }

    [Fact]
    public void CreateDefault_ReportsManagedServerSideEncryptionVariants()
    {
        var caps = S3StorageCapabilities.CreateDefault(new S3StorageOptions { ForcePathStyle = true });

        Assert.Collection(
            caps.ServerSideEncryptionDetails.Variants,
            variant => AssertManagedVariant(variant, ObjectServerSideEncryptionAlgorithm.Aes256, supportsKeyId: false, supportsContext: false),
            variant => AssertManagedVariant(variant, ObjectServerSideEncryptionAlgorithm.Kms, supportsKeyId: true, supportsContext: true),
            variant => AssertManagedVariant(variant, ObjectServerSideEncryptionAlgorithm.KmsDsse, supportsKeyId: true, supportsContext: true));
    }

    [Fact]
    public async Task GetCapabilitiesAsync_ForcePathStyle_True_ReflectsInServiceOutput()
    {
        var options = new S3StorageOptions { ForcePathStyle = true };
        var svc = new S3StorageService(options, new FakeS3Client());

        var caps = await svc.GetCapabilitiesAsync();

        Assert.Equal(StorageCapabilitySupport.Native, caps.PathStyleAddressing);
        Assert.Equal(StorageCapabilitySupport.Unsupported, caps.VirtualHostedStyleAddressing);
        Assert.Equal(StorageCapabilitySupport.Native, caps.ObjectLock);
    }

    [Fact]
    public async Task GetCapabilitiesAsync_ForcePathStyle_False_ReflectsInServiceOutput()
    {
        var options = new S3StorageOptions { ForcePathStyle = false };
        var svc = new S3StorageService(options, new FakeS3Client());

        var caps = await svc.GetCapabilitiesAsync();

        Assert.Equal(StorageCapabilitySupport.Unsupported, caps.PathStyleAddressing);
        Assert.Equal(StorageCapabilitySupport.Native, caps.VirtualHostedStyleAddressing);
    }

    // ── Option normalization ────────────────────────────────────────────────

    [Theory]
    [InlineData("  mykey  ", "mykey")]
    [InlineData("mykey", "mykey")]
    [InlineData("  ", null)]
    [InlineData("", null)]
    public void Normalize_AccessKey_TrimsWhitespace_AndTreatsBlankAsNull(string raw, string? expected)
    {
        var normalized = NormalizeViaAddS3Storage(o => o.AccessKey = raw);
        Assert.Equal(expected, normalized.AccessKey);
    }

    [Theory]
    [InlineData("  mysecret  ", "mysecret")]
    [InlineData("mysecret", "mysecret")]
    [InlineData("  ", null)]
    [InlineData("", null)]
    public void Normalize_SecretKey_TrimsWhitespace_AndTreatsBlankAsNull(string raw, string? expected)
    {
        var normalized = NormalizeViaAddS3Storage(o => o.SecretKey = raw);
        Assert.Equal(expected, normalized.SecretKey);
    }

    [Fact]
    public void Normalize_NullAccessKey_RemainsNull()
    {
        var normalized = NormalizeViaAddS3Storage(o => o.AccessKey = null);
        Assert.Null(normalized.AccessKey);
    }

    [Fact]
    public void Normalize_NullSecretKey_RemainsNull()
    {
        var normalized = NormalizeViaAddS3Storage(o => o.SecretKey = null);
        Assert.Null(normalized.SecretKey);
    }

    [Theory]
    [InlineData("  http://localhost:9000  ", "http://localhost:9000")]
    [InlineData("http://localhost:9000", "http://localhost:9000")]
    [InlineData("  ", null)]
    [InlineData("", null)]
    public void Normalize_ServiceUrl_TrimsWhitespace_AndTreatsBlankAsNull(string raw, string? expected)
    {
        var normalized = NormalizeViaAddS3Storage(o => o.ServiceUrl = raw);
        Assert.Equal(expected, normalized.ServiceUrl);
    }

    // ── Explicit credential construction ───────────────────────────────────

    [Fact]
    public void AwsS3StorageClient_WithExplicitCredentials_ConstructsWithoutThrowing()
    {
        var options = new S3StorageOptions
        {
            Region = "us-east-1",
            AccessKey = "test-access-key",
            SecretKey = "test-secret-key"
        };

        // Construction should succeed; actual connectivity is not tested here.
        using var client = new AwsS3StorageClient(options);
        Assert.NotNull(client);
    }

    [Fact]
    public void AwsS3StorageClient_WithoutCredentials_ConstructsWithoutThrowing()
    {
        var options = new S3StorageOptions
        {
            Region = "us-east-1"
            // AccessKey and SecretKey intentionally omitted → ambient credential chain
        };

        using var client = new AwsS3StorageClient(options);
        Assert.NotNull(client);
    }

    [Fact]
    public void AwsS3StorageClient_WithServiceUrl_AndExplicitCredentials_ConstructsWithoutThrowing()
    {
        var options = new S3StorageOptions
        {
            ServiceUrl = "http://localhost:9000",
            ForcePathStyle = true,
            AccessKey = "minioadmin",
            SecretKey = "minioadmin"
        };

        using var client = new AwsS3StorageClient(options);
        Assert.NotNull(client);
    }

    [Fact]
    public void CreateConfig_WithCustomHttpServiceUrl_UsesAuthenticationRegion_Http_AndRequiredChecksumModes()
    {
        var config = AwsS3StorageClient.CreateConfig(new S3StorageOptions
        {
            Region = "us-east-1",
            ServiceUrl = "http://localhost:9000",
            ForcePathStyle = true
        });

        Assert.Equal(new Uri("http://localhost:9000"), new Uri(config.ServiceURL!, UriKind.Absolute));
        Assert.True(config.ForcePathStyle);
        Assert.Equal("us-east-1", config.AuthenticationRegion);
        Assert.True(config.UseHttp);
        Assert.Equal(RequestChecksumCalculation.WHEN_REQUIRED, config.RequestChecksumCalculation);
        Assert.Equal(ResponseChecksumValidation.WHEN_REQUIRED, config.ResponseChecksumValidation);
    }

    [Fact]
    public void CreateConfig_WithoutServiceUrl_UsesRegionalEndpoint_AndDefaultChecksumModes()
    {
        var config = AwsS3StorageClient.CreateConfig(new S3StorageOptions
        {
            Region = "eu-central-1"
        });

        Assert.Null(config.ServiceURL);
        Assert.Equal("eu-central-1", config.RegionEndpoint?.SystemName);
        Assert.Null(config.AuthenticationRegion);
        Assert.False(config.UseHttp);
        Assert.Equal(RequestChecksumCalculation.WHEN_SUPPORTED, config.RequestChecksumCalculation);
        Assert.Equal(ResponseChecksumValidation.WHEN_SUPPORTED, config.ResponseChecksumValidation);
    }

    [Fact]
    public async Task CreatePresignedGetObjectUrlAsync_WithCustomHttpServiceUrl_PreservesHttpScheme_AndSigV4Shape()
    {
        using var client = new AwsS3StorageClient(new S3StorageOptions
        {
            Region = "us-east-1",
            ServiceUrl = "http://localhost:9000",
            ForcePathStyle = true,
            AccessKey = "minioadmin",
            SecretKey = "minioadmin"
        });

        var presignedUrl = await client.CreatePresignedGetObjectUrlAsync(
            "bucket-name",
            "docs/object.txt",
            "v-123",
            DateTimeOffset.UtcNow.AddMinutes(10));

        Assert.Equal("http", presignedUrl.Scheme);
        Assert.Equal("localhost", presignedUrl.Host);
        Assert.Equal(9000, presignedUrl.Port);
        Assert.Equal("/bucket-name/docs/object.txt", presignedUrl.AbsolutePath);
        Assert.Contains("versionId=v-123", presignedUrl.Query, StringComparison.Ordinal);
        Assert.Contains("X-Amz-Algorithm=AWS4-HMAC-SHA256", presignedUrl.Query, StringComparison.Ordinal);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs the option through AddS3Storage normalization without actually registering
    /// anything meaningful, and returns the normalized options snapshot.
    /// </summary>
    private static S3StorageOptions NormalizeViaAddS3Storage(Action<S3StorageOptions> configure)
    {
        S3StorageOptions? captured = null;
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddS3Storage(o =>
        {
            // Provide minimal valid defaults so normalization doesn't fail for other fields.
            o.Region = "us-east-1";
            configure(o);
            captured = o;
        });
        return captured!;
    }

    private static void AssertManagedVariant(
        StorageServerSideEncryptionVariantDescriptor variant,
        ObjectServerSideEncryptionAlgorithm algorithm,
        bool supportsKeyId,
        bool supportsContext)
    {
        Assert.Equal(algorithm, variant.Algorithm);
        Assert.Equal(StorageServerSideEncryptionRequestStyle.Managed, variant.RequestStyle);
        Assert.Equal(
            [
                StorageServerSideEncryptionRequestOperation.PutObject,
                StorageServerSideEncryptionRequestOperation.CopyDestination,
                StorageServerSideEncryptionRequestOperation.InitiateMultipartUpload
            ],
            variant.SupportedRequestOperations);
        Assert.True(variant.SupportsResponseMetadata);
        Assert.Equal(supportsKeyId, variant.SupportsKeyId);
        Assert.Equal(supportsContext, variant.SupportsContext);
    }
}

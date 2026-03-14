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

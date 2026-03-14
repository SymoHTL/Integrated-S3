using System.Net;
using Amazon.Runtime;
using Amazon.S3;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Provider.S3.Internal;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class S3ErrorTranslatorTests
{
    private static AmazonS3Exception MakeException(string errorCode, HttpStatusCode statusCode) =>
        new(errorCode, ErrorType.Sender, errorCode, "req-test", statusCode);

    [Fact]
    public void NoSuchKey_MapsTo_ObjectNotFound()
    {
        var ex = MakeException("NoSuchKey", HttpStatusCode.NotFound);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.ObjectNotFound, error.Code);
        Assert.Contains("my-object.txt", error.Message);
        Assert.Contains("my-bucket", error.Message);
    }

    [Fact]
    public void Generic404_WithObjectKey_MapsTo_ObjectNotFound()
    {
        var ex = MakeException("UnknownError", HttpStatusCode.NotFound);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.ObjectNotFound, error.Code);
        Assert.Contains("my-object.txt", error.Message);
    }

    [Fact]
    public void Generic404_WithoutObjectKey_MapsTo_BucketNotFound()
    {
        var ex = MakeException("UnknownError", HttpStatusCode.NotFound);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", objectKey: null);

        Assert.Equal(StorageErrorCode.BucketNotFound, error.Code);
        Assert.Contains("my-bucket", error.Message);
    }

    [Fact]
    public void Generic404_WithEmptyObjectKey_MapsTo_BucketNotFound()
    {
        var ex = MakeException("UnknownError", HttpStatusCode.NotFound);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", objectKey: "");

        Assert.Equal(StorageErrorCode.BucketNotFound, error.Code);
    }

    [Fact]
    public void NoSuchBucket_MapsTo_BucketNotFound()
    {
        var ex = MakeException("NoSuchBucket", HttpStatusCode.NotFound);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "missing-bucket");

        Assert.Equal(StorageErrorCode.BucketNotFound, error.Code);
        Assert.Contains("missing-bucket", error.Message);
    }

    [Fact]
    public void NoSuchUpload_MapsTo_MultipartConflict()
    {
        var ex = MakeException("NoSuchUpload", HttpStatusCode.NotFound);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.MultipartConflict, error.Code);
        Assert.Contains("my-object.txt", error.Message);
    }

    [Fact]
    public void InvalidPart_MapsTo_MultipartConflict()
    {
        var ex = MakeException("InvalidPart", HttpStatusCode.BadRequest);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.MultipartConflict, error.Code);
    }

    [Fact]
    public void BadDigest_MapsTo_InvalidChecksum()
    {
        var ex = MakeException("BadDigest", HttpStatusCode.BadRequest);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.InvalidChecksum, error.Code);
    }

    [Fact]
    public void InvalidTag_MapsTo_InvalidTag()
    {
        var ex = MakeException("InvalidTag", HttpStatusCode.BadRequest);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.InvalidTag, error.Code);
    }
}

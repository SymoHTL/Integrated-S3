using System.Net;
using System.Net.Http;
using System.Net.Sockets;
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
    public void NoSuchUpload_MapsTo_NoSuchUpload()
    {
        var ex = MakeException("NoSuchUpload", HttpStatusCode.NotFound);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.NoSuchUpload, error.Code);
        Assert.Contains("my-object.txt", error.Message);
    }

    [Fact]
    public void InvalidPart_MapsTo_InvalidPart()
    {
        var ex = MakeException("InvalidPart", HttpStatusCode.BadRequest);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.InvalidPart, error.Code);
    }

    [Fact]
    public void InvalidPartOrder_MapsTo_InvalidPartOrder()
    {
        var ex = MakeException("InvalidPartOrder", HttpStatusCode.BadRequest);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.InvalidPartOrder, error.Code);
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

    // --- Generic 409 conflict translation (issue #139) ---

    [Fact]
    public void OperationAborted_MapsTo_VersionConflict_NotBucketAlreadyExists()
    {
        // S3 returns 409 OperationAborted when a conflicting/aborted operation is in progress on a
        // bucket. It must not be relabeled as a bucket-name collision.
        var ex = MakeException("OperationAborted", HttpStatusCode.Conflict);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket");

        Assert.Equal(StorageErrorCode.VersionConflict, error.Code);
        Assert.NotEqual(StorageErrorCode.BucketAlreadyExists, error.Code);
        Assert.Contains("my-bucket", error.Message);
        Assert.DoesNotContain("already exists", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Generic409_WithoutObjectKey_MapsTo_VersionConflict_NotBucketAlreadyExists()
    {
        // A bare 409 with an unrecognized error code and no object key (e.g. a bucket-config write)
        // is a generic conflict, not a bucket-name collision.
        var ex = MakeException("SomeUnrecognizedConflict", HttpStatusCode.Conflict);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", objectKey: null);

        Assert.Equal(StorageErrorCode.VersionConflict, error.Code);
        Assert.NotEqual(StorageErrorCode.BucketAlreadyExists, error.Code);
        Assert.DoesNotContain("already exists", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Generic409_WithObjectKey_MapsTo_MultipartConflict()
    {
        var ex = MakeException("SomeUnrecognizedConflict", HttpStatusCode.Conflict);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.MultipartConflict, error.Code);
    }

    [Fact]
    public void BucketAlreadyExists_StillMapsTo_BucketAlreadyExists()
    {
        // A genuine bucket-name collision must still surface as BucketAlreadyExists.
        var ex = MakeException("BucketAlreadyExists", HttpStatusCode.Conflict);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket");

        Assert.Equal(StorageErrorCode.BucketAlreadyExists, error.Code);
    }

    [Fact]
    public void BucketAlreadyOwnedByYou_StillMapsTo_BucketAlreadyOwnedByYou()
    {
        var ex = MakeException("BucketAlreadyOwnedByYou", HttpStatusCode.Conflict);

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket");

        Assert.Equal(StorageErrorCode.BucketAlreadyOwnedByYou, error.Code);
    }

    // --- Transport / service-level failure translation (issue #129) ---

    [Fact]
    public void ServiceException_With503_MapsTo_ProviderUnavailable()
    {
        var ex = new AmazonServiceException("Service Unavailable")
        {
            StatusCode = HttpStatusCode.ServiceUnavailable
        };

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket");

        Assert.Equal(StorageErrorCode.ProviderUnavailable, error.Code);
        Assert.Equal(503, error.SuggestedHttpStatusCode);
    }

    [Fact]
    public void ServiceException_WithGeneric500_MapsTo_ProviderUnavailable()
    {
        var ex = new AmazonServiceException("Internal Server Error")
        {
            StatusCode = HttpStatusCode.InternalServerError
        };

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket");

        Assert.Equal(StorageErrorCode.ProviderUnavailable, error.Code);
    }

    [Fact]
    public void ServiceException_WithNoHttpStatus_MapsTo_ProviderUnavailable()
    {
        // StatusCode defaults to 0 when the request never completed against the upstream.
        var ex = new AmazonServiceException("The endpoint could not be reached.");

        var error = S3ErrorTranslator.Translate(ex, "test-provider", "my-bucket");

        Assert.Equal(StorageErrorCode.ProviderUnavailable, error.Code);
        Assert.Equal(503, error.SuggestedHttpStatusCode);
    }

    [Fact]
    public void TranslateTransport_MapsTo_ProviderUnavailable_AndPreservesMessage()
    {
        var ex = new HttpRequestException("Connection reset by peer.");

        var error = S3ErrorTranslator.TranslateTransport(ex, "test-provider", "my-bucket", "my-object.txt");

        Assert.Equal(StorageErrorCode.ProviderUnavailable, error.Code);
        Assert.Equal(503, error.SuggestedHttpStatusCode);
        Assert.Equal("my-bucket", error.BucketName);
        Assert.Equal("my-object.txt", error.ObjectKey);
        Assert.Contains("Connection reset by peer.", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(typeof(HttpRequestException))]
    [InlineData(typeof(SocketException))]
    [InlineData(typeof(IOException))]
    [InlineData(typeof(TaskCanceledException))]
    public void IsTransportFailure_ReturnsTrue_ForTransportExceptions(Type exceptionType)
    {
        var ex = exceptionType == typeof(SocketException)
            ? new SocketException((int)SocketError.HostNotFound)
            : (Exception)Activator.CreateInstance(exceptionType)!;

        Assert.True(S3ErrorTranslator.IsTransportFailure(ex, CancellationToken.None));
    }

    [Fact]
    public void IsTransportFailure_ReturnsFalse_ForCallerCancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var ex = new OperationCanceledException(cts.Token);

        Assert.False(S3ErrorTranslator.IsTransportFailure(ex, cts.Token));
    }

    [Fact]
    public void IsTransportFailure_ReturnsTrue_ForTimeoutTaskCanceled_WhenCallerDidNotCancel()
    {
        // HttpClient request timeout: a TaskCanceledException with no caller cancellation.
        var ex = new TaskCanceledException("timeout");

        Assert.True(S3ErrorTranslator.IsTransportFailure(ex, CancellationToken.None));
    }
}

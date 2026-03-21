using System.Diagnostics;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;
using IntegratedS3.Abstractions.Models;
using Microsoft.Extensions.Logging;
using System.Globalization;
using PutBucketDefaultEncryptionStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketDefaultEncryptionRequest;
using UploadPartCopyStorageRequest = IntegratedS3.Abstractions.Requests.UploadPartCopyRequest;
using PutBucketTaggingStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketTaggingRequest;
using PutBucketLoggingStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketLoggingRequest;
using PutBucketWebsiteStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketWebsiteRequest;
using PutBucketRequestPaymentStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketRequestPaymentRequest;
using PutBucketAccelerateStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketAccelerateRequest;
using PutBucketLifecycleStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketLifecycleRequest;
using PutBucketReplicationStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketReplicationRequest;
using PutBucketNotificationConfigurationStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketNotificationConfigurationRequest;
using PutObjectLockConfigurationStorageRequest = IntegratedS3.Abstractions.Requests.PutObjectLockConfigurationRequest;
using ObjectLockConfiguration = IntegratedS3.Abstractions.Models.ObjectLockConfiguration;
using PutBucketAnalyticsConfigurationStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketAnalyticsConfigurationRequest;
using PutBucketMetricsConfigurationStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketMetricsConfigurationRequest;
using PutBucketInventoryConfigurationStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketInventoryConfigurationRequest;
using PutBucketIntelligentTieringConfigurationStorageRequest = IntegratedS3.Abstractions.Requests.PutBucketIntelligentTieringConfigurationRequest;
using PutObjectRetentionStorageRequest = IntegratedS3.Abstractions.Requests.PutObjectRetentionRequest;
using PutObjectLegalHoldStorageRequest = IntegratedS3.Abstractions.Requests.PutObjectLegalHoldRequest;
using GetObjectAttributesStorageRequest = IntegratedS3.Abstractions.Requests.GetObjectAttributesRequest;
using GetObjectAttributesStorageResponse = IntegratedS3.Abstractions.Responses.GetObjectAttributesResponse;
using ObjectPartsInfo = IntegratedS3.Abstractions.Responses.ObjectPartsInfo;
using ObjectPartInfo = IntegratedS3.Abstractions.Responses.ObjectPartInfo;
using RestoreObjectStorageRequest = IntegratedS3.Abstractions.Requests.RestoreObjectRequest;
using SelectObjectContentStorageRequest = IntegratedS3.Abstractions.Requests.SelectObjectContentRequest;

namespace IntegratedS3.Provider.S3.Internal;

internal sealed class AwsS3StorageClient : IS3StorageClient
{
    private readonly IAmazonS3 _s3;
    private readonly Uri? _serviceUri;
    private readonly ILogger<AwsS3StorageClient>? _logger;

    public AwsS3StorageClient(S3StorageOptions options, ILogger<AwsS3StorageClient>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        _logger = logger;
        _serviceUri = TryCreateAbsoluteUri(options.ServiceUrl);
        var config = CreateConfig(options);
        _s3 = !string.IsNullOrWhiteSpace(options.AccessKey) && !string.IsNullOrWhiteSpace(options.SecretKey)
            ? new AmazonS3Client(new BasicAWSCredentials(options.AccessKey, options.SecretKey), config)
            : new AmazonS3Client(config);
    }

    internal static AmazonS3Config CreateConfig(S3StorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var config = new AmazonS3Config
        {
            ForcePathStyle = options.ForcePathStyle
        };

        if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
        {
            config.ServiceURL = options.ServiceUrl;
            config.AuthenticationRegion = options.Region;

            if (TryCreateAbsoluteUri(options.ServiceUrl) is { } serviceUri)
                config.UseHttp = string.Equals(serviceUri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase);

            // Local S3-compatible endpoints frequently lag the AWS SDK v4
            // flexible-checksum defaults; required-only mode preserves explicit
            // checksum flows without injecting extra compatibility failures.
            config.RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED;
            config.ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED;
        }
        else
        {
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region);
        }

        return config;
    }

    // -------------------------------------------------------------------------
    // Bucket operations
    // -------------------------------------------------------------------------

    public async Task<IReadOnlyList<S3BucketEntry>> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        var response = await _s3.ListBucketsAsync(cancellationToken).ConfigureAwait(false);
        return response.Buckets
            .Select(b => new S3BucketEntry(
                b.BucketName,
                b.CreationDate.HasValue
                    ? new DateTimeOffset(b.CreationDate.Value, TimeSpan.Zero)
                    : DateTimeOffset.UtcNow))
            .ToList();
    }

    public async Task<S3BucketEntry> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("CreateBucket", bucketName);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}", "CreateBucket", bucketName);

        try
        {
            var request = new PutBucketRequest { BucketName = bucketName };
            await _s3.PutBucketAsync(request, cancellationToken).ConfigureAwait(false);
            var result = new S3BucketEntry(bucketName, DateTimeOffset.UtcNow);

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("CreateBucket", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}", "CreateBucket", sw.ElapsedMilliseconds, bucketName);
            return result;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("CreateBucket", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}", "CreateBucket", ex.ErrorCode, ex.Message, bucketName);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("CreateBucket", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}", "CreateBucket", bucketName);
            throw;
        }
    }

    public async Task<S3BucketEntry?> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("HeadBucket", bucketName);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}", "HeadBucket", bucketName);

        try
        {
            var request = new HeadBucketRequest { BucketName = bucketName };
            await _s3.HeadBucketAsync(request, cancellationToken).ConfigureAwait(false);

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("HeadBucket", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}", "HeadBucket", sw.ElapsedMilliseconds, bucketName);
            return new S3BucketEntry(bucketName, DateTimeOffset.UtcNow);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            sw.Stop();
            S3StorageTelemetry.RecordSuccess("HeadBucket", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName} (not found)", "HeadBucket", sw.ElapsedMilliseconds, bucketName);
            return null;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("HeadBucket", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}", "HeadBucket", ex.ErrorCode, ex.Message, bucketName);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("HeadBucket", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}", "HeadBucket", bucketName);
            throw;
        }
    }

    public async Task DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("DeleteBucket", bucketName);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}", "DeleteBucket", bucketName);

        try
        {
            var request = new DeleteBucketRequest { BucketName = bucketName };
            await _s3.DeleteBucketAsync(request, cancellationToken).ConfigureAwait(false);

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("DeleteBucket", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}", "DeleteBucket", sw.ElapsedMilliseconds, bucketName);
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("DeleteBucket", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}", "DeleteBucket", ex.ErrorCode, ex.Message, bucketName);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("DeleteBucket", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}", "DeleteBucket", bucketName);
            throw;
        }
    }

    public async Task<S3BucketLocationEntry> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new GetBucketLocationRequest { BucketName = bucketName };
        var response = await _s3.GetBucketLocationAsync(request, cancellationToken).ConfigureAwait(false);
        return new S3BucketLocationEntry(response.Location?.Value);
    }

    // -------------------------------------------------------------------------
    // Bucket versioning
    // -------------------------------------------------------------------------

    public async Task<S3VersioningEntry> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new GetBucketVersioningRequest { BucketName = bucketName };
        var response = await _s3.GetBucketVersioningAsync(request, cancellationToken).ConfigureAwait(false);
        var status = MapVersioningStatus(response.VersioningConfig?.Status);
        return new S3VersioningEntry(status);
    }

    public async Task<S3VersioningEntry> SetBucketVersioningAsync(string bucketName, BucketVersioningStatus status, CancellationToken cancellationToken = default)
    {
        var request = new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = MapVersioningStatusToSdk(status)
            }
        };
        await _s3.PutBucketVersioningAsync(request, cancellationToken).ConfigureAwait(false);
        return new S3VersioningEntry(status);
    }

    // -------------------------------------------------------------------------
    // Bucket CORS
    // -------------------------------------------------------------------------

    public async Task<S3CorsConfigurationEntry?> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new GetCORSConfigurationRequest
        {
            BucketName = bucketName
        };

        var response = await _s3.GetCORSConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        return new S3CorsConfigurationEntry(
            (response.Configuration?.Rules ?? [])
                .Select(MapCorsRule)
                .ToArray());
    }

    public async Task<S3CorsConfigurationEntry> SetBucketCorsAsync(string bucketName, IReadOnlyList<S3CorsRuleEntry> rules, CancellationToken cancellationToken = default)
    {
        var request = new PutCORSConfigurationRequest
        {
            BucketName = bucketName,
            Configuration = new CORSConfiguration
            {
                Rules = rules.Select(MapCorsRule).ToList()
            }
        };

        await _s3.PutCORSConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        return new S3CorsConfigurationEntry(rules.ToArray());
    }

    public async Task DeleteBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new DeleteCORSConfigurationRequest
        {
            BucketName = bucketName
        };

        await _s3.DeleteCORSConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<BucketDefaultEncryptionConfiguration> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new GetBucketEncryptionRequest
        {
            BucketName = bucketName
        };

        var response = await _s3.GetBucketEncryptionAsync(request, cancellationToken).ConfigureAwait(false);
        return S3BucketDefaultEncryptionMapper.ToBucketDefaultEncryptionConfiguration(bucketName, response.ServerSideEncryptionConfiguration);
    }

    public async Task<BucketDefaultEncryptionConfiguration> SetBucketDefaultEncryptionAsync(PutBucketDefaultEncryptionStorageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var putRequest = new PutBucketEncryptionRequest
        {
            BucketName = request.BucketName,
            ServerSideEncryptionConfiguration = S3BucketDefaultEncryptionMapper.ToServerSideEncryptionConfiguration(request.Rule)
        };

        await _s3.PutBucketEncryptionAsync(putRequest, cancellationToken).ConfigureAwait(false);
        return new BucketDefaultEncryptionConfiguration
        {
            BucketName = request.BucketName,
            Rule = new BucketDefaultEncryptionRule
            {
                Algorithm = request.Rule.Algorithm,
                KeyId = request.Rule.KeyId,
                BucketKeyEnabled = request.Rule.BucketKeyEnabled
            }
        };
    }

    public async Task DeleteBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new DeleteBucketEncryptionRequest
        {
            BucketName = bucketName
        };

        await _s3.DeleteBucketEncryptionAsync(request, cancellationToken).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Object listing
    // -------------------------------------------------------------------------

    public async Task<S3ObjectListPage> ListObjectsAsync(
        string bucketName,
        string? prefix,
        string? continuationToken,
        int? maxKeys,
        CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("ListObjects", bucketName);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}", "ListObjects", bucketName);

        try
        {
            var request = new ListObjectsV2Request
            {
                BucketName = bucketName,
                Prefix = prefix,
                ContinuationToken = continuationToken
            };

            if (maxKeys.HasValue)
                request.MaxKeys = maxKeys.Value;

            var response = await _s3.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

            var entries = (response.S3Objects ?? [])
                .Select(o => new S3ObjectEntry(
                    Key: o.Key ?? string.Empty,
                    ContentLength: o.Size ?? 0,
                    ContentType: null,
                    ETag: o.ETag,
                    LastModifiedUtc: ToDateTimeOffset(o.LastModified),
                    Metadata: null,
                    VersionId: null))
                .ToList();

            var result = new S3ObjectListPage(
                entries,
                response.IsTruncated == true ? response.NextContinuationToken : null);

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("ListObjects", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}", "ListObjects", sw.ElapsedMilliseconds, bucketName);
            return result;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("ListObjects", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}", "ListObjects", ex.ErrorCode, ex.Message, bucketName);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("ListObjects", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}", "ListObjects", bucketName);
            throw;
        }
    }

    public async Task<S3ObjectVersionListPage> ListObjectVersionsAsync(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? versionIdMarker,
        int? maxKeys,
        CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("ListObjectVersions", bucketName);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}", "ListObjectVersions", bucketName);

        try
        {
            var request = new ListVersionsRequest
            {
                BucketName = bucketName,
                Prefix = prefix,
                Delimiter = delimiter,
                KeyMarker = keyMarker,
                VersionIdMarker = versionIdMarker
            };

            if (maxKeys.HasValue)
                request.MaxKeys = maxKeys.Value;

            var response = await _s3.ListVersionsAsync(request, cancellationToken).ConfigureAwait(false);

            // In SDK v4, Versions contains both object versions and delete markers (distinguished by IsDeleteMarker)
            var entries = (response.Versions ?? [])
                .Select(v => new S3ObjectEntry(
                    Key: v.Key ?? string.Empty,
                    ContentLength: v.IsDeleteMarker == true ? 0 : (v.Size ?? 0),
                    ContentType: null,
                    ETag: v.IsDeleteMarker == true ? null : v.ETag,
                    LastModifiedUtc: ToDateTimeOffset(v.LastModified),
                    Metadata: null,
                    VersionId: v.VersionId,
                    IsLatest: v.IsLatest == true,
                    IsDeleteMarker: v.IsDeleteMarker == true))
                .ToList();

            var result = new S3ObjectVersionListPage(
                entries,
                response.IsTruncated == true ? response.NextKeyMarker : null,
                response.IsTruncated == true ? response.NextVersionIdMarker : null);

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("ListObjectVersions", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}", "ListObjectVersions", sw.ElapsedMilliseconds, bucketName);
            return result;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("ListObjectVersions", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}", "ListObjectVersions", ex.ErrorCode, ex.Message, bucketName);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("ListObjectVersions", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}", "ListObjectVersions", bucketName);
            throw;
        }
    }

    // -------------------------------------------------------------------------
    // Object CRUD
    // -------------------------------------------------------------------------

    public async Task<S3ObjectEntry?> HeadObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        ObjectCustomerEncryptionSettings? customerEncryption,
        CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("HeadObject", bucketName, key);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}/{Key}", "HeadObject", bucketName, key);

        try
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId
            };

            S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, customerEncryption);

            var response = await _s3.GetObjectMetadataAsync(request, cancellationToken).ConfigureAwait(false);

            var result = new S3ObjectEntry(
                Key: key,
                ContentLength: response.ContentLength,
                ContentType: response.ContentType,
                ETag: response.ETag,
                LastModifiedUtc: ToDateTimeOffset(response.LastModified),
                Metadata: BuildMetadataDictionary(response.Metadata),
                VersionId: response.VersionId,
                CacheControl: response.CacheControl,
                ContentDisposition: response.ContentDisposition,
                ContentEncoding: response.ContentEncoding,
                ContentLanguage: response.ContentLanguage,
                ExpiresUtc: ParseExpiresString(response.ExpiresString),
                ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                    response.ServerSideEncryptionMethod,
                    response.ServerSideEncryptionKeyManagementServiceKeyId,
                    response.BucketKeyEnabled),
                RetentionMode: S3ObjectLockMapper.ToRetentionMode(response.ObjectLockMode),
                RetainUntilDateUtc: ToNullableDateTimeOffset(response.ObjectLockRetainUntilDate),
                LegalHoldStatus: S3ObjectLockMapper.ToLegalHoldStatus(response.ObjectLockLegalHoldStatus),
                CustomerEncryption: S3ServerSideEncryptionMapper.ToCustomerEncryptionInfo(
                    response.ServerSideEncryptionCustomerMethod,
                    response.ServerSideEncryptionCustomerProvidedKeyMD5));

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("HeadObject", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}/{Key}", "HeadObject", sw.ElapsedMilliseconds, bucketName, key);
            return result;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            sw.Stop();
            S3StorageTelemetry.RecordSuccess("HeadObject", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}/{Key} (not found)", "HeadObject", sw.ElapsedMilliseconds, bucketName, key);
            return null;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("HeadObject", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}/{Key}", "HeadObject", ex.ErrorCode, ex.Message, bucketName, key);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("HeadObject", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}/{Key}", "HeadObject", bucketName, key);
            throw;
        }
    }

    public Task<Uri> CreatePresignedGetObjectUrlAsync(
        string bucketName,
        string key,
        string? versionId,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var request = new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId,
            Expires = expiresAtUtc.UtcDateTime,
            Verb = HttpVerb.GET
        };

        var presignedUrl = _s3.GetPreSignedURL(request);
        return Task.FromResult(AlignPresignedUrlWithServiceUrl(new Uri(presignedUrl, UriKind.Absolute)));
    }

    public Task<Uri> CreatePresignedPutObjectUrlAsync(
        string bucketName,
        string key,
        string? contentType,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var request = new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentType = contentType,
            Expires = expiresAtUtc.UtcDateTime,
            Verb = HttpVerb.PUT
        };

        var presignedUrl = _s3.GetPreSignedURL(request);
        return Task.FromResult(new Uri(presignedUrl, UriKind.Absolute));
    }

    public async Task<S3GetObjectResult> GetObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        ObjectRange? range,
        string? ifMatchETag,
        string? ifNoneMatchETag,
        DateTimeOffset? ifModifiedSinceUtc,
        DateTimeOffset? ifUnmodifiedSinceUtc,
        ObjectCustomerEncryptionSettings? customerEncryption,
        CancellationToken cancellationToken = default)
    {
        // Activity is not disposed here because the response stream outlives this call.
        var activity = S3StorageTelemetry.StartActivity("GetObject", bucketName, key);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}/{Key}", "GetObject", bucketName, key);

        try
        {
            var request = new Amazon.S3.Model.GetObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId
            };

            if (range?.Start.HasValue == true || range?.End.HasValue == true)
            {
                request.ByteRange = range switch
                {
                    { Start: not null, End: not null } => new ByteRange(range.Start.Value, range.End.Value),
                    { Start: not null } => new ByteRange($"bytes={range.Start.Value}-"),
                    { End: not null } => new ByteRange($"bytes=-{range.End.Value}"),
                    _ => null
                };
            }

            if (!string.IsNullOrEmpty(ifMatchETag))
                request.EtagToMatch = ifMatchETag;

            if (!string.IsNullOrEmpty(ifNoneMatchETag))
                request.EtagToNotMatch = ifNoneMatchETag;

            if (ifModifiedSinceUtc.HasValue)
                request.ModifiedSinceDate = ifModifiedSinceUtc.Value.UtcDateTime;

            if (ifUnmodifiedSinceUtc.HasValue)
                request.UnmodifiedSinceDate = ifUnmodifiedSinceUtc.Value.UtcDateTime;

            S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, customerEncryption);

            var response = await _s3.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);

            var entry = new S3ObjectEntry(
                Key: key,
                ContentLength: response.ContentLength,
                ContentType: response.Headers.ContentType,
                ETag: response.ETag,
                LastModifiedUtc: ToDateTimeOffset(response.LastModified),
                Metadata: BuildMetadataDictionary(response.Metadata),
                VersionId: response.VersionId,
                CacheControl: response.Headers.CacheControl,
                ContentDisposition: response.Headers.ContentDisposition,
                ContentEncoding: response.Headers.ContentEncoding,
                ContentLanguage: GetHeaderValue(response.Headers, "Content-Language"),
                ExpiresUtc: ParseExpiresString(response.ExpiresString),
                ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                    response.ServerSideEncryptionMethod,
                    response.ServerSideEncryptionKeyManagementServiceKeyId,
                    response.BucketKeyEnabled),
                RetentionMode: S3ObjectLockMapper.ToRetentionMode(response.ObjectLockMode),
                RetainUntilDateUtc: ToNullableDateTimeOffset(response.ObjectLockRetainUntilDate),
                LegalHoldStatus: S3ObjectLockMapper.ToLegalHoldStatus(response.ObjectLockLegalHoldStatus),
                CustomerEncryption: S3ServerSideEncryptionMapper.ToCustomerEncryptionInfo(
                    response.ServerSideEncryptionCustomerMethod,
                    response.ServerSideEncryptionCustomerProvidedKeyMD5));

            long totalContentLength = TryParseContentRangeTotal(response.ContentRange)
                ?? response.ContentLength;

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("GetObject", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}/{Key}", "GetObject", sw.ElapsedMilliseconds, bucketName, key);
            activity?.Dispose();
            return new S3GetObjectResult(entry, response.ResponseStream, totalContentLength, response);
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("GetObject", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}/{Key}", "GetObject", ex.ErrorCode, ex.Message, bucketName, key);
            activity?.Dispose();
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("GetObject", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}/{Key}", "GetObject", bucketName, key);
            activity?.Dispose();
            throw;
        }
    }

    public async Task<ObjectRetentionInfo> GetObjectRetentionAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default)
    {
        var request = new GetObjectRetentionRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        };

        var response = await _s3.GetObjectRetentionAsync(request, cancellationToken).ConfigureAwait(false);

        return new ObjectRetentionInfo
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId,
            Mode = S3ObjectLockMapper.ToRetentionMode(response.Retention?.Mode),
            RetainUntilDateUtc = ToNullableDateTimeOffset(response.Retention?.RetainUntilDate)
        };
    }

    public async Task<ObjectLegalHoldInfo> GetObjectLegalHoldAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default)
    {
        var request = new GetObjectLegalHoldRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        };

        var response = await _s3.GetObjectLegalHoldAsync(request, cancellationToken).ConfigureAwait(false);

        return new ObjectLegalHoldInfo
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId,
            Status = S3ObjectLockMapper.ToLegalHoldStatus(response.LegalHold?.Status)
        };
    }

    public async Task<GetObjectAttributesStorageResponse> GetObjectAttributesAsync(
        GetObjectAttributesStorageRequest request,
        CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.GetObjectAttributesRequest
        {
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId,
        };

        foreach (var attr in request.ObjectAttributes)
        {
            sdkRequest.ObjectAttributes.Add(new Amazon.S3.ObjectAttributes(attr));
        }

        var sdkResponse = await _s3.GetObjectAttributesAsync(sdkRequest, cancellationToken).ConfigureAwait(false);

        var checksums = new Dictionary<string, string>();
        if (sdkResponse.Checksum?.ChecksumCRC32 is not null) checksums["crc32"] = sdkResponse.Checksum.ChecksumCRC32;
        if (sdkResponse.Checksum?.ChecksumCRC32C is not null) checksums["crc32c"] = sdkResponse.Checksum.ChecksumCRC32C;
        if (sdkResponse.Checksum?.ChecksumSHA1 is not null) checksums["sha1"] = sdkResponse.Checksum.ChecksumSHA1;
        if (sdkResponse.Checksum?.ChecksumSHA256 is not null) checksums["sha256"] = sdkResponse.Checksum.ChecksumSHA256;
        if (sdkResponse.Checksum?.ChecksumCRC64NVME is not null) checksums["crc64nvme"] = sdkResponse.Checksum.ChecksumCRC64NVME;

        ObjectPartsInfo? partsInfo = null;
        if (sdkResponse.ObjectParts?.TotalPartsCount > 0)
        {
            partsInfo = new ObjectPartsInfo
            {
                TotalPartsCount = sdkResponse.ObjectParts.TotalPartsCount ?? 0,
                PartNumberMarker = sdkResponse.ObjectParts.PartNumberMarker,
                NextPartNumberMarker = sdkResponse.ObjectParts.NextPartNumberMarker,
                MaxParts = sdkResponse.ObjectParts.MaxParts,
                IsTruncated = sdkResponse.ObjectParts.IsTruncated ?? false,
                Parts = sdkResponse.ObjectParts.Parts?.Select(p => new ObjectPartInfo
                {
                    PartNumber = p.PartNumber ?? 0,
                    Size = p.Size ?? 0,
                    ChecksumCrc32 = p.ChecksumCRC32,
                    ChecksumCrc32C = p.ChecksumCRC32C,
                    ChecksumSha1 = p.ChecksumSHA1,
                    ChecksumSha256 = p.ChecksumSHA256,
                    ChecksumCrc64Nvme = p.ChecksumCRC64NVME,
                }).ToList()
            };
        }

        return new GetObjectAttributesStorageResponse
        {
            VersionId = sdkResponse.VersionId,
            IsDeleteMarker = sdkResponse.DeleteMarker ?? false,
            LastModifiedUtc = sdkResponse.LastModified.HasValue && sdkResponse.LastModified.Value != default ? new DateTimeOffset(sdkResponse.LastModified.Value, TimeSpan.Zero) : null,
            ETag = sdkResponse.ETag?.Trim('"'),
            ObjectSize = sdkResponse.ObjectSize > 0 ? sdkResponse.ObjectSize : null,
            StorageClass = sdkResponse.StorageClass?.Value,
            Checksums = checksums.Count > 0 ? checksums : null,
            ObjectParts = partsInfo,
        };
    }

    public async Task<S3ObjectEntry> PutObjectAsync(
        string bucketName,
        string key,
        Stream content,
        long? contentLength,
        string? contentType,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        IReadOnlyDictionary<string, string>? checksums,
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        ObjectCustomerEncryptionSettings? customerEncryption,
        string? storageClass,
        string? ifMatchETag,
        string? ifNoneMatchETag,
        CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("PutObject", bucketName, key);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}/{Key}", "PutObject", bucketName, key);

        try
        {
            var request = new Amazon.S3.Model.PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                InputStream = content,
                ContentType = contentType ?? "application/octet-stream",
                AutoCloseStream = false
            };

            if (contentLength.HasValue)
                request.Headers.ContentLength = contentLength.Value;

            ApplyStandardObjectHeaders(request.Headers, cacheControl, contentDisposition, contentEncoding, contentLanguage, expiresUtc);

            if (metadata is not null)
            {
                foreach (var (k, v) in metadata)
                    request.Metadata[k] = v;
            }

            request.TagSet = BuildTagSet(tags);
            ApplyChecksumHeaders(request, checksumAlgorithm: null, checksums);
            S3ServerSideEncryptionMapper.ApplyTo(request, serverSideEncryption);
            S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, customerEncryption);

            if (!string.IsNullOrWhiteSpace(storageClass))
                request.StorageClass = new S3StorageClass(storageClass);

            if (!string.IsNullOrWhiteSpace(ifMatchETag))
                request.IfMatch = ifMatchETag;

            if (!string.IsNullOrWhiteSpace(ifNoneMatchETag))
                request.IfNoneMatch = ifNoneMatchETag;

            var response = await _s3.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);

            var result = new S3ObjectEntry(
                Key: key,
                ContentLength: contentLength ?? 0,
                ContentType: contentType,
                ETag: response.ETag,
                LastModifiedUtc: DateTimeOffset.UtcNow,
                Metadata: metadata,
                VersionId: response.VersionId,
                Checksums: BuildChecksums(
                    response.ChecksumCRC32,
                    response.ChecksumCRC32C,
                    response.ChecksumCRC64NVME,
                    response.ChecksumSHA1,
                    response.ChecksumSHA256),
                CacheControl: cacheControl,
                ContentDisposition: contentDisposition,
                ContentEncoding: contentEncoding,
                ContentLanguage: contentLanguage,
                ExpiresUtc: expiresUtc,
                ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                    response.ServerSideEncryptionMethod,
                    response.ServerSideEncryptionKeyManagementServiceKeyId,
                    response.BucketKeyEnabled),
                CustomerEncryption: S3ServerSideEncryptionMapper.ToCustomerEncryptionInfo(
                    response.ServerSideEncryptionCustomerMethod,
                    response.ServerSideEncryptionCustomerProvidedKeyMD5));

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("PutObject", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}/{Key}", "PutObject", sw.ElapsedMilliseconds, bucketName, key);
            return result;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("PutObject", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}/{Key}", "PutObject", ex.ErrorCode, ex.Message, bucketName, key);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("PutObject", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}/{Key}", "PutObject", bucketName, key);
            throw;
        }
    }

    public async Task<S3DeleteObjectResult> DeleteObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("DeleteObject", bucketName, key);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}/{Key}", "DeleteObject", bucketName, key);

        try
        {
            var request = new Amazon.S3.Model.DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId
            };

            var response = await _s3.DeleteObjectAsync(request, cancellationToken).ConfigureAwait(false);

            // In SDK v4, DeleteMarker is a string ("true"/"false") not a bool
            var isDeleteMarker = string.Equals(response.DeleteMarker, "true", StringComparison.OrdinalIgnoreCase);
            var result = new S3DeleteObjectResult(key, response.VersionId, isDeleteMarker);

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("DeleteObject", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}/{Key}", "DeleteObject", sw.ElapsedMilliseconds, bucketName, key);
            return result;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("DeleteObject", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}/{Key}", "DeleteObject", ex.ErrorCode, ex.Message, bucketName, key);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("DeleteObject", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}/{Key}", "DeleteObject", bucketName, key);
            throw;
        }
    }

    // -------------------------------------------------------------------------
    // Copy + multipart
    // -------------------------------------------------------------------------

    public async Task<S3ObjectEntry> CopyObjectAsync(
        string sourceBucketName,
        string sourceKey,
        string destinationBucketName,
        string destinationKey,
        string? sourceVersionId,
        string? sourceIfMatchETag,
        string? sourceIfNoneMatchETag,
        DateTimeOffset? sourceIfModifiedSinceUtc,
        DateTimeOffset? sourceIfUnmodifiedSinceUtc,
        CopyObjectMetadataDirective metadataDirective,
        string? contentType,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc,
        IReadOnlyDictionary<string, string>? metadata,
        bool overwriteIfExists,
        ObjectTaggingDirective taggingDirective,
        IReadOnlyDictionary<string, string>? tags,
        string? checksumAlgorithm,
        IReadOnlyDictionary<string, string>? checksums,
        ObjectServerSideEncryptionSettings? destinationServerSideEncryption,
        ObjectCustomerEncryptionSettings? sourceCustomerEncryption,
        ObjectCustomerEncryptionSettings? destinationCustomerEncryption,
        string? storageClass,
        CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("CopyObject", destinationBucketName, destinationKey);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}/{Key}", "CopyObject", destinationBucketName, destinationKey);

        try
        {
            var request = new CopyObjectRequest
            {
                SourceBucket = sourceBucketName,
                SourceKey = sourceKey,
                SourceVersionId = sourceVersionId,
                DestinationBucket = destinationBucketName,
                DestinationKey = destinationKey,
                ETagToMatch = sourceIfMatchETag,
                ETagToNotMatch = sourceIfNoneMatchETag
            };

            if (sourceIfModifiedSinceUtc.HasValue)
                request.ModifiedSinceDate = sourceIfModifiedSinceUtc.Value.UtcDateTime;

            if (sourceIfUnmodifiedSinceUtc.HasValue)
                request.UnmodifiedSinceDate = sourceIfUnmodifiedSinceUtc.Value.UtcDateTime;

            if (!overwriteIfExists)
                request.IfNoneMatch = "*";

            request.MetadataDirective = metadataDirective == CopyObjectMetadataDirective.Replace
                ? S3MetadataDirective.REPLACE
                : S3MetadataDirective.COPY;

            if (metadataDirective == CopyObjectMetadataDirective.Replace)
            {
                request.ContentType = contentType;
                ApplyStandardObjectHeaders(request.Headers, cacheControl, contentDisposition, contentEncoding, contentLanguage, expiresUtc);

                if (metadata is not null)
                {
                    foreach (var (name, value) in metadata)
                        request.Metadata[name] = value;
                }
            }

            request.TaggingDirective = taggingDirective == ObjectTaggingDirective.Replace
                ? TaggingDirective.REPLACE
                : TaggingDirective.COPY;

            if (taggingDirective == ObjectTaggingDirective.Replace)
            {
                request.TagSet = BuildTagSet(tags) ?? new List<Tag>();
            }

            if (!string.IsNullOrWhiteSpace(storageClass))
                request.StorageClass = new S3StorageClass(storageClass);

            ApplyChecksumHeaders(request, checksumAlgorithm, checksums);
            S3ServerSideEncryptionMapper.ApplyTo(request, destinationServerSideEncryption);
            S3ServerSideEncryptionMapper.ApplyCopySourceCustomerEncryption(request, sourceCustomerEncryption);
            S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, destinationCustomerEncryption);

            var response = await _s3.CopyObjectAsync(request, cancellationToken).ConfigureAwait(false);

            var result = new S3ObjectEntry(
                Key: destinationKey,
                ContentLength: 0,
                ContentType: metadataDirective == CopyObjectMetadataDirective.Replace ? contentType : null,
                ETag: response.ETag,
                LastModifiedUtc: string.IsNullOrEmpty(response.LastModified) ? DateTimeOffset.UtcNow
                    : DateTime.TryParse(response.LastModified, out var dt) ? new DateTimeOffset(dt, TimeSpan.Zero) : DateTimeOffset.UtcNow,
                Metadata: metadataDirective == CopyObjectMetadataDirective.Replace ? metadata : null,
                VersionId: response.VersionId,
                Checksums: BuildChecksums(
                    response.ChecksumCRC32,
                    response.ChecksumCRC32C,
                    response.ChecksumCRC64NVME,
                    response.ChecksumSHA1,
                    response.ChecksumSHA256),
                CacheControl: metadataDirective == CopyObjectMetadataDirective.Replace ? cacheControl : null,
                ContentDisposition: metadataDirective == CopyObjectMetadataDirective.Replace ? contentDisposition : null,
                ContentEncoding: metadataDirective == CopyObjectMetadataDirective.Replace ? contentEncoding : null,
                ContentLanguage: metadataDirective == CopyObjectMetadataDirective.Replace ? contentLanguage : null,
                ExpiresUtc: metadataDirective == CopyObjectMetadataDirective.Replace ? expiresUtc : null,
                ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                    response.ServerSideEncryptionMethod,
                    response.ServerSideEncryptionKeyManagementServiceKeyId,
                    response.BucketKeyEnabled),
                CustomerEncryption: S3ServerSideEncryptionMapper.ToCustomerEncryptionInfo(
                    response.ServerSideEncryptionCustomerMethod,
                    response.ServerSideEncryptionCustomerProvidedKeyMD5));

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("CopyObject", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}/{Key}", "CopyObject", sw.ElapsedMilliseconds, destinationBucketName, destinationKey);
            return result;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("CopyObject", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}/{Key}", "CopyObject", ex.ErrorCode, ex.Message, destinationBucketName, destinationKey);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("CopyObject", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}/{Key}", "CopyObject", destinationBucketName, destinationKey);
            throw;
        }
    }

    public async Task<MultipartUploadInfo> InitiateMultipartUploadAsync(
        string bucketName,
        string key,
        string? contentType,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        string? checksumAlgorithm,
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        ObjectCustomerEncryptionSettings? customerEncryption,
        string? storageClass,
        CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("InitiateMultipartUpload", bucketName, key);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}/{Key}", "InitiateMultipartUpload", bucketName, key);

        try
        {
            var request = new InitiateMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = key,
                ContentType = contentType
            };

            ApplyStandardObjectHeaders(request.Headers, cacheControl, contentDisposition, contentEncoding, contentLanguage, expiresUtc);

            if (metadata is not null)
            {
                foreach (var (name, value) in metadata)
                    request.Metadata[name] = value;
            }

            request.TagSet = BuildTagSet(tags);
            var sdkChecksumAlgorithm = MapChecksumAlgorithm(checksumAlgorithm);
            if (sdkChecksumAlgorithm is not null)
                request.ChecksumAlgorithm = sdkChecksumAlgorithm;

            S3ServerSideEncryptionMapper.ApplyTo(request, serverSideEncryption);
            S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, customerEncryption);

            if (!string.IsNullOrWhiteSpace(storageClass))
                request.StorageClass = new S3StorageClass(storageClass);

            var response = await _s3.InitiateMultipartUploadAsync(request, cancellationToken).ConfigureAwait(false);

            var result = new MultipartUploadInfo
            {
                BucketName = bucketName,
                Key = key,
                UploadId = response.UploadId,
                InitiatedAtUtc = DateTimeOffset.UtcNow,
                ChecksumAlgorithm = NormalizeChecksumAlgorithm(response.ChecksumAlgorithm?.ToString()) ?? NormalizeChecksumAlgorithm(checksumAlgorithm),
                ServerSideEncryption = S3ServerSideEncryptionMapper.ToInfo(
                    response.ServerSideEncryptionMethod,
                    response.ServerSideEncryptionKeyManagementServiceKeyId,
                    response.BucketKeyEnabled),
                CustomerEncryption = S3ServerSideEncryptionMapper.ToCustomerEncryptionInfo(
                    response.ServerSideEncryptionCustomerMethod,
                    response.ServerSideEncryptionCustomerProvidedKeyMD5)
            };

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("InitiateMultipartUpload", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}/{Key}", "InitiateMultipartUpload", sw.ElapsedMilliseconds, bucketName, key);
            return result;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("InitiateMultipartUpload", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}/{Key}", "InitiateMultipartUpload", ex.ErrorCode, ex.Message, bucketName, key);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("InitiateMultipartUpload", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}/{Key}", "InitiateMultipartUpload", bucketName, key);
            throw;
        }
    }

    public async Task<MultipartUploadPart> UploadMultipartPartAsync(
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        Stream content,
        long? contentLength,
        string? checksumAlgorithm,
        IReadOnlyDictionary<string, string>? checksums,
        ObjectCustomerEncryptionSettings? customerEncryption,
        CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("UploadPart", bucketName, key);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}/{Key}", "UploadPart", bucketName, key);

        try
        {
            var request = new UploadPartRequest
            {
                BucketName = bucketName,
                Key = key,
                UploadId = uploadId,
                PartNumber = partNumber,
                InputStream = content
            };

            if (contentLength.HasValue)
                request.PartSize = contentLength.Value;

            ApplyChecksumHeaders(request, checksumAlgorithm, checksums);
            S3ServerSideEncryptionMapper.ApplyCustomerEncryption(request, customerEncryption);

            var response = await _s3.UploadPartAsync(request, cancellationToken).ConfigureAwait(false);

            var result = new MultipartUploadPart
            {
                PartNumber = response.PartNumber.GetValueOrDefault(partNumber),
                ETag = response.ETag ?? string.Empty,
                ContentLength = contentLength ?? 0,
                LastModifiedUtc = DateTimeOffset.UtcNow,
                Checksums = BuildChecksums(
                    response.ChecksumCRC32,
                    response.ChecksumCRC32C,
                    response.ChecksumCRC64NVME,
                    response.ChecksumSHA1,
                    response.ChecksumSHA256)
            };

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("UploadPart", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}/{Key}", "UploadPart", sw.ElapsedMilliseconds, bucketName, key);
            return result;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("UploadPart", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}/{Key}", "UploadPart", ex.ErrorCode, ex.Message, bucketName, key);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("UploadPart", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}/{Key}", "UploadPart", bucketName, key);
            throw;
        }
    }

    public async Task<MultipartUploadPart> UploadPartCopyAsync(
        UploadPartCopyStorageRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var copyRequest = new CopyPartRequest
        {
            DestinationBucket = request.BucketName,
            DestinationKey = request.Key,
            UploadId = request.UploadId,
            PartNumber = request.PartNumber,
            SourceBucket = request.SourceBucketName,
            SourceKey = request.SourceKey,
            SourceVersionId = request.SourceVersionId
        };

        if (!string.IsNullOrWhiteSpace(request.SourceIfMatchETag)) {
            copyRequest.ETagToMatch =
            [
                request.SourceIfMatchETag
            ];
        }

        if (!string.IsNullOrWhiteSpace(request.SourceIfNoneMatchETag)) {
            copyRequest.ETagsToNotMatch =
            [
                .. request.SourceIfNoneMatchETag.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            ];
        }

        if (request.SourceIfModifiedSinceUtc.HasValue) {
            copyRequest.ModifiedSinceDate = request.SourceIfModifiedSinceUtc.Value.UtcDateTime;
        }

        if (request.SourceIfUnmodifiedSinceUtc.HasValue) {
            copyRequest.UnmodifiedSinceDate = request.SourceIfUnmodifiedSinceUtc.Value.UtcDateTime;
        }

        long contentLength = 0;
        if (request.SourceRange is not null) {
            if (request.SourceRange.Start is null || request.SourceRange.End is null) {
                throw new NotSupportedException("UploadPartCopy source ranges must specify both start and end byte offsets.");
            }

            copyRequest.FirstByte = request.SourceRange.Start.Value;
            copyRequest.LastByte = request.SourceRange.End.Value;
            contentLength = request.SourceRange.End.Value - request.SourceRange.Start.Value + 1;
        }
        else {
            var sourceEntry = await HeadObjectAsync(
                request.SourceBucketName,
                request.SourceKey,
                request.SourceVersionId,
                request.SourceCustomerEncryption,
                cancellationToken).ConfigureAwait(false);
            contentLength = sourceEntry?.ContentLength ?? 0;
        }

        ApplyChecksumHeaders(copyRequest, request.ChecksumAlgorithm, request.Checksums);
        S3ServerSideEncryptionMapper.ApplyCopySourceCustomerEncryption(copyRequest, request.SourceCustomerEncryption);
        S3ServerSideEncryptionMapper.ApplyCustomerEncryption(copyRequest, request.DestinationCustomerEncryption);

        var response = await _s3.CopyPartAsync(copyRequest, cancellationToken).ConfigureAwait(false);

        return new MultipartUploadPart
        {
            PartNumber = response.PartNumber ?? request.PartNumber,
            ETag = response.ETag ?? string.Empty,
            ContentLength = contentLength,
            LastModifiedUtc = response.LastModified is not { } lastModifiedUtc
                ? DateTimeOffset.UtcNow
                : new DateTimeOffset(DateTime.SpecifyKind(lastModifiedUtc, DateTimeKind.Utc)),
            Checksums = BuildChecksums(
                response.ChecksumCRC32,
                response.ChecksumCRC32C,
                response.ChecksumCRC64NVME,
                response.ChecksumSHA1,
                response.ChecksumSHA256)
        };
    }

    public async Task<S3ObjectEntry> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        IReadOnlyList<MultipartUploadPart> parts,
        CancellationToken cancellationToken = default)
    {
        using var activity = S3StorageTelemetry.StartActivity("CompleteMultipartUpload", bucketName, key);
        var sw = Stopwatch.StartNew();
        _logger?.LogDebug("S3 {Operation} starting for {BucketName}/{Key}", "CompleteMultipartUpload", bucketName, key);

        try
        {
            var request = new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = key,
                UploadId = uploadId
            };

            request.PartETags = parts
                .OrderBy(static part => part.PartNumber)
                .Select(CreatePartETag)
                .ToList();

            var response = await _s3.CompleteMultipartUploadAsync(request, cancellationToken).ConfigureAwait(false);

            var result = new S3ObjectEntry(
                Key: key,
                ContentLength: 0,
                ContentType: null,
                ETag: response.ETag,
                LastModifiedUtc: DateTimeOffset.UtcNow,
                Metadata: null,
                VersionId: response.VersionId,
                Checksums: BuildChecksums(
                    response.ChecksumCRC32,
                    response.ChecksumCRC32C,
                    response.ChecksumCRC64NVME,
                    response.ChecksumSHA1,
                    response.ChecksumSHA256),
                ServerSideEncryption: S3ServerSideEncryptionMapper.ToInfo(
                    response.ServerSideEncryptionMethod,
                    response.ServerSideEncryptionKeyManagementServiceKeyId,
                    response.BucketKeyEnabled));

            sw.Stop();
            S3StorageTelemetry.RecordSuccess("CompleteMultipartUpload", sw.Elapsed);
            _logger?.LogDebug("S3 {Operation} completed in {ElapsedMs}ms for {BucketName}/{Key}", "CompleteMultipartUpload", sw.ElapsedMilliseconds, bucketName, key);
            return result;
        }
        catch (AmazonS3Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("CompleteMultipartUpload", sw.Elapsed, ex.ErrorCode);
            S3StorageTelemetry.MarkFailure(activity, ex.ErrorCode, ex.Message);
            _logger?.LogWarning(ex, "S3 {Operation} returned error {ErrorCode}: {Message} for {BucketName}/{Key}", "CompleteMultipartUpload", ex.ErrorCode, ex.Message, bucketName, key);
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            S3StorageTelemetry.RecordFailure("CompleteMultipartUpload", sw.Elapsed, "UnexpectedError");
            S3StorageTelemetry.MarkFailure(activity, "UnexpectedError", ex.Message);
            _logger?.LogError(ex, "S3 {Operation} failed unexpectedly for {BucketName}/{Key}", "CompleteMultipartUpload", bucketName, key);
            throw;
        }
    }

    public async Task AbortMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        var request = new AbortMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = key,
            UploadId = uploadId
        };

        await _s3.AbortMultipartUploadAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<S3MultipartUploadListPage> ListMultipartUploadsAsync(
        string bucketName,
        string? prefix,
        string? keyMarker,
        string? uploadIdMarker,
        int? maxUploads,
        CancellationToken cancellationToken = default)
    {
        var request = new ListMultipartUploadsRequest
        {
            BucketName = bucketName,
            Prefix = prefix,
            KeyMarker = keyMarker,
            UploadIdMarker = uploadIdMarker
        };

        if (maxUploads.HasValue)
            request.MaxUploads = Math.Min(maxUploads.Value, 1000);

        var response = await _s3.ListMultipartUploadsAsync(request, cancellationToken).ConfigureAwait(false);

        var entries = (response.MultipartUploads ?? [])
            .Select(upload => new MultipartUploadInfo
            {
                BucketName = bucketName,
                Key = upload.Key ?? string.Empty,
                UploadId = upload.UploadId ?? string.Empty,
                InitiatedAtUtc = ToDateTimeOffset(upload.Initiated),
                ChecksumAlgorithm = NormalizeChecksumAlgorithm(upload.ChecksumAlgorithm?.ToString())
            })
            .ToList();

        return new S3MultipartUploadListPage(
            entries,
            response.IsTruncated == true ? response.NextKeyMarker : null,
            response.IsTruncated == true ? response.NextUploadIdMarker : null);
    }

    public async Task<S3MultipartPartListPage> ListMultipartPartsAsync(
        string bucketName,
        string key,
        string uploadId,
        int? partNumberMarker,
        int? maxParts,
        CancellationToken cancellationToken = default)
    {
        var request = new ListPartsRequest
        {
            BucketName = bucketName,
            Key = key,
            UploadId = uploadId
        };

        if (partNumberMarker is > 0) {
            request.PartNumberMarker = partNumberMarker.Value.ToString(CultureInfo.InvariantCulture);
        }

        if (maxParts.HasValue) {
            request.MaxParts = Math.Min(maxParts.Value, 1000);
        }

        var response = await _s3.ListPartsAsync(request, cancellationToken).ConfigureAwait(false);

        var entries = (response.Parts ?? [])
            .Select(part => new MultipartUploadPart
            {
                PartNumber = part.PartNumber ?? 0,
                ETag = part.ETag ?? string.Empty,
                ContentLength = part.Size ?? 0,
                LastModifiedUtc = ToDateTimeOffset(part.LastModified),
                Checksums = BuildChecksums(
                    part.ChecksumCRC32,
                    part.ChecksumCRC32C,
                    part.ChecksumCRC64NVME,
                    part.ChecksumSHA1,
                    part.ChecksumSHA256)
            })
            .ToList();

        return new S3MultipartPartListPage(
            entries,
            response.IsTruncated == true ? response.NextPartNumberMarker : null);
    }

    // -------------------------------------------------------------------------
    // Bucket Tagging
    // -------------------------------------------------------------------------

    public async Task<BucketTaggingConfiguration> GetBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketTaggingRequest { BucketName = bucketName };
        var response = await _s3.GetBucketTaggingAsync(request, cancellationToken).ConfigureAwait(false);
        return new BucketTaggingConfiguration
        {
            BucketName = bucketName,
            Tags = (response.TagSet ?? []).ToDictionary(t => t.Key, t => t.Value, StringComparer.Ordinal)
        };
    }

    public async Task<BucketTaggingConfiguration> PutBucketTaggingAsync(PutBucketTaggingStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.PutBucketTaggingRequest
        {
            BucketName = request.BucketName,
            TagSet = request.Tags.Select(kvp => new Tag { Key = kvp.Key, Value = kvp.Value }).ToList()
        };
        await _s3.PutBucketTaggingAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketTaggingConfiguration
        {
            BucketName = request.BucketName,
            Tags = request.Tags
        };
    }

    public async Task DeleteBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.DeleteBucketTaggingRequest { BucketName = bucketName };
        await _s3.DeleteBucketTaggingAsync(request, cancellationToken).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Bucket Logging
    // -------------------------------------------------------------------------

    public async Task<BucketLoggingConfiguration> GetBucketLoggingAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketLoggingRequest { BucketName = bucketName };
        var response = await _s3.GetBucketLoggingAsync(request, cancellationToken).ConfigureAwait(false);
        return new BucketLoggingConfiguration
        {
            BucketName = bucketName,
            TargetBucket = response.BucketLoggingConfig?.TargetBucketName,
            TargetPrefix = response.BucketLoggingConfig?.TargetPrefix
        };
    }

    public async Task<BucketLoggingConfiguration> PutBucketLoggingAsync(PutBucketLoggingStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.PutBucketLoggingRequest
        {
            BucketName = request.BucketName,
            LoggingConfig = new S3BucketLoggingConfig
            {
                TargetBucketName = request.TargetBucket,
                TargetPrefix = request.TargetPrefix
            }
        };
        await _s3.PutBucketLoggingAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketLoggingConfiguration
        {
            BucketName = request.BucketName,
            TargetBucket = request.TargetBucket,
            TargetPrefix = request.TargetPrefix
        };
    }

    // -------------------------------------------------------------------------
    // Bucket Website
    // -------------------------------------------------------------------------

    public async Task<BucketWebsiteConfiguration> GetBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketWebsiteRequest { BucketName = bucketName };
        var response = await _s3.GetBucketWebsiteAsync(request, cancellationToken).ConfigureAwait(false);
        return new BucketWebsiteConfiguration
        {
            BucketName = bucketName,
            IndexDocumentSuffix = response.WebsiteConfiguration?.IndexDocumentSuffix,
            ErrorDocumentKey = response.WebsiteConfiguration?.ErrorDocument,
            RedirectAllRequestsTo = response.WebsiteConfiguration?.RedirectAllRequestsTo is { } redirect
                ? new BucketWebsiteRedirectAllRequestsTo
                {
                    HostName = redirect.HostName,
                    Protocol = redirect.Protocol
                }
                : null,
            RoutingRules = (response.WebsiteConfiguration?.RoutingRules ?? []).Select(r => new BucketWebsiteRoutingRule
            {
                Condition = r.Condition is not null
                    ? new BucketWebsiteRoutingRuleCondition
                    {
                        KeyPrefixEquals = r.Condition.KeyPrefixEquals,
                        HttpErrorCodeReturnedEquals = int.TryParse(r.Condition.HttpErrorCodeReturnedEquals, out var code) ? code : null
                    }
                    : null,
                Redirect = new BucketWebsiteRoutingRuleRedirect
                {
                    HostName = r.Redirect?.HostName,
                    Protocol = r.Redirect?.Protocol,
                    ReplaceKeyPrefixWith = r.Redirect?.ReplaceKeyPrefixWith,
                    ReplaceKeyWith = r.Redirect?.ReplaceKeyWith,
                    HttpRedirectCode = r.Redirect?.HttpRedirectCode is { } hrc
                        ? int.TryParse(hrc, out var hrcCode) ? hrcCode : null
                        : null
                }
            }).ToArray()
        };
    }

    public async Task<BucketWebsiteConfiguration> PutBucketWebsiteAsync(PutBucketWebsiteStorageRequest request, CancellationToken cancellationToken = default)
    {
        var websiteConfig = new WebsiteConfiguration
        {
            IndexDocumentSuffix = request.IndexDocumentSuffix,
            ErrorDocument = request.ErrorDocumentKey
        };
        if (request.RedirectAllRequestsTo is { } rart)
        {
            websiteConfig.RedirectAllRequestsTo = new RoutingRuleRedirect
            {
                HostName = rart.HostName,
                Protocol = rart.Protocol
            };
        }
        if (request.RoutingRules.Count > 0)
        {
            websiteConfig.RoutingRules = request.RoutingRules.Select(r => new RoutingRule
            {
                Condition = r.Condition is not null
                    ? new RoutingRuleCondition
                    {
                        KeyPrefixEquals = r.Condition.KeyPrefixEquals,
                        HttpErrorCodeReturnedEquals = r.Condition.HttpErrorCodeReturnedEquals?.ToString()
                    }
                    : null,
                Redirect = new RoutingRuleRedirect
                {
                    HostName = r.Redirect.HostName,
                    Protocol = r.Redirect.Protocol,
                    ReplaceKeyPrefixWith = r.Redirect.ReplaceKeyPrefixWith,
                    ReplaceKeyWith = r.Redirect.ReplaceKeyWith,
                    HttpRedirectCode = r.Redirect.HttpRedirectCode?.ToString()
                }
            }).ToList();
        }
        var sdkRequest = new Amazon.S3.Model.PutBucketWebsiteRequest
        {
            BucketName = request.BucketName,
            WebsiteConfiguration = websiteConfig
        };
        await _s3.PutBucketWebsiteAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketWebsiteConfiguration
        {
            BucketName = request.BucketName,
            IndexDocumentSuffix = request.IndexDocumentSuffix,
            ErrorDocumentKey = request.ErrorDocumentKey,
            RedirectAllRequestsTo = request.RedirectAllRequestsTo,
            RoutingRules = request.RoutingRules
        };
    }

    public async Task DeleteBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.DeleteBucketWebsiteRequest { BucketName = bucketName };
        await _s3.DeleteBucketWebsiteAsync(request, cancellationToken).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Bucket Request Payment
    // -------------------------------------------------------------------------

    public async Task<BucketRequestPaymentConfiguration> GetBucketRequestPaymentAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketRequestPaymentRequest { BucketName = bucketName };
        var response = await _s3.GetBucketRequestPaymentAsync(request, cancellationToken).ConfigureAwait(false);
        var payer = string.Equals(response.Payer, "Requester", StringComparison.OrdinalIgnoreCase)
            ? BucketPayer.Requester
            : BucketPayer.BucketOwner;
        return new BucketRequestPaymentConfiguration { BucketName = bucketName, Payer = payer };
    }

    public async Task<BucketRequestPaymentConfiguration> PutBucketRequestPaymentAsync(PutBucketRequestPaymentStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.PutBucketRequestPaymentRequest
        {
            BucketName = request.BucketName,
            RequestPaymentConfiguration = new RequestPaymentConfiguration
            {
                Payer = request.Payer == BucketPayer.Requester ? "Requester" : "BucketOwner"
            }
        };
        await _s3.PutBucketRequestPaymentAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketRequestPaymentConfiguration { BucketName = request.BucketName, Payer = request.Payer };
    }

    // -------------------------------------------------------------------------
    // Bucket Accelerate
    // -------------------------------------------------------------------------

    public async Task<BucketAccelerateConfiguration> GetBucketAccelerateAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new GetBucketAccelerateConfigurationRequest { BucketName = bucketName };
        var response = await _s3.GetBucketAccelerateConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        var status = string.Equals(response.Status, "Enabled", StringComparison.OrdinalIgnoreCase)
            ? Abstractions.Models.BucketAccelerateStatus.Enabled
            : Abstractions.Models.BucketAccelerateStatus.Suspended;
        return new BucketAccelerateConfiguration { BucketName = bucketName, Status = status };
    }

    public async Task<BucketAccelerateConfiguration> PutBucketAccelerateAsync(PutBucketAccelerateStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new PutBucketAccelerateConfigurationRequest
        {
            BucketName = request.BucketName,
            AccelerateConfiguration = new AccelerateConfiguration
            {
                Status = request.Status == Abstractions.Models.BucketAccelerateStatus.Enabled ? "Enabled" : "Suspended"
            }
        };
        await _s3.PutBucketAccelerateConfigurationAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketAccelerateConfiguration { BucketName = request.BucketName, Status = request.Status };
    }

    // -------------------------------------------------------------------------
    // Bucket Lifecycle
    // -------------------------------------------------------------------------

    public async Task<BucketLifecycleConfiguration> GetBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new GetLifecycleConfigurationRequest { BucketName = bucketName };
        var response = await _s3.GetLifecycleConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        return new BucketLifecycleConfiguration
        {
            BucketName = bucketName,
            Rules = (response.Configuration?.Rules ?? []).Select(MapLifecycleRule).ToArray()
        };
    }

    public async Task<BucketLifecycleConfiguration> PutBucketLifecycleAsync(PutBucketLifecycleStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new PutLifecycleConfigurationRequest
        {
            BucketName = request.BucketName,
            Configuration = new LifecycleConfiguration
            {
                Rules = request.Rules.Select(MapLifecycleRuleToSdk).ToList()
            }
        };
        await _s3.PutLifecycleConfigurationAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketLifecycleConfiguration { BucketName = request.BucketName, Rules = request.Rules };
    }

    public async Task DeleteBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new DeleteLifecycleConfigurationRequest { BucketName = bucketName };
        await _s3.DeleteLifecycleConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Bucket Replication
    // -------------------------------------------------------------------------

    public async Task<BucketReplicationConfiguration> GetBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketReplicationRequest { BucketName = bucketName };
        var response = await _s3.GetBucketReplicationAsync(request, cancellationToken).ConfigureAwait(false);
        return new BucketReplicationConfiguration
        {
            BucketName = bucketName,
            Role = response.Configuration?.Role,
            Rules = (response.Configuration?.Rules ?? []).Select(r => new BucketReplicationRule
            {
                Id = r.Id,
                Status = string.Equals(r.Status?.Value, "Enabled", StringComparison.OrdinalIgnoreCase)
                    ? BucketReplicationRuleStatus.Enabled
                    : BucketReplicationRuleStatus.Disabled,
                FilterPrefix = r.Filter?.Prefix,
                Destination = new BucketReplicationDestination
                {
                    Bucket = r.Destination?.BucketArn ?? string.Empty,
                    StorageClass = r.Destination?.StorageClass?.Value,
                    Account = r.Destination?.AccountId
                },
                Priority = r.Priority,
                DeleteMarkerReplication = string.Equals(r.DeleteMarkerReplication?.Status?.Value, "Enabled", StringComparison.OrdinalIgnoreCase)
            }).ToArray()
        };
    }

    public async Task<BucketReplicationConfiguration> PutBucketReplicationAsync(PutBucketReplicationStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.PutBucketReplicationRequest
        {
            BucketName = request.BucketName,
            Configuration = new Amazon.S3.Model.ReplicationConfiguration
            {
                Role = request.Role,
                Rules = request.Rules.Select(r => new Amazon.S3.Model.ReplicationRule
                {
                    Id = r.Id,
                    Status = r.Status == BucketReplicationRuleStatus.Enabled
                        ? ReplicationRuleStatus.Enabled
                        : ReplicationRuleStatus.Disabled,
                    Filter = new ReplicationRuleFilter { Prefix = r.FilterPrefix },
                    Destination = new Amazon.S3.Model.ReplicationDestination
                    {
                        BucketArn = r.Destination.Bucket,
                        StorageClass = !string.IsNullOrWhiteSpace(r.Destination.StorageClass)
                            ? new S3StorageClass(r.Destination.StorageClass)
                            : null,
                        AccountId = r.Destination.Account
                    },
                    Priority = r.Priority ?? 0,
                    DeleteMarkerReplication = new DeleteMarkerReplication
                    {
                        Status = r.DeleteMarkerReplication
                            ? DeleteMarkerReplicationStatus.Enabled
                            : DeleteMarkerReplicationStatus.Disabled
                    }
                }).ToList()
            }
        };
        await _s3.PutBucketReplicationAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketReplicationConfiguration
        {
            BucketName = request.BucketName,
            Role = request.Role,
            Rules = request.Rules
        };
    }

    public async Task DeleteBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.DeleteBucketReplicationRequest { BucketName = bucketName };
        await _s3.DeleteBucketReplicationAsync(request, cancellationToken).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Bucket Notifications
    // -------------------------------------------------------------------------

    public async Task<BucketNotificationConfiguration> GetBucketNotificationConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketNotificationRequest { BucketName = bucketName };
        var response = await _s3.GetBucketNotificationAsync(request, cancellationToken).ConfigureAwait(false);
        return new BucketNotificationConfiguration
        {
            BucketName = bucketName,
            TopicConfigurations = (response.TopicConfigurations ?? []).Select(t => new BucketNotificationTopicConfiguration
            {
                Id = t.Id,
                TopicArn = t.Topic ?? string.Empty,
                Events = (t.Events ?? []).Select(e => e.Value).ToArray(),
                Filter = MapNotificationFilter(t.Filter)
            }).ToArray(),
            QueueConfigurations = (response.QueueConfigurations ?? []).Select(q => new BucketNotificationQueueConfiguration
            {
                Id = q.Id,
                QueueArn = q.Queue ?? string.Empty,
                Events = (q.Events ?? []).Select(e => e.Value).ToArray(),
                Filter = MapNotificationFilter(q.Filter)
            }).ToArray(),
            LambdaFunctionConfigurations = (response.LambdaFunctionConfigurations ?? []).Select(l => new BucketNotificationLambdaConfiguration
            {
                Id = l.Id,
                LambdaFunctionArn = l.FunctionArn ?? string.Empty,
                Events = (l.Events ?? []).Select(e => e.Value).ToArray(),
                Filter = MapNotificationFilter(l.Filter)
            }).ToArray()
        };
    }

    public async Task<BucketNotificationConfiguration> PutBucketNotificationConfigurationAsync(PutBucketNotificationConfigurationStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.PutBucketNotificationRequest
        {
            BucketName = request.BucketName,
            TopicConfigurations = request.TopicConfigurations.Select(t => new TopicConfiguration
            {
                Id = t.Id,
                Topic = t.TopicArn,
                Events = t.Events.Select(e => new EventType(e)).ToList(),
                Filter = MapNotificationFilterToSdk(t.Filter)
            }).ToList(),
            QueueConfigurations = request.QueueConfigurations.Select(q => new QueueConfiguration
            {
                Id = q.Id,
                Queue = q.QueueArn,
                Events = q.Events.Select(e => new EventType(e)).ToList(),
                Filter = MapNotificationFilterToSdk(q.Filter)
            }).ToList(),
            LambdaFunctionConfigurations = request.LambdaFunctionConfigurations.Select(l => new LambdaFunctionConfiguration
            {
                Id = l.Id,
                FunctionArn = l.LambdaFunctionArn,
                Events = l.Events.Select(e => new EventType(e)).ToList(),
                Filter = MapNotificationFilterToSdk(l.Filter)
            }).ToList()
        };
        await _s3.PutBucketNotificationAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketNotificationConfiguration
        {
            BucketName = request.BucketName,
            TopicConfigurations = request.TopicConfigurations,
            QueueConfigurations = request.QueueConfigurations,
            LambdaFunctionConfigurations = request.LambdaFunctionConfigurations
        };
    }

    // -------------------------------------------------------------------------
    // Object Lock Configuration (bucket-level)
    // -------------------------------------------------------------------------

    public async Task<ObjectLockConfiguration> GetObjectLockConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetObjectLockConfigurationRequest { BucketName = bucketName };
        var response = await _s3.GetObjectLockConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        var cfg = response.ObjectLockConfiguration;
        return new ObjectLockConfiguration
        {
            BucketName = bucketName,
            ObjectLockEnabled = cfg?.ObjectLockEnabled == ObjectLockEnabled.Enabled,
            DefaultRetention = cfg?.Rule?.DefaultRetention is { } dr
                ? new ObjectLockDefaultRetention
                {
                    Mode = S3ObjectLockMapper.ToRetentionMode(dr.Mode) ?? ObjectRetentionMode.Compliance,
                    Days = dr.Days > 0 ? dr.Days : null,
                    Years = dr.Years > 0 ? dr.Years : null
                }
                : null
        };
    }

    public async Task<ObjectLockConfiguration> PutObjectLockConfigurationAsync(PutObjectLockConfigurationStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkConfig = new Amazon.S3.Model.ObjectLockConfiguration
        {
            ObjectLockEnabled = request.ObjectLockEnabled ? ObjectLockEnabled.Enabled : new ObjectLockEnabled("Disabled")
        };
        if (request.DefaultRetention is { } retention)
        {
            sdkConfig.Rule = new ObjectLockRule
            {
                DefaultRetention = new DefaultRetention
                {
                    Mode = retention.Mode == ObjectRetentionMode.Governance
                        ? ObjectLockRetentionMode.Governance
                        : ObjectLockRetentionMode.Compliance,
                    Days = retention.Days ?? 0,
                    Years = retention.Years ?? 0
                }
            };
        }
        var sdkRequest = new Amazon.S3.Model.PutObjectLockConfigurationRequest
        {
            BucketName = request.BucketName,
            ObjectLockConfiguration = sdkConfig
        };
        await _s3.PutObjectLockConfigurationAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new ObjectLockConfiguration
        {
            BucketName = request.BucketName,
            ObjectLockEnabled = request.ObjectLockEnabled,
            DefaultRetention = request.DefaultRetention
        };
    }

    // -------------------------------------------------------------------------
    // Bucket Analytics
    // -------------------------------------------------------------------------

    public async Task<BucketAnalyticsConfiguration> GetBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketAnalyticsConfigurationRequest { BucketName = bucketName, AnalyticsId = id };
        var response = await _s3.GetBucketAnalyticsConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        var cfg = response.AnalyticsConfiguration;
        return MapAnalyticsConfigurationFromSdk(bucketName, cfg);
    }

    public async Task<BucketAnalyticsConfiguration> PutBucketAnalyticsConfigurationAsync(PutBucketAnalyticsConfigurationStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkConfig = new Amazon.S3.Model.AnalyticsConfiguration
        {
            AnalyticsId = request.Id,
            AnalyticsFilter = new AnalyticsFilter { AnalyticsFilterPredicate = !string.IsNullOrWhiteSpace(request.FilterPrefix) ? new AnalyticsPrefixPredicate(request.FilterPrefix) : null },
            StorageClassAnalysis = request.StorageClassAnalysis is { DataExport: { } dataExport }
                ? new Amazon.S3.Model.StorageClassAnalysis
                {
                    DataExport = new StorageClassAnalysisDataExport
                    {
                        OutputSchemaVersion = new StorageClassAnalysisSchemaVersion(dataExport.OutputSchemaVersion),
                        Destination = dataExport.Destination is { } dest
                            ? new AnalyticsExportDestination
                            {
                                S3BucketDestination = new AnalyticsS3BucketDestination
                                {
                                    Format = new AnalyticsS3ExportFileFormat(dest.Format),
                                    BucketAccountId = dest.BucketAccountId,
                                    BucketName = dest.Bucket,
                                    Prefix = dest.Prefix
                                }
                            }
                            : null
                    }
                }
                : null
        };
        var sdkRequest = new Amazon.S3.Model.PutBucketAnalyticsConfigurationRequest
        {
            BucketName = request.BucketName,
            AnalyticsConfiguration = sdkConfig
        };
        await _s3.PutBucketAnalyticsConfigurationAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketAnalyticsConfiguration
        {
            BucketName = request.BucketName,
            Id = request.Id,
            FilterPrefix = request.FilterPrefix,
            FilterTags = request.FilterTags,
            StorageClassAnalysis = request.StorageClassAnalysis
        };
    }

    public async Task DeleteBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.DeleteBucketAnalyticsConfigurationRequest { BucketName = bucketName, AnalyticsId = id };
        await _s3.DeleteBucketAnalyticsConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<BucketAnalyticsConfiguration>> ListBucketAnalyticsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.ListBucketAnalyticsConfigurationsRequest { BucketName = bucketName };
        var response = await _s3.ListBucketAnalyticsConfigurationsAsync(request, cancellationToken).ConfigureAwait(false);
        return response.AnalyticsConfigurationList
            .Select(cfg => MapAnalyticsConfigurationFromSdk(bucketName, cfg))
            .ToList();
    }

    // -------------------------------------------------------------------------
    // Bucket Metrics
    // -------------------------------------------------------------------------

    public async Task<BucketMetricsConfiguration> GetBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketMetricsConfigurationRequest { BucketName = bucketName, MetricsId = id };
        var response = await _s3.GetBucketMetricsConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        var cfg = response.MetricsConfiguration;
        return MapMetricsConfigurationFromSdk(bucketName, cfg);
    }

    public async Task<BucketMetricsConfiguration> PutBucketMetricsConfigurationAsync(PutBucketMetricsConfigurationStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkConfig = new Amazon.S3.Model.MetricsConfiguration
        {
            MetricsId = request.Id
        };
        if (request.Filter is { } filter)
        {
            sdkConfig.MetricsFilter = new MetricsFilter
            {
                MetricsFilterPredicate = !string.IsNullOrWhiteSpace(filter.Prefix)
                    ? new MetricsPrefixPredicate(filter.Prefix)
                    : null
            };
        }
        var sdkRequest = new Amazon.S3.Model.PutBucketMetricsConfigurationRequest
        {
            BucketName = request.BucketName,
            MetricsConfiguration = sdkConfig
        };
        await _s3.PutBucketMetricsConfigurationAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketMetricsConfiguration
        {
            BucketName = request.BucketName,
            Id = request.Id,
            Filter = request.Filter
        };
    }

    public async Task DeleteBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.DeleteBucketMetricsConfigurationRequest { BucketName = bucketName, MetricsId = id };
        await _s3.DeleteBucketMetricsConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<BucketMetricsConfiguration>> ListBucketMetricsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.ListBucketMetricsConfigurationsRequest { BucketName = bucketName };
        var response = await _s3.ListBucketMetricsConfigurationsAsync(request, cancellationToken).ConfigureAwait(false);
        return response.MetricsConfigurationList
            .Select(cfg => MapMetricsConfigurationFromSdk(bucketName, cfg))
            .ToList();
    }

    // -------------------------------------------------------------------------
    // Bucket Inventory
    // -------------------------------------------------------------------------

    public async Task<BucketInventoryConfiguration> GetBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketInventoryConfigurationRequest { BucketName = bucketName, InventoryId = id };
        var response = await _s3.GetBucketInventoryConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        var cfg = response.InventoryConfiguration;
        return MapInventoryConfigurationFromSdk(bucketName, cfg);
    }

    public async Task<BucketInventoryConfiguration> PutBucketInventoryConfigurationAsync(PutBucketInventoryConfigurationStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkConfig = new Amazon.S3.Model.InventoryConfiguration
        {
            InventoryId = request.Id,
            IsEnabled = request.IsEnabled,
            IncludedObjectVersions = new InventoryIncludedObjectVersions(request.IncludedObjectVersions),
            InventoryOptionalFields = request.OptionalFields.Select(f => new InventoryOptionalField(f)).ToList(),
            Schedule = request.Schedule is { } sched
                ? new InventorySchedule { Frequency = new InventoryFrequency(sched.Frequency) }
                : null,
            InventoryFilter = request.Filter is { Prefix: { } prefix }
                ? new InventoryFilter { InventoryFilterPredicate = new InventoryPrefixPredicate(prefix) }
                : null,
            Destination = request.Destination is { S3BucketDestination: { } s3Dest }
                ? new InventoryDestination
                {
                    S3BucketDestination = new InventoryS3BucketDestination
                    {
                        InventoryFormat = new InventoryFormat(s3Dest.Format),
                        AccountId = s3Dest.AccountId,
                        BucketName = s3Dest.Bucket,
                        Prefix = s3Dest.Prefix
                    }
                }
                : null
        };
        var sdkRequest = new Amazon.S3.Model.PutBucketInventoryConfigurationRequest
        {
            BucketName = request.BucketName,
            InventoryConfiguration = sdkConfig
        };
        await _s3.PutBucketInventoryConfigurationAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketInventoryConfiguration
        {
            BucketName = request.BucketName,
            Id = request.Id,
            IsEnabled = request.IsEnabled,
            Destination = request.Destination,
            Schedule = request.Schedule,
            Filter = request.Filter,
            IncludedObjectVersions = request.IncludedObjectVersions,
            OptionalFields = request.OptionalFields
        };
    }

    public async Task DeleteBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.DeleteBucketInventoryConfigurationRequest { BucketName = bucketName, InventoryId = id };
        await _s3.DeleteBucketInventoryConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<BucketInventoryConfiguration>> ListBucketInventoryConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.ListBucketInventoryConfigurationsRequest { BucketName = bucketName };
        var response = await _s3.ListBucketInventoryConfigurationsAsync(request, cancellationToken).ConfigureAwait(false);
        return response.InventoryConfigurationList
            .Select(cfg => MapInventoryConfigurationFromSdk(bucketName, cfg))
            .ToList();
    }

    // -------------------------------------------------------------------------
    // Bucket Intelligent-Tiering
    // -------------------------------------------------------------------------

    public async Task<BucketIntelligentTieringConfiguration> GetBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.GetBucketIntelligentTieringConfigurationRequest { BucketName = bucketName, IntelligentTieringId = id };
        var response = await _s3.GetBucketIntelligentTieringConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
        var cfg = response.IntelligentTieringConfiguration;
        return MapIntelligentTieringConfigurationFromSdk(bucketName, cfg);
    }

    public async Task<BucketIntelligentTieringConfiguration> PutBucketIntelligentTieringConfigurationAsync(PutBucketIntelligentTieringConfigurationStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkConfig = new IntelligentTieringConfiguration
        {
            IntelligentTieringId = request.Id,
            Status = string.Equals(request.Status, "Enabled", StringComparison.OrdinalIgnoreCase)
                ? IntelligentTieringStatus.Enabled
                : IntelligentTieringStatus.Disabled,
            Tierings = request.Tierings.Select(t => new Tiering
            {
                AccessTier = new IntelligentTieringAccessTier(t.AccessTier),
                Days = t.Days
            }).ToList()
        };
        if (request.Filter is { } filter)
        {
            sdkConfig.IntelligentTieringFilter = new IntelligentTieringFilter
            {
                IntelligentTieringFilterPredicate = !string.IsNullOrWhiteSpace(filter.Prefix)
                    ? new IntelligentTieringPrefixPredicate(filter.Prefix)
                    : null
            };
        }
        var sdkRequest = new Amazon.S3.Model.PutBucketIntelligentTieringConfigurationRequest
        {
            BucketName = request.BucketName,
            IntelligentTieringConfiguration = sdkConfig
        };
        await _s3.PutBucketIntelligentTieringConfigurationAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new BucketIntelligentTieringConfiguration
        {
            BucketName = request.BucketName,
            Id = request.Id,
            Status = request.Status,
            Filter = request.Filter,
            Tierings = request.Tierings
        };
    }

    public async Task DeleteBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.DeleteBucketIntelligentTieringConfigurationRequest { BucketName = bucketName, IntelligentTieringId = id };
        await _s3.DeleteBucketIntelligentTieringConfigurationAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<BucketIntelligentTieringConfiguration>> ListBucketIntelligentTieringConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var request = new Amazon.S3.Model.ListBucketIntelligentTieringConfigurationsRequest { BucketName = bucketName };
        var response = await _s3.ListBucketIntelligentTieringConfigurationsAsync(request, cancellationToken).ConfigureAwait(false);
        return response.IntelligentTieringConfigurationList
            .Select(cfg => MapIntelligentTieringConfigurationFromSdk(bucketName, cfg))
            .ToList();
    }

    // -------------------------------------------------------------------------
    // Object Lock Write Operations
    // -------------------------------------------------------------------------

    public async Task<ObjectRetentionInfo> PutObjectRetentionAsync(PutObjectRetentionStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.PutObjectRetentionRequest
        {
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId,
            BypassGovernanceRetention = request.BypassGovernanceRetention,
            Retention = new ObjectLockRetention
            {
                Mode = request.Mode switch
                {
                    ObjectRetentionMode.Governance => ObjectLockRetentionMode.Governance,
                    ObjectRetentionMode.Compliance => ObjectLockRetentionMode.Compliance,
                    _ => null
                },
                RetainUntilDate = request.RetainUntilDateUtc?.UtcDateTime ?? default
            }
        };
        await _s3.PutObjectRetentionAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new ObjectRetentionInfo
        {
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId,
            Mode = request.Mode,
            RetainUntilDateUtc = request.RetainUntilDateUtc
        };
    }

    public async Task<ObjectLegalHoldInfo> PutObjectLegalHoldAsync(PutObjectLegalHoldStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.PutObjectLegalHoldRequest
        {
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId,
            LegalHold = new ObjectLockLegalHold
            {
                Status = request.Status == ObjectLegalHoldStatus.On
                    ? ObjectLockLegalHoldStatus.On
                    : ObjectLockLegalHoldStatus.Off
            }
        };
        await _s3.PutObjectLegalHoldAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new ObjectLegalHoldInfo
        {
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId,
            Status = request.Status
        };
    }

    // -------------------------------------------------------------------------
    // Restore Object
    // -------------------------------------------------------------------------

    public async Task<S3RestoreObjectResult> RestoreObjectAsync(RestoreObjectStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.RestoreObjectRequest
        {
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId,
            Days = request.Days ?? 1
        };
        if (!string.IsNullOrWhiteSpace(request.GlacierTier))
        {
            sdkRequest.Tier = request.GlacierTier switch
            {
                "Expedited" => GlacierJobTier.Expedited,
                "Standard" => GlacierJobTier.Standard,
                "Bulk" => GlacierJobTier.Bulk,
                _ => GlacierJobTier.Standard
            };
        }
        var response = await _s3.RestoreObjectAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        return new S3RestoreObjectResult(
            IsAlreadyRestored: (int)response.HttpStatusCode == 200,
            RestoreOutputPath: response.RestoreOutputPath);
    }

    // -------------------------------------------------------------------------
    // Select Object Content
    // -------------------------------------------------------------------------

    public async Task<S3SelectObjectContentResult> SelectObjectContentAsync(SelectObjectContentStorageRequest request, CancellationToken cancellationToken = default)
    {
        var sdkRequest = new Amazon.S3.Model.SelectObjectContentRequest
        {
            BucketName = request.BucketName,
            Key = request.Key,
            Expression = request.Expression,
            ExpressionType = new ExpressionType(request.ExpressionType),
            InputSerialization = BuildInputSerialization(request),
            OutputSerialization = BuildOutputSerialization(request)
        };
        var response = await _s3.SelectObjectContentAsync(sdkRequest, cancellationToken).ConfigureAwait(false);
        var outputStream = new MemoryStream();
        if (response.Payload is { } payload)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            payload.RecordsEventReceived += (_, args) =>
            {
                if (args.EventStreamEvent?.Payload is { } recordPayload)
                    recordPayload.CopyTo(outputStream);
            };
            payload.EndEventReceived += (_, _) => tcs.TrySetResult();
            payload.ExceptionReceived += (_, args) => tcs.TrySetException(args.EventStreamException);
            payload.StartProcessing();
            await tcs.Task.ConfigureAwait(false);
            outputStream.Position = 0;
        }
        return new S3SelectObjectContentResult(
            EventStream: outputStream,
            ContentType: "application/octet-stream");
    }

    // -------------------------------------------------------------------------
    // Object tags
    // -------------------------------------------------------------------------

    public async Task<IReadOnlyDictionary<string, string>> GetObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default)
    {
        var request = new GetObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        };

        var response = await _s3.GetObjectTaggingAsync(request, cancellationToken).ConfigureAwait(false);

        return response.Tagging
            .ToDictionary(t => t.Key, t => t.Value, StringComparer.Ordinal);
    }

    public async Task PutObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken = default)
    {
        var request = new PutObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId,
            Tagging = new Tagging
            {
                TagSet = tags.Select(kvp => new Tag { Key = kvp.Key, Value = kvp.Value }).ToList()
            }
        };

        await _s3.PutObjectTaggingAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default)
    {
        var request = new DeleteObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        };

        await _s3.DeleteObjectTaggingAsync(request, cancellationToken).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // Mapping helpers for new bucket config operations
    // -------------------------------------------------------------------------

    private static BucketLifecycleRule MapLifecycleRule(LifecycleRule rule) => new()
    {
        Id = rule.Id,
        FilterPrefix = rule.Filter?.LifecycleFilterPredicate is LifecyclePrefixPredicate pp ? pp.Prefix : null,
        Status = string.Equals(rule.Status?.Value, "Enabled", StringComparison.OrdinalIgnoreCase)
            ? BucketLifecycleRuleStatus.Enabled
            : BucketLifecycleRuleStatus.Disabled,
        ExpirationDays = rule.Expiration?.Days > 0 ? rule.Expiration.Days : null,
        ExpirationDate = rule.Expiration?.Date is { } date && date != default
            ? new DateTimeOffset(DateTime.SpecifyKind(date, DateTimeKind.Utc))
            : null,
        ExpiredObjectDeleteMarker = rule.Expiration?.ExpiredObjectDeleteMarker,
        NoncurrentVersionExpirationDays = rule.NoncurrentVersionExpiration?.NoncurrentDays > 0
            ? rule.NoncurrentVersionExpiration.NoncurrentDays
            : null,
        AbortIncompleteMultipartUploadDaysAfterInitiation = rule.AbortIncompleteMultipartUpload?.DaysAfterInitiation > 0
            ? rule.AbortIncompleteMultipartUpload.DaysAfterInitiation
            : null,
        Transitions = (rule.Transitions ?? []).Select(t => new BucketLifecycleTransition
        {
            Days = t.Days > 0 ? t.Days : null,
            Date = t.Date is { } td && td != default
                ? new DateTimeOffset(DateTime.SpecifyKind(td, DateTimeKind.Utc))
                : null,
            StorageClass = t.StorageClass?.Value ?? string.Empty
        }).ToArray(),
        NoncurrentVersionTransitions = (rule.NoncurrentVersionTransitions ?? []).Select(t => new BucketLifecycleNoncurrentVersionTransition
        {
            NoncurrentDays = t.NoncurrentDays > 0 ? t.NoncurrentDays : null,
            StorageClass = t.StorageClass?.Value ?? string.Empty
        }).ToArray()
    };

    private static LifecycleRule MapLifecycleRuleToSdk(BucketLifecycleRule rule)
    {
        var sdkRule = new LifecycleRule
        {
            Id = rule.Id,
            Status = rule.Status == BucketLifecycleRuleStatus.Enabled
                ? LifecycleRuleStatus.Enabled
                : LifecycleRuleStatus.Disabled,
            Filter = new LifecycleFilter
            {
                Prefix = rule.FilterPrefix
            },
            Expiration = new LifecycleRuleExpiration
            {
                Days = rule.ExpirationDays ?? 0,
                Date = rule.ExpirationDate?.UtcDateTime,
                ExpiredObjectDeleteMarker = rule.ExpiredObjectDeleteMarker ?? false
            },
            Transitions = rule.Transitions.Select(t => new LifecycleTransition
            {
                Days = t.Days ?? 0,
                Date = t.Date?.UtcDateTime,
                StorageClass = new S3StorageClass(t.StorageClass)
            }).ToList(),
            NoncurrentVersionTransitions = rule.NoncurrentVersionTransitions.Select(t => new LifecycleRuleNoncurrentVersionTransition
            {
                NoncurrentDays = t.NoncurrentDays ?? 0,
                StorageClass = new S3StorageClass(t.StorageClass)
            }).ToList()
        };
        if (rule.NoncurrentVersionExpirationDays.HasValue)
        {
            sdkRule.NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration
            {
                NoncurrentDays = rule.NoncurrentVersionExpirationDays.Value
            };
        }
        if (rule.AbortIncompleteMultipartUploadDaysAfterInitiation.HasValue)
        {
            sdkRule.AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload
            {
                DaysAfterInitiation = rule.AbortIncompleteMultipartUploadDaysAfterInitiation.Value
            };
        }
        return sdkRule;
    }

    private static BucketNotificationFilter? MapNotificationFilter(Filter? filter)
    {
        if (filter?.S3KeyFilter?.FilterRules is not { Count: > 0 } rules)
            return null;
        return new BucketNotificationFilter
        {
            KeyFilterRules = rules.Select(r => new BucketNotificationFilterRule
            {
                Name = r.Name ?? string.Empty,
                Value = r.Value ?? string.Empty
            }).ToArray()
        };
    }

    private static Filter? MapNotificationFilterToSdk(BucketNotificationFilter? filter)
    {
        if (filter is null || filter.KeyFilterRules.Count == 0)
            return null;
        return new Filter
        {
            S3KeyFilter = new S3KeyFilter
            {
                FilterRules = filter.KeyFilterRules.Select(r => new FilterRule
                {
                    Name = new FilterRuleName(r.Name),
                    Value = r.Value
                }).ToList()
            }
        };
    }

    private static BucketAnalyticsConfiguration MapAnalyticsConfigurationFromSdk(string bucketName, AnalyticsConfiguration cfg) => new()
    {
        BucketName = bucketName,
        Id = cfg.AnalyticsId ?? string.Empty,
        FilterPrefix = cfg.AnalyticsFilter?.AnalyticsFilterPredicate is AnalyticsPrefixPredicate ap ? ap.Prefix : null,
        StorageClassAnalysis = cfg.StorageClassAnalysis is { } sca
            ? new BucketAnalyticsStorageClassAnalysis
            {
                DataExport = sca.DataExport is { } de
                    ? new BucketAnalyticsDataExport
                    {
                        OutputSchemaVersion = de.OutputSchemaVersion?.Value ?? "V_1",
                        Destination = de.Destination?.S3BucketDestination is { } s3d
                            ? new BucketAnalyticsS3BucketDestination
                            {
                                Format = s3d.Format ?? "CSV",
                                BucketAccountId = s3d.BucketAccountId,
                                Bucket = s3d.BucketName ?? string.Empty,
                                Prefix = s3d.Prefix
                            }
                            : null
                    }
                    : null
            }
            : null
    };

    private static BucketMetricsConfiguration MapMetricsConfigurationFromSdk(string bucketName, MetricsConfiguration cfg) => new()
    {
        BucketName = bucketName,
        Id = cfg.MetricsId ?? string.Empty,
        Filter = cfg.MetricsFilter?.MetricsFilterPredicate is MetricsPrefixPredicate mp
            ? new BucketMetricsFilter { Prefix = mp.Prefix }
            : null
    };

    private static BucketInventoryConfiguration MapInventoryConfigurationFromSdk(string bucketName, InventoryConfiguration cfg) => new()
    {
        BucketName = bucketName,
        Id = cfg.InventoryId ?? string.Empty,
        IsEnabled = cfg.IsEnabled ?? false,
        IncludedObjectVersions = cfg.IncludedObjectVersions?.Value ?? "All",
        OptionalFields = (cfg.InventoryOptionalFields ?? []).Select(f => f.Value).ToArray(),
        Schedule = cfg.Schedule is { } sched
            ? new BucketInventorySchedule { Frequency = sched.Frequency?.Value ?? "Daily" }
            : null,
        Filter = cfg.InventoryFilter?.InventoryFilterPredicate is InventoryPrefixPredicate ip
            ? new BucketInventoryFilter { Prefix = ip.Prefix }
            : null,
        Destination = cfg.Destination?.S3BucketDestination is { } s3d
            ? new BucketInventoryDestination
            {
                S3BucketDestination = new BucketInventoryS3BucketDestination
                {
                    Format = s3d.InventoryFormat?.Value ?? "CSV",
                    AccountId = s3d.AccountId,
                    Bucket = s3d.BucketName ?? string.Empty,
                    Prefix = s3d.Prefix
                }
            }
            : null
    };

    private static BucketIntelligentTieringConfiguration MapIntelligentTieringConfigurationFromSdk(string bucketName, IntelligentTieringConfiguration cfg) => new()
    {
        BucketName = bucketName,
        Id = cfg.IntelligentTieringId ?? string.Empty,
        Status = cfg.Status?.Value ?? "Enabled",
        Filter = cfg.IntelligentTieringFilter?.IntelligentTieringFilterPredicate is IntelligentTieringPrefixPredicate itp
            ? new BucketIntelligentTieringFilter { Prefix = itp.Prefix }
            : null,
        Tierings = (cfg.Tierings ?? []).Select(t => new BucketIntelligentTiering
        {
            AccessTier = t.AccessTier?.Value ?? string.Empty,
            Days = t.Days ?? 0
        }).ToArray()
    };

    private static InputSerialization BuildInputSerialization(SelectObjectContentStorageRequest request)
    {
        var input = new InputSerialization();
        if (request.InputSerializationJson is not null)
            input.JSON = new JSONInput { JsonType = new JsonType(request.InputSerializationJson) };
        else if (request.InputSerializationCsv is not null)
            input.CSV = new CSVInput();
        else if (request.InputSerializationParquet is not null)
            input.Parquet = new ParquetInput();
        return input;
    }

    private static OutputSerialization BuildOutputSerialization(SelectObjectContentStorageRequest request)
    {
        var output = new OutputSerialization();
        if (request.OutputSerializationJson is not null)
            output.JSON = new JSONOutput();
        else if (request.OutputSerializationCsv is not null)
            output.CSV = new CSVOutput();
        return output;
    }

    public void Dispose() => _s3.Dispose();

    private static DateTimeOffset ToDateTimeOffset(DateTime? value)
        => value.HasValue ? new DateTimeOffset(DateTime.SpecifyKind(value.Value, DateTimeKind.Utc)) : DateTimeOffset.UtcNow;

    private static DateTimeOffset? ToNullableDateTimeOffset(DateTime? value)
        => value.HasValue ? new DateTimeOffset(DateTime.SpecifyKind(value.Value, DateTimeKind.Utc)) : null;

    private static DateTimeOffset? ParseExpiresString(string? rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
            return null;

        return DateTimeOffset.TryParse(
            rawValue,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var parsedValue)
            ? parsedValue
            : null;
    }

    private static IReadOnlyDictionary<string, string>? BuildMetadataDictionary(MetadataCollection? metadata)
    {
        if (metadata is null || metadata.Count == 0)
            return null;

        var dict = new Dictionary<string, string>(metadata.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var key in metadata.Keys)
            dict[key] = metadata[key];

        return dict;
    }

    private static IReadOnlyDictionary<string, string>? BuildChecksums(
        string? checksumCRC32,
        string? checksumCRC32C,
        string? checksumCRC64NVME,
        string? checksumSHA1,
        string? checksumSHA256)
    {
        Dictionary<string, string>? checksums = null;

        AddChecksum(ref checksums, "crc32", checksumCRC32);
        AddChecksum(ref checksums, "crc32c", checksumCRC32C);
        AddChecksum(ref checksums, "crc64nvme", checksumCRC64NVME);
        AddChecksum(ref checksums, "sha1", checksumSHA1);
        AddChecksum(ref checksums, "sha256", checksumSHA256);

        return checksums;
    }

    private static void AddChecksum(ref Dictionary<string, string>? checksums, string algorithm, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return;

        checksums ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        checksums[algorithm] = value;
    }

    private static List<Tag>? BuildTagSet(IReadOnlyDictionary<string, string>? tags)
    {
        return tags is null || tags.Count == 0
            ? null
            : tags.Select(static pair => new Tag
            {
                Key = pair.Key,
                Value = pair.Value
            }).ToList();
    }

    private static void ApplyStandardObjectHeaders(
        HeadersCollection headers,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc)
    {
        headers.CacheControl = cacheControl;
        headers.ContentDisposition = contentDisposition;
        headers.ContentEncoding = contentEncoding;
        headers["Content-Language"] = contentLanguage;
        headers.Expires = expiresUtc?.UtcDateTime;
    }

    private static string? GetHeaderValue(HeadersCollection headers, string headerName)
    {
        var value = headers[headerName];
        return string.IsNullOrWhiteSpace(value)
            ? null
            : value;
    }

    private static void ApplyChecksumHeaders(PutObjectRequest request, string? checksumAlgorithm, IReadOnlyDictionary<string, string>? checksums)
    {
        var sdkChecksumAlgorithm = MapChecksumAlgorithm(checksumAlgorithm);
        if (sdkChecksumAlgorithm is not null)
            request.ChecksumAlgorithm = sdkChecksumAlgorithm;

        if (TryGetChecksumValue(checksums, "crc32", out var checksumCRC32))
            request.ChecksumCRC32 = checksumCRC32;
        if (TryGetChecksumValue(checksums, "crc32c", out var checksumCRC32C))
            request.ChecksumCRC32C = checksumCRC32C;
        if (TryGetChecksumValue(checksums, "crc64nvme", out var checksumCRC64NVME))
            request.ChecksumCRC64NVME = checksumCRC64NVME;
        if (TryGetChecksumValue(checksums, "sha1", out var checksumSHA1))
            request.ChecksumSHA1 = checksumSHA1;
        if (TryGetChecksumValue(checksums, "sha256", out var checksumSHA256))
            request.ChecksumSHA256 = checksumSHA256;
    }

    private static void ApplyChecksumHeaders(CopyObjectRequest request, string? checksumAlgorithm, IReadOnlyDictionary<string, string>? checksums)
    {
        var sdkChecksumAlgorithm = MapChecksumAlgorithm(checksumAlgorithm);
        if (sdkChecksumAlgorithm is not null)
            request.ChecksumAlgorithm = sdkChecksumAlgorithm;

        if (TryGetChecksumValue(checksums, "crc32", out var checksumCRC32))
            request.Headers["x-amz-checksum-crc32"] = checksumCRC32;
        if (TryGetChecksumValue(checksums, "crc32c", out var checksumCRC32C))
            request.Headers["x-amz-checksum-crc32c"] = checksumCRC32C;
        if (TryGetChecksumValue(checksums, "crc64nvme", out var checksumCRC64NVME))
            request.Headers["x-amz-checksum-crc64nvme"] = checksumCRC64NVME;
        if (TryGetChecksumValue(checksums, "sha1", out var checksumSHA1))
            request.Headers["x-amz-checksum-sha1"] = checksumSHA1;
        if (TryGetChecksumValue(checksums, "sha256", out var checksumSHA256))
            request.Headers["x-amz-checksum-sha256"] = checksumSHA256;
    }

    private static void ApplyChecksumHeaders(UploadPartRequest request, string? checksumAlgorithm, IReadOnlyDictionary<string, string>? checksums)
    {
        var sdkChecksumAlgorithm = MapChecksumAlgorithm(checksumAlgorithm);
        if (sdkChecksumAlgorithm is not null)
            request.ChecksumAlgorithm = sdkChecksumAlgorithm;

        if (TryGetChecksumValue(checksums, "crc32", out var checksumCRC32))
            request.ChecksumCRC32 = checksumCRC32;
        if (TryGetChecksumValue(checksums, "crc32c", out var checksumCRC32C))
            request.ChecksumCRC32C = checksumCRC32C;
        if (TryGetChecksumValue(checksums, "crc64nvme", out var checksumCRC64NVME))
            request.ChecksumCRC64NVME = checksumCRC64NVME;
        if (TryGetChecksumValue(checksums, "sha1", out var checksumSHA1))
            request.ChecksumSHA1 = checksumSHA1;
        if (TryGetChecksumValue(checksums, "sha256", out var checksumSHA256))
            request.ChecksumSHA256 = checksumSHA256;
    }

    private static void ApplyChecksumHeaders(CopyPartRequest request, string? checksumAlgorithm, IReadOnlyDictionary<string, string>? checksums)
    {
        var checksumHeaders = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var sdkChecksumAlgorithm = MapChecksumAlgorithm(checksumAlgorithm);
        if (sdkChecksumAlgorithm is not null)
            checksumHeaders["x-amz-checksum-algorithm"] = sdkChecksumAlgorithm.ToString();

        if (TryGetChecksumValue(checksums, "crc32", out var checksumCRC32))
            checksumHeaders["x-amz-checksum-crc32"] = checksumCRC32;
        if (TryGetChecksumValue(checksums, "crc32c", out var checksumCRC32C))
            checksumHeaders["x-amz-checksum-crc32c"] = checksumCRC32C;
        if (TryGetChecksumValue(checksums, "crc64nvme", out var checksumCRC64NVME))
            checksumHeaders["x-amz-checksum-crc64nvme"] = checksumCRC64NVME;
        if (TryGetChecksumValue(checksums, "sha1", out var checksumSHA1))
            checksumHeaders["x-amz-checksum-sha1"] = checksumSHA1;
        if (TryGetChecksumValue(checksums, "sha256", out var checksumSHA256))
            checksumHeaders["x-amz-checksum-sha256"] = checksumSHA256;

        if (checksumHeaders.Count == 0) {
            return;
        }

        ((IAmazonWebServiceRequest)request).AddBeforeRequestHandler((_, eventArgs) => {
            if (eventArgs is not WebServiceRequestEventArgs requestEventArgs) {
                return;
            }

            foreach (var (headerName, headerValue) in checksumHeaders) {
                requestEventArgs.Headers[headerName] = headerValue;
            }
        });
    }

    private static PartETag CreatePartETag(MultipartUploadPart part)
    {
        var partETag = new PartETag(part.PartNumber, part.ETag);

        if (TryGetChecksumValue(part.Checksums, "crc32", out var checksumCRC32))
            partETag.ChecksumCRC32 = checksumCRC32;
        if (TryGetChecksumValue(part.Checksums, "crc32c", out var checksumCRC32C))
            partETag.ChecksumCRC32C = checksumCRC32C;
        if (TryGetChecksumValue(part.Checksums, "crc64nvme", out var checksumCRC64NVME))
            partETag.ChecksumCRC64NVME = checksumCRC64NVME;
        if (TryGetChecksumValue(part.Checksums, "sha1", out var checksumSHA1))
            partETag.ChecksumSHA1 = checksumSHA1;
        if (TryGetChecksumValue(part.Checksums, "sha256", out var checksumSHA256))
            partETag.ChecksumSHA256 = checksumSHA256;

        return partETag;
    }

    private static bool TryGetChecksumValue(IReadOnlyDictionary<string, string>? checksums, string algorithm, out string value)
    {
        value = string.Empty;
        if (checksums is null)
            return false;

        if (checksums.TryGetValue(algorithm, out var directValue) && !string.IsNullOrWhiteSpace(directValue))
        {
            value = directValue;
            return true;
        }

        foreach (var checksum in checksums)
        {
            if (string.Equals(checksum.Key, algorithm, StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrWhiteSpace(checksum.Value))
            {
                value = checksum.Value;
                return true;
            }
        }

        return false;
    }

    private Uri AlignPresignedUrlWithServiceUrl(Uri presignedUrl)
    {
        if (_serviceUri is null
            || string.Equals(presignedUrl.Scheme, _serviceUri.Scheme, StringComparison.OrdinalIgnoreCase))
        {
            return presignedUrl;
        }

        var builder = new UriBuilder(presignedUrl)
        {
            Scheme = _serviceUri.Scheme,
            Port = _serviceUri.IsDefaultPort ? -1 : _serviceUri.Port
        };

        return builder.Uri;
    }

    private static Uri? TryCreateAbsoluteUri(string? value)
    {
        return Uri.TryCreate(value, UriKind.Absolute, out var uri)
            ? uri
            : null;
    }

    private static string? NormalizeChecksumAlgorithm(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return value switch
        {
            "SHA256" => "sha256",
            "SHA1" => "sha1",
            "CRC32" => "crc32",
            "CRC32C" => "crc32c",
            "CRC64NVME" => "crc64nvme",
            _ => value.ToLowerInvariant()
        };
    }

    private static ChecksumAlgorithm? MapChecksumAlgorithm(string? value)
    {
        return NormalizeChecksumAlgorithm(value) switch
        {
            "sha256" => ChecksumAlgorithm.SHA256,
            "sha1" => ChecksumAlgorithm.SHA1,
            "crc32" => ChecksumAlgorithm.CRC32,
            "crc32c" => ChecksumAlgorithm.CRC32C,
            "crc64nvme" => ChecksumAlgorithm.CRC64NVME,
            _ => null
        };
    }

    private static BucketVersioningStatus MapVersioningStatus(Amazon.S3.VersionStatus? sdkStatus)
    {
        if (sdkStatus == Amazon.S3.VersionStatus.Enabled)
            return BucketVersioningStatus.Enabled;
        if (sdkStatus == Amazon.S3.VersionStatus.Suspended)
            return BucketVersioningStatus.Suspended;
        return BucketVersioningStatus.Disabled;
    }

    private static Amazon.S3.VersionStatus MapVersioningStatusToSdk(BucketVersioningStatus status) => status switch
    {
        BucketVersioningStatus.Enabled => Amazon.S3.VersionStatus.Enabled,
        BucketVersioningStatus.Suspended => Amazon.S3.VersionStatus.Suspended,
        _ => Amazon.S3.VersionStatus.Off
    };

    private static S3CorsRuleEntry MapCorsRule(CORSRule rule)
    {
        return new S3CorsRuleEntry(
            rule.Id,
            (rule.AllowedOrigins ?? []).ToArray(),
            (rule.AllowedMethods ?? []).Select(MapCorsMethod).ToArray(),
            (rule.AllowedHeaders ?? []).ToArray(),
            (rule.ExposeHeaders ?? []).ToArray(),
            rule.MaxAgeSeconds);
    }

    private static CORSRule MapCorsRule(S3CorsRuleEntry rule)
    {
        return new CORSRule
        {
            Id = rule.Id,
            AllowedOrigins = rule.AllowedOrigins.ToList(),
            AllowedMethods = rule.AllowedMethods.Select(MapCorsMethod).ToList(),
            AllowedHeaders = rule.AllowedHeaders.ToList(),
            ExposeHeaders = rule.ExposeHeaders.ToList(),
            MaxAgeSeconds = rule.MaxAgeSeconds
        };
    }

    private static BucketCorsMethod MapCorsMethod(string method)
    {
        return method switch
        {
            "GET" => BucketCorsMethod.Get,
            "PUT" => BucketCorsMethod.Put,
            "POST" => BucketCorsMethod.Post,
            "DELETE" => BucketCorsMethod.Delete,
            "HEAD" => BucketCorsMethod.Head,
            _ => throw new InvalidOperationException($"Unsupported S3 CORS method '{method}'.")
        };
    }

    private static string MapCorsMethod(BucketCorsMethod method)
    {
        return method switch
        {
            BucketCorsMethod.Get => "GET",
            BucketCorsMethod.Put => "PUT",
            BucketCorsMethod.Post => "POST",
            BucketCorsMethod.Delete => "DELETE",
            BucketCorsMethod.Head => "HEAD",
            _ => throw new ArgumentOutOfRangeException(nameof(method), method, "Unsupported CORS method.")
        };
    }

    private static long? TryParseContentRangeTotal(string? contentRange)
    {
        // Content-Range: bytes 0-499/1234
        if (string.IsNullOrEmpty(contentRange))
            return null;

        var slashIndex = contentRange.LastIndexOf('/');
        if (slashIndex < 0 || slashIndex == contentRange.Length - 1)
            return null;

        var totalStr = contentRange.AsSpan(slashIndex + 1);
        return long.TryParse(totalStr, out var total) ? total : null;
    }
}

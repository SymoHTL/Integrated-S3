using System.Text;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using IntegratedS3.Testing;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using static IntegratedS3.Tests.ChecksumTestAlgorithms;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class DiskStorageServiceTests
{
    [Fact]
    public async Task DiskStorage_RoundTripsBucketAndObjectContent()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var createBucket = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "photos"
        });

        Assert.True(createBucket.IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "photos",
            Key = "2026/launch.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["author"] = "copilot"
            }
        });

        Assert.True(putResult.IsSuccess);
        Assert.False(string.IsNullOrWhiteSpace(putResult.Value!.VersionId));
        Assert.Equal("text/plain", putResult.Value!.ContentType);
        Assert.Equal("copilot", putResult.Value.Metadata!["author"]);
        Assert.Equal(ComputeSha256Base64("hello integrated s3"), putResult.Value.Checksums!["sha256"]);
        Assert.Equal(ChecksumTestAlgorithms.ComputeCrc32cBase64("hello integrated s3"), putResult.Value.Checksums["crc32c"]);

        var objects = await storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "photos"
        }).ToArrayAsync();

        Assert.Single(objects);
        Assert.Equal("2026/launch.txt", objects[0].Key);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "photos",
            Key = "2026/launch.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            var content = await reader.ReadToEndAsync();

            Assert.Equal("hello integrated s3", content);
            Assert.Equal(putResult.Value.VersionId, response.Object.VersionId);
            Assert.Equal("copilot", response.Object.Metadata!["author"]);
            Assert.Equal(ComputeSha256Base64("hello integrated s3"), response.Object.Checksums!["sha256"]);
            Assert.Equal(ChecksumTestAlgorithms.ComputeCrc32cBase64("hello integrated s3"), response.Object.Checksums["crc32c"]);
        }

        var deleteObject = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "photos",
            Key = "2026/launch.txt"
        });

        Assert.True(deleteObject.IsSuccess);

        var deleteBucket = await storageService.DeleteBucketAsync(new DeleteBucketRequest
        {
            BucketName = "photos"
        });

        Assert.True(deleteBucket.IsSuccess);
    }

    [Fact]
    public async Task DeleteBucketAsync_ReturnsBucketNotEmpty_WhenBucketContainsObjects()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "non-empty-delete"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("still here"));
        Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "non-empty-delete",
            Key = "docs/hello.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        })).IsSuccess);

        var deleteBucket = await storageService.DeleteBucketAsync(new DeleteBucketRequest
        {
            BucketName = "non-empty-delete"
        });

        Assert.False(deleteBucket.IsSuccess);
        Assert.Equal(StorageErrorCode.BucketNotEmpty, deleteBucket.Error!.Code);
        Assert.Equal(409, deleteBucket.Error.SuggestedHttpStatusCode);
        Assert.Contains("empty", deleteBucket.Error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DiskStorage_ValidatesRequestedChecksumsOnPut()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "checksums"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("checksum payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "checksums",
            Key = "docs/checksum.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = "invalid-checksum"
            }
        });

        Assert.False(putResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.InvalidChecksum, putResult.Error!.Code);

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "checksums",
            Key = "docs/checksum.txt"
        });

        Assert.False(headResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, headResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PutObject_RejectsServerSideEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "encrypted-put"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("encrypted payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "encrypted-put",
            Key = "docs/object.txt",
            Content = uploadStream,
            ServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(putResult.IsSuccess);
        AssertUnsupportedServerSideEncryption(putResult.Error, "encrypted-put", "docs/object.txt");

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "encrypted-put",
            Key = "docs/object.txt"
        });

        Assert.False(headResult.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, headResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PutObject_WithSha1Checksum_RoundTripsAndRejectsMismatch()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "sha1-checksums"
        })).IsSuccess);

        const string payload = "sha1 checksum payload";
        var checksum = ComputeSha1Base64(payload);

        await using var validUploadStream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/sha1.txt",
            Content = validUploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha1"] = checksum
            }
        });

        Assert.True(putResult.IsSuccess);
        Assert.NotNull(putResult.Value!.Checksums);
        Assert.Single(putResult.Value.Checksums!);
        Assert.Equal(checksum, putResult.Value.Checksums["sha1"]);
        Assert.False(putResult.Value.Checksums.ContainsKey("sha256"));

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/sha1.txt"
        });

        Assert.True(headResult.IsSuccess);
        Assert.NotNull(headResult.Value!.Checksums);
        Assert.Single(headResult.Value.Checksums!);
        Assert.Equal(checksum, headResult.Value.Checksums["sha1"]);
        Assert.False(headResult.Value.Checksums.ContainsKey("sha256"));

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/sha1.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            Assert.Equal(payload, await reader.ReadToEndAsync());
            Assert.NotNull(response.Object.Checksums);
            Assert.Single(response.Object.Checksums!);
            Assert.Equal(checksum, response.Object.Checksums["sha1"]);
            Assert.False(response.Object.Checksums.ContainsKey("sha256"));
        }

        await using var invalidUploadStream = new MemoryStream(Encoding.UTF8.GetBytes("sha1 checksum mismatch"));
        var invalidPutResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/invalid-sha1.txt",
            Content = invalidUploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha1"] = "invalid-checksum"
            }
        });

        Assert.False(invalidPutResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.InvalidChecksum, invalidPutResult.Error!.Code);

        var invalidHeadResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "sha1-checksums",
            Key = "docs/invalid-sha1.txt"
        });

        Assert.False(invalidHeadResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, invalidHeadResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PutObject_WithCrc32cChecksum_RoundTripsAndRejectsMismatch()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "crc32c-checksums"
        })).IsSuccess);

        const string payload = "crc32c checksum payload";
        var checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(payload);

        await using var validUploadStream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/crc32c.txt",
            Content = validUploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["crc32c"] = checksum
            }
        });

        Assert.True(putResult.IsSuccess);
        Assert.NotNull(putResult.Value!.Checksums);
        Assert.Single(putResult.Value.Checksums!);
        Assert.Equal(checksum, putResult.Value.Checksums["crc32c"]);
        Assert.False(putResult.Value.Checksums.ContainsKey("sha256"));

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/crc32c.txt"
        });

        Assert.True(headResult.IsSuccess);
        Assert.NotNull(headResult.Value!.Checksums);
        Assert.Single(headResult.Value.Checksums!);
        Assert.Equal(checksum, headResult.Value.Checksums["crc32c"]);
        Assert.False(headResult.Value.Checksums.ContainsKey("sha256"));

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/crc32c.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            Assert.Equal(payload, await reader.ReadToEndAsync());
            Assert.NotNull(response.Object.Checksums);
            Assert.Single(response.Object.Checksums!);
            Assert.Equal(checksum, response.Object.Checksums["crc32c"]);
            Assert.False(response.Object.Checksums.ContainsKey("sha256"));
        }

        await using var invalidUploadStream = new MemoryStream(Encoding.UTF8.GetBytes("crc32c checksum mismatch"));
        var invalidPutResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/invalid-crc32c.txt",
            Content = invalidUploadStream,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["crc32c"] = "invalid-checksum"
            }
        });

        Assert.False(invalidPutResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.InvalidChecksum, invalidPutResult.Error!.Code);

        var invalidHeadResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "crc32c-checksums",
            Key = "docs/invalid-crc32c.txt"
        });

        Assert.False(invalidHeadResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, invalidHeadResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_CurrentVersionRequests_RespectPersistedVersionId()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "versions"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("versioned payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);
        var versionId = Assert.IsType<string>(putResult.Value!.VersionId);
        Assert.False(string.IsNullOrWhiteSpace(versionId));

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            VersionId = versionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["owner"] = "copilot"
            }
        });

        Assert.True(putTagsResult.IsSuccess);
        Assert.Equal(versionId, putTagsResult.Value!.VersionId);

        var getTagsResult = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            VersionId = versionId
        });

        Assert.True(getTagsResult.IsSuccess);
        Assert.Equal(versionId, getTagsResult.Value!.VersionId);
        Assert.Equal("copilot", getTagsResult.Value.Tags["owner"]);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            VersionId = versionId
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            Assert.Equal(versionId, response.Object.VersionId);
            Assert.Equal(ComputeSha256Base64("versioned payload"), response.Object.Checksums!["sha256"]);
        }

        var wrongVersionHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "versions",
            Key = "docs/versioned.txt",
            VersionId = "wrong-version"
        });

        Assert.False(wrongVersionHead.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, wrongVersionHead.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_BucketVersioning_CanBeEnabledAndSuspended()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var createResult = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "bucket-versioning",
            EnableVersioning = true
        });

        Assert.True(createResult.IsSuccess);
        Assert.True(createResult.Value!.VersioningEnabled);

        var initialVersioning = await storageService.GetBucketVersioningAsync("bucket-versioning");
        Assert.True(initialVersioning.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Enabled, initialVersioning.Value!.Status);

        var suspended = await storageService.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "bucket-versioning",
            Status = BucketVersioningStatus.Suspended
        });

        Assert.True(suspended.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Suspended, suspended.Value!.Status);

        var headBucket = await storageService.HeadBucketAsync("bucket-versioning");
        Assert.True(headBucket.IsSuccess);
        Assert.False(headBucket.Value!.VersioningEnabled);

        var listBuckets = await storageService.ListBucketsAsync().ToArrayAsync();
        Assert.Contains(listBuckets, static bucket => bucket.Name == "bucket-versioning" && !bucket.VersioningEnabled);
    }

    [Fact]
    public async Task DiskStorage_BucketLocation_ReturnsDefaultEmptyConstraint()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "bucket-location"
        })).IsSuccess);

        var location = await storageService.GetBucketLocationAsync("bucket-location");

        Assert.True(location.IsSuccess);
        Assert.Equal("bucket-location", location.Value!.BucketName);
        Assert.Null(location.Value.LocationConstraint);
    }

    [Fact]
    public async Task DiskStorage_BucketCors_RoundTripsAndPreservesVersioningMetadata()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var createResult = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "bucket-cors",
            EnableVersioning = true
        });

        Assert.True(createResult.IsSuccess);

        var putCors = await storageService.PutBucketCorsAsync(new PutBucketCorsRequest
        {
            BucketName = "bucket-cors",
            Rules =
            [
                new BucketCorsRule
                {
                    Id = "browser-rule",
                    AllowedOrigins = ["https://app.example"],
                    AllowedMethods = [BucketCorsMethod.Get, BucketCorsMethod.Put],
                    AllowedHeaders = ["authorization", "x-amz-*"],
                    ExposeHeaders = ["etag"],
                    MaxAgeSeconds = 900
                }
            ]
        });

        Assert.True(putCors.IsSuccess);
        var initialRule = Assert.Single(putCors.Value!.Rules);
        Assert.Equal("browser-rule", initialRule.Id);

        var suspended = await storageService.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "bucket-cors",
            Status = BucketVersioningStatus.Suspended
        });

        Assert.True(suspended.IsSuccess);

        var getCors = await storageService.GetBucketCorsAsync("bucket-cors");
        Assert.True(getCors.IsSuccess);
        var storedRule = Assert.Single(getCors.Value!.Rules);
        Assert.Equal([BucketCorsMethod.Get, BucketCorsMethod.Put], storedRule.AllowedMethods);
        Assert.Equal(["authorization", "x-amz-*"], storedRule.AllowedHeaders);

        var versioning = await storageService.GetBucketVersioningAsync("bucket-cors");
        Assert.True(versioning.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Suspended, versioning.Value!.Status);

        var deleteCors = await storageService.DeleteBucketCorsAsync(new DeleteBucketCorsRequest
        {
            BucketName = "bucket-cors"
        });

        Assert.True(deleteCors.IsSuccess);

        var missingCors = await storageService.GetBucketCorsAsync("bucket-cors");
        Assert.False(missingCors.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.CorsConfigurationNotFound, missingCors.Error!.Code);

        var preservedVersioning = await storageService.GetBucketVersioningAsync("bucket-cors");
        Assert.True(preservedVersioning.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Suspended, preservedVersioning.Value!.Status);
    }

    [Fact]
    public async Task DiskStorage_BucketMetadataConfigurations_RoundTripAcrossReload()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "bucket-config-roundtrip";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName,
            EnableVersioning = true
        })).IsSuccess);

        var putTagging = await storageService.PutBucketTaggingAsync(new PutBucketTaggingRequest
        {
            BucketName = bucketName,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "test",
                ["team"] = "storage"
            }
        });
        Assert.True(putTagging.IsSuccess);

        var putLogging = await storageService.PutBucketLoggingAsync(new PutBucketLoggingRequest
        {
            BucketName = bucketName,
            TargetBucket = "audit-bucket",
            TargetPrefix = "bucket-config-roundtrip/"
        });
        Assert.True(putLogging.IsSuccess);

        var putWebsite = await storageService.PutBucketWebsiteAsync(new PutBucketWebsiteRequest
        {
            BucketName = bucketName,
            IndexDocumentSuffix = "index.html",
            ErrorDocumentKey = "error.html",
            RoutingRules =
            [
                new BucketWebsiteRoutingRule
                {
                    Condition = new BucketWebsiteRoutingRuleCondition
                    {
                        KeyPrefixEquals = "docs/",
                        HttpErrorCodeReturnedEquals = 404
                    },
                    Redirect = new BucketWebsiteRoutingRuleRedirect
                    {
                        HostName = "www.example.test",
                        Protocol = "https",
                        ReplaceKeyPrefixWith = "documents/",
                        HttpRedirectCode = 302
                    }
                }
            ]
        });
        Assert.True(putWebsite.IsSuccess);

        var putRequestPayment = await storageService.PutBucketRequestPaymentAsync(new PutBucketRequestPaymentRequest
        {
            BucketName = bucketName,
            Payer = BucketPayer.Requester
        });
        Assert.True(putRequestPayment.IsSuccess);

        var putAccelerate = await storageService.PutBucketAccelerateAsync(new PutBucketAccelerateRequest
        {
            BucketName = bucketName,
            Status = BucketAccelerateStatus.Enabled
        });
        Assert.True(putAccelerate.IsSuccess);

        var putLifecycle = await storageService.PutBucketLifecycleAsync(new PutBucketLifecycleRequest
        {
            BucketName = bucketName,
            Rules =
            [
                new BucketLifecycleRule
                {
                    Id = "expire-docs",
                    FilterPrefix = "docs/",
                    FilterTags = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["class"] = "cold"
                    },
                    Status = BucketLifecycleRuleStatus.Enabled,
                    ExpirationDays = 30,
                    AbortIncompleteMultipartUploadDaysAfterInitiation = 7,
                    Transitions =
                    [
                        new BucketLifecycleTransition
                        {
                            Days = 15,
                            StorageClass = "GLACIER"
                        }
                    ],
                    NoncurrentVersionTransitions =
                    [
                        new BucketLifecycleNoncurrentVersionTransition
                        {
                            NoncurrentDays = 10,
                            StorageClass = "DEEP_ARCHIVE"
                        }
                    ]
                }
            ]
        });
        Assert.True(putLifecycle.IsSuccess);

        var putReplication = await storageService.PutBucketReplicationAsync(new PutBucketReplicationRequest
        {
            BucketName = bucketName,
            Role = "arn:aws:iam::123456789012:role/replication",
            Rules =
            [
                new BucketReplicationRule
                {
                    Id = "replicate-docs",
                    Status = BucketReplicationRuleStatus.Enabled,
                    FilterPrefix = "docs/",
                    Destination = new BucketReplicationDestination
                    {
                        Bucket = "arn:aws:s3:::replica-bucket",
                        StorageClass = "STANDARD_IA",
                        Account = "123456789012"
                    },
                    Priority = 1,
                    DeleteMarkerReplication = true
                }
            ]
        });
        Assert.True(putReplication.IsSuccess);

        var putNotifications = await storageService.PutBucketNotificationConfigurationAsync(new PutBucketNotificationConfigurationRequest
        {
            BucketName = bucketName,
            TopicConfigurations =
            [
                new BucketNotificationTopicConfiguration
                {
                    Id = "topic-config",
                    TopicArn = "arn:aws:sns:eu-central-1:123456789012:bucket-events",
                    Events = ["s3:ObjectCreated:*"],
                    Filter = new BucketNotificationFilter
                    {
                        KeyFilterRules =
                        [
                            new BucketNotificationFilterRule { Name = "prefix", Value = "incoming/" }
                        ]
                    }
                }
            ],
            QueueConfigurations =
            [
                new BucketNotificationQueueConfiguration
                {
                    Id = "queue-config",
                    QueueArn = "arn:aws:sqs:eu-central-1:123456789012:bucket-events",
                    Events = ["s3:ObjectRemoved:*"]
                }
            ],
            LambdaFunctionConfigurations =
            [
                new BucketNotificationLambdaConfiguration
                {
                    Id = "lambda-config",
                    LambdaFunctionArn = "arn:aws:lambda:eu-central-1:123456789012:function:bucket-events",
                    Events = ["s3:ObjectRestore:*"]
                }
            ]
        });
        Assert.True(putNotifications.IsSuccess);

        var putObjectLock = await storageService.PutObjectLockConfigurationAsync(new PutObjectLockConfigurationRequest
        {
            BucketName = bucketName,
            ObjectLockEnabled = true,
            DefaultRetention = new ObjectLockDefaultRetention
            {
                Mode = ObjectRetentionMode.Governance,
                Years = 2
            }
        });
        Assert.True(putObjectLock.IsSuccess);

        var bucketMetadataPath = GetBucketMetadataPath(fixture.RootPath, bucketName);
        Assert.True(File.Exists(bucketMetadataPath));

        using var reloadedServices = CreateDiskStorageServiceProvider(fixture.RootPath);
        var reloadedStorageService = reloadedServices.GetRequiredService<IStorageBackend>();

        var tagging = await reloadedStorageService.GetBucketTaggingAsync(bucketName);
        Assert.True(tagging.IsSuccess);
        Assert.Equal("test", tagging.Value!.Tags["environment"]);
        Assert.Equal("storage", tagging.Value.Tags["team"]);

        var logging = await reloadedStorageService.GetBucketLoggingAsync(bucketName);
        Assert.True(logging.IsSuccess);
        Assert.Equal("audit-bucket", logging.Value!.TargetBucket);
        Assert.Equal("bucket-config-roundtrip/", logging.Value.TargetPrefix);

        var website = await reloadedStorageService.GetBucketWebsiteAsync(bucketName);
        Assert.True(website.IsSuccess);
        Assert.Equal("index.html", website.Value!.IndexDocumentSuffix);
        Assert.Equal("error.html", website.Value.ErrorDocumentKey);
        var routingRule = Assert.Single(website.Value.RoutingRules);
        Assert.Equal("docs/", routingRule.Condition!.KeyPrefixEquals);
        Assert.Equal(404, routingRule.Condition.HttpErrorCodeReturnedEquals);
        Assert.Equal("documents/", routingRule.Redirect.ReplaceKeyPrefixWith);
        Assert.Equal(302, routingRule.Redirect.HttpRedirectCode);

        var requestPayment = await reloadedStorageService.GetBucketRequestPaymentAsync(bucketName);
        Assert.True(requestPayment.IsSuccess);
        Assert.Equal(BucketPayer.Requester, requestPayment.Value!.Payer);

        var accelerate = await reloadedStorageService.GetBucketAccelerateAsync(bucketName);
        Assert.True(accelerate.IsSuccess);
        Assert.Equal(BucketAccelerateStatus.Enabled, accelerate.Value!.Status);

        var lifecycle = await reloadedStorageService.GetBucketLifecycleAsync(bucketName);
        Assert.True(lifecycle.IsSuccess);
        var lifecycleRule = Assert.Single(lifecycle.Value!.Rules);
        Assert.Equal("expire-docs", lifecycleRule.Id);
        Assert.Equal("docs/", lifecycleRule.FilterPrefix);
        Assert.Equal("cold", lifecycleRule.FilterTags!["class"]);
        Assert.Equal(30, lifecycleRule.ExpirationDays);
        Assert.Equal(7, lifecycleRule.AbortIncompleteMultipartUploadDaysAfterInitiation);
        Assert.Equal("GLACIER", Assert.Single(lifecycleRule.Transitions).StorageClass);
        Assert.Equal("DEEP_ARCHIVE", Assert.Single(lifecycleRule.NoncurrentVersionTransitions).StorageClass);

        var replication = await reloadedStorageService.GetBucketReplicationAsync(bucketName);
        Assert.True(replication.IsSuccess);
        Assert.Equal("arn:aws:iam::123456789012:role/replication", replication.Value!.Role);
        var replicationRule = Assert.Single(replication.Value.Rules);
        Assert.Equal("replicate-docs", replicationRule.Id);
        Assert.Equal(BucketReplicationRuleStatus.Enabled, replicationRule.Status);
        Assert.Equal("arn:aws:s3:::replica-bucket", replicationRule.Destination.Bucket);
        Assert.Equal("STANDARD_IA", replicationRule.Destination.StorageClass);
        Assert.Equal("123456789012", replicationRule.Destination.Account);
        Assert.True(replicationRule.DeleteMarkerReplication);

        var notifications = await reloadedStorageService.GetBucketNotificationConfigurationAsync(bucketName);
        Assert.True(notifications.IsSuccess);
        var topicConfiguration = Assert.Single(notifications.Value!.TopicConfigurations);
        Assert.Equal("topic-config", topicConfiguration.Id);
        Assert.Equal("arn:aws:sns:eu-central-1:123456789012:bucket-events", topicConfiguration.TopicArn);
        Assert.Equal("incoming/", Assert.Single(topicConfiguration.Filter!.KeyFilterRules).Value);
        Assert.Equal("arn:aws:sqs:eu-central-1:123456789012:bucket-events", Assert.Single(notifications.Value.QueueConfigurations).QueueArn);
        Assert.Equal("arn:aws:lambda:eu-central-1:123456789012:function:bucket-events", Assert.Single(notifications.Value.LambdaFunctionConfigurations).LambdaFunctionArn);

        var objectLock = await reloadedStorageService.GetObjectLockConfigurationAsync(bucketName);
        Assert.True(objectLock.IsSuccess);
        Assert.True(objectLock.Value!.ObjectLockEnabled);
        Assert.Equal(ObjectRetentionMode.Governance, objectLock.Value.DefaultRetention!.Mode);
        Assert.Equal(2, objectLock.Value.DefaultRetention.Years);
    }

    [Fact]
    public async Task DiskStorage_CreateBucket_WhenAlreadyExists_ReturnsBucketAlreadyOwnedByYou()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "owned-by-you";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        })).IsSuccess);

        var secondCreate = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        });

        Assert.False(secondCreate.IsSuccess);
        // Single-tenant disk backend: an owner re-create is BucketAlreadyOwnedByYou, not BucketAlreadyExists.
        Assert.Equal(StorageErrorCode.BucketAlreadyOwnedByYou, secondCreate.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_BucketMetadataDeleteOperations_PreserveOtherConfigurationsUntilEmpty()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "bucket-config-deletes";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        })).IsSuccess);

        Assert.True((await storageService.PutBucketTaggingAsync(new PutBucketTaggingRequest
        {
            BucketName = bucketName,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["state"] = "present"
            }
        })).IsSuccess);

        Assert.True((await storageService.PutBucketWebsiteAsync(new PutBucketWebsiteRequest
        {
            BucketName = bucketName,
            IndexDocumentSuffix = "index.html"
        })).IsSuccess);

        Assert.True((await storageService.PutBucketLifecycleAsync(new PutBucketLifecycleRequest
        {
            BucketName = bucketName,
            Rules =
            [
                new BucketLifecycleRule
                {
                    Id = "cleanup",
                    Status = BucketLifecycleRuleStatus.Enabled,
                    ExpirationDays = 7
                }
            ]
        })).IsSuccess);

        Assert.True((await storageService.PutBucketReplicationAsync(new PutBucketReplicationRequest
        {
            BucketName = bucketName,
            Rules =
            [
                new BucketReplicationRule
                {
                    Id = "replicate",
                    Status = BucketReplicationRuleStatus.Enabled,
                    Destination = new BucketReplicationDestination
                    {
                        Bucket = "arn:aws:s3:::replica"
                    }
                }
            ]
        })).IsSuccess);

        var bucketMetadataPath = GetBucketMetadataPath(fixture.RootPath, bucketName);
        Assert.True(File.Exists(bucketMetadataPath));

        var deleteTagging = await storageService.DeleteBucketTaggingAsync(new DeleteBucketTaggingRequest
        {
            BucketName = bucketName
        });
        Assert.True(deleteTagging.IsSuccess);
        Assert.True(File.Exists(bucketMetadataPath));

        var missingTagging = await storageService.GetBucketTaggingAsync(bucketName);
        Assert.False(missingTagging.IsSuccess);
        Assert.Equal(StorageErrorCode.TaggingConfigurationNotFound, missingTagging.Error!.Code);
        Assert.True((await storageService.GetBucketWebsiteAsync(bucketName)).IsSuccess);
        Assert.True((await storageService.GetBucketLifecycleAsync(bucketName)).IsSuccess);
        Assert.True((await storageService.GetBucketReplicationAsync(bucketName)).IsSuccess);

        var deleteWebsite = await storageService.DeleteBucketWebsiteAsync(new DeleteBucketWebsiteRequest
        {
            BucketName = bucketName
        });
        Assert.True(deleteWebsite.IsSuccess);
        Assert.True(File.Exists(bucketMetadataPath));

        var missingWebsite = await storageService.GetBucketWebsiteAsync(bucketName);
        Assert.False(missingWebsite.IsSuccess);
        Assert.Equal(StorageErrorCode.WebsiteConfigurationNotFound, missingWebsite.Error!.Code);
        Assert.True((await storageService.GetBucketLifecycleAsync(bucketName)).IsSuccess);
        Assert.True((await storageService.GetBucketReplicationAsync(bucketName)).IsSuccess);

        var deleteLifecycle = await storageService.DeleteBucketLifecycleAsync(new DeleteBucketLifecycleRequest
        {
            BucketName = bucketName
        });
        Assert.True(deleteLifecycle.IsSuccess);
        Assert.True(File.Exists(bucketMetadataPath));

        var missingLifecycle = await storageService.GetBucketLifecycleAsync(bucketName);
        Assert.False(missingLifecycle.IsSuccess);
        Assert.Equal(StorageErrorCode.LifecycleConfigurationNotFound, missingLifecycle.Error!.Code);
        Assert.True((await storageService.GetBucketReplicationAsync(bucketName)).IsSuccess);

        using (var reloadedServices = CreateDiskStorageServiceProvider(fixture.RootPath)) {
            var reloadedStorageService = reloadedServices.GetRequiredService<IStorageBackend>();
            Assert.True((await reloadedStorageService.GetBucketReplicationAsync(bucketName)).IsSuccess);
        }

        var deleteReplication = await storageService.DeleteBucketReplicationAsync(new DeleteBucketReplicationRequest
        {
            BucketName = bucketName
        });
        Assert.True(deleteReplication.IsSuccess);
        Assert.False(File.Exists(bucketMetadataPath));

        using var finalReloadedServices = CreateDiskStorageServiceProvider(fixture.RootPath);
        var finalReloadedStorageService = finalReloadedServices.GetRequiredService<IStorageBackend>();
        var missingReplication = await finalReloadedStorageService.GetBucketReplicationAsync(bucketName);
        Assert.False(missingReplication.IsSuccess);
        Assert.Equal(StorageErrorCode.ReplicationConfigurationNotFound, missingReplication.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_KeyedBucketMetadataConfigurations_RoundTripAndDeleteAcrossReload()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "bucket-config-keyed";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        })).IsSuccess);

        var putAnalytics = await storageService.PutBucketAnalyticsConfigurationAsync(new PutBucketAnalyticsConfigurationRequest
        {
            BucketName = bucketName,
            Id = "analytics-1",
            FilterPrefix = "logs/",
            FilterTags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["dataset"] = "access"
            },
            StorageClassAnalysis = new BucketAnalyticsStorageClassAnalysis
            {
                DataExport = new BucketAnalyticsDataExport
                {
                    OutputSchemaVersion = "V_1",
                    Destination = new BucketAnalyticsS3BucketDestination
                    {
                        Format = "CSV",
                        BucketAccountId = "123456789012",
                        Bucket = "arn:aws:s3:::analytics-export",
                        Prefix = "reports/"
                    }
                }
            }
        });
        Assert.True(putAnalytics.IsSuccess);

        var putMetrics = await storageService.PutBucketMetricsConfigurationAsync(new PutBucketMetricsConfigurationRequest
        {
            BucketName = bucketName,
            Id = "metrics-1",
            Filter = new BucketMetricsFilter
            {
                Prefix = "logs/",
                AccessPointArn = "arn:aws:s3:eu-central-1:123456789012:accesspoint/metrics",
                Tags = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["team"] = "storage"
                }
            }
        });
        Assert.True(putMetrics.IsSuccess);

        var putInventory = await storageService.PutBucketInventoryConfigurationAsync(new PutBucketInventoryConfigurationRequest
        {
            BucketName = bucketName,
            Id = "inventory-1",
            IsEnabled = true,
            Destination = new BucketInventoryDestination
            {
                S3BucketDestination = new BucketInventoryS3BucketDestination
                {
                    Format = "CSV",
                    AccountId = "123456789012",
                    Bucket = "arn:aws:s3:::inventory-export",
                    Prefix = "daily/"
                }
            },
            Schedule = new BucketInventorySchedule
            {
                Frequency = "Daily"
            },
            Filter = new BucketInventoryFilter
            {
                Prefix = "logs/"
            },
            IncludedObjectVersions = "Current",
            OptionalFields = ["ETag", "Size"]
        });
        Assert.True(putInventory.IsSuccess);

        var bucketMetadataPath = GetBucketMetadataPath(fixture.RootPath, bucketName);
        Assert.True(File.Exists(bucketMetadataPath));

        using (var reloadedServices = CreateDiskStorageServiceProvider(fixture.RootPath)) {
            var reloadedStorageService = reloadedServices.GetRequiredService<IStorageBackend>();

            var analytics = await reloadedStorageService.GetBucketAnalyticsConfigurationAsync(bucketName, "analytics-1");
            Assert.True(analytics.IsSuccess);
            Assert.Equal("logs/", analytics.Value!.FilterPrefix);
            Assert.Equal("access", analytics.Value.FilterTags!["dataset"]);
            Assert.Equal("arn:aws:s3:::analytics-export", analytics.Value.StorageClassAnalysis!.DataExport!.Destination!.Bucket);

            var metrics = await reloadedStorageService.GetBucketMetricsConfigurationAsync(bucketName, "metrics-1");
            Assert.True(metrics.IsSuccess);
            Assert.Equal("logs/", metrics.Value!.Filter!.Prefix);
            Assert.Equal("arn:aws:s3:eu-central-1:123456789012:accesspoint/metrics", metrics.Value.Filter.AccessPointArn);
            Assert.Equal("storage", metrics.Value.Filter.Tags["team"]);

            var inventory = await reloadedStorageService.GetBucketInventoryConfigurationAsync(bucketName, "inventory-1");
            Assert.True(inventory.IsSuccess);
            Assert.True(inventory.Value!.IsEnabled);
            Assert.Equal("arn:aws:s3:::inventory-export", inventory.Value.Destination!.S3BucketDestination!.Bucket);
            Assert.Equal("Daily", inventory.Value.Schedule!.Frequency);
            Assert.Equal("logs/", inventory.Value.Filter!.Prefix);
            Assert.Equal("Current", inventory.Value.IncludedObjectVersions);
            Assert.Equal(["ETag", "Size"], inventory.Value.OptionalFields);
        }

        var deleteAnalytics = await storageService.DeleteBucketAnalyticsConfigurationAsync(new DeleteBucketAnalyticsConfigurationRequest
        {
            BucketName = bucketName,
            Id = "analytics-1"
        });
        Assert.True(deleteAnalytics.IsSuccess);
        Assert.True(File.Exists(bucketMetadataPath));

        var missingAnalytics = await storageService.GetBucketAnalyticsConfigurationAsync(bucketName, "analytics-1");
        Assert.False(missingAnalytics.IsSuccess);
        Assert.Equal(StorageErrorCode.AnalyticsConfigurationNotFound, missingAnalytics.Error!.Code);
        Assert.True((await storageService.GetBucketMetricsConfigurationAsync(bucketName, "metrics-1")).IsSuccess);
        Assert.True((await storageService.GetBucketInventoryConfigurationAsync(bucketName, "inventory-1")).IsSuccess);

        var deleteMetrics = await storageService.DeleteBucketMetricsConfigurationAsync(new DeleteBucketMetricsConfigurationRequest
        {
            BucketName = bucketName,
            Id = "metrics-1"
        });
        Assert.True(deleteMetrics.IsSuccess);
        Assert.True(File.Exists(bucketMetadataPath));

        var missingMetrics = await storageService.GetBucketMetricsConfigurationAsync(bucketName, "metrics-1");
        Assert.False(missingMetrics.IsSuccess);
        Assert.Equal(StorageErrorCode.MetricsConfigurationNotFound, missingMetrics.Error!.Code);
        Assert.True((await storageService.GetBucketInventoryConfigurationAsync(bucketName, "inventory-1")).IsSuccess);

        var deleteInventory = await storageService.DeleteBucketInventoryConfigurationAsync(new DeleteBucketInventoryConfigurationRequest
        {
            BucketName = bucketName,
            Id = "inventory-1"
        });
        Assert.True(deleteInventory.IsSuccess);
        Assert.False(File.Exists(bucketMetadataPath));

        using var finalReloadedServices = CreateDiskStorageServiceProvider(fixture.RootPath);
        var finalReloadedStorageService = finalReloadedServices.GetRequiredService<IStorageBackend>();
        var missingInventory = await finalReloadedStorageService.GetBucketInventoryConfigurationAsync(bucketName, "inventory-1");
        Assert.False(missingInventory.IsSuccess);
        Assert.Equal(StorageErrorCode.InventoryConfigurationNotFound, missingInventory.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_VersionedOverwrite_PreservesHistoricalVersionAccess()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-history",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        Assert.True(v1Put.IsSuccess);
        Assert.True(v2Put.IsSuccess);
        Assert.NotEqual(v1Put.Value!.VersionId, v2Put.Value!.VersionId);

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt"
        });

        Assert.True(currentGet.IsSuccess);
        await using (var currentResponse = currentGet.Value!) {
            using var reader = new StreamReader(currentResponse.Content, Encoding.UTF8);
            Assert.Equal("version two", await reader.ReadToEndAsync());
            Assert.Equal(v2Put.Value.VersionId, currentResponse.Object.VersionId);
        }

        var historicalGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId
        });

        Assert.True(historicalGet.IsSuccess);
        await using (var historicalResponse = historicalGet.Value!) {
            using var reader = new StreamReader(historicalResponse.Content, Encoding.UTF8);
            Assert.Equal("version one", await reader.ReadToEndAsync());
            Assert.Equal(v1Put.Value.VersionId, historicalResponse.Object.VersionId);
        }

        var historicalTags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["generation"] = "one"
            }
        });

        Assert.True(historicalTags.IsSuccess);

        var rereadHistoricalTags = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "version-history",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId
        });

        Assert.True(rereadHistoricalTags.IsSuccess);
        Assert.Equal("one", rereadHistoricalTags.Value!.Tags["generation"]);
    }

    [Fact]
    public async Task DiskStorage_DeleteSpecificHistoricalVersion_RemovesOnlyThatVersion()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-delete",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("delete me first"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("keep me current"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        Assert.True(v1Put.IsSuccess);
        Assert.True(v2Put.IsSuccess);

        var deleteV1 = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt",
            VersionId = v1Put.Value!.VersionId
        });

        Assert.True(deleteV1.IsSuccess);

        var missingV1 = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt",
            VersionId = v1Put.Value.VersionId
        });

        Assert.False(missingV1.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, missingV1.Error!.Code);

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete",
            Key = "docs/delete.txt"
        });

        Assert.True(currentGet.IsSuccess);
        await using (var currentResponse = currentGet.Value!) {
            using var reader = new StreamReader(currentResponse.Content, Encoding.UTF8);
            Assert.Equal("keep me current", await reader.ReadToEndAsync());
            Assert.Equal(v2Put.Value!.VersionId, currentResponse.Object.VersionId);
        }
    }

    [Fact]
    public async Task DiskStorage_DeleteMissingObject_IsIdempotentAndCreatesVersionedDeleteMarkerWhenNeeded()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "delete-missing"
        })).IsSuccess);

        var deleteMissing = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "delete-missing",
            Key = "docs/missing.txt"
        });

        Assert.True(deleteMissing.IsSuccess);
        Assert.False(deleteMissing.Value!.IsDeleteMarker);
        Assert.Null(deleteMissing.Value.VersionId);

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "delete-missing-versioned",
            EnableVersioning = true
        })).IsSuccess);

        var deleteVersionedMissing = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "delete-missing-versioned",
            Key = "docs/missing.txt"
        });

        Assert.True(deleteVersionedMissing.IsSuccess);
        Assert.True(deleteVersionedMissing.Value!.IsDeleteMarker);
        var deleteMarkerVersionId = Assert.IsType<string>(deleteVersionedMissing.Value.VersionId);

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "delete-missing-versioned",
            Key = "docs/missing.txt"
        });

        Assert.False(currentGet.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, currentGet.Error!.Code);
        Assert.True(currentGet.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, currentGet.Error.VersionId);

        var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "delete-missing-versioned"
        }).ToArrayAsync();

        var deleteMarker = Assert.Single(versions);
        Assert.Equal("docs/missing.txt", deleteMarker.Key);
        Assert.Equal(deleteMarkerVersionId, deleteMarker.VersionId);
        Assert.True(deleteMarker.IsDeleteMarker);
        Assert.True(deleteMarker.IsLatest);
    }

    [Fact]
    public async Task DiskStorage_VersionedDelete_CreatesDeleteMarkerAndListsVersions()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-delete-marker",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete-marker",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);
        Assert.False(string.IsNullOrWhiteSpace(deleteCurrent.Value.VersionId));

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete-marker",
            Key = "docs/history.txt"
        });

        Assert.False(currentGet.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, currentGet.Error!.Code);

        var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "version-delete-marker",
            Prefix = "docs/history.txt"
        }).ToArrayAsync();

        Assert.Equal(3, versions.Length);
        Assert.Equal("docs/history.txt", versions[0].Key);
        Assert.True(versions[0].IsDeleteMarker);
        Assert.True(versions[0].IsLatest);
        Assert.Equal(deleteCurrent.Value.VersionId, versions[0].VersionId);
        Assert.Contains(versions, static version => !version.IsDeleteMarker && version.VersionId is not null && version.IsLatest is false);
        Assert.Contains(versions, version => version.VersionId == v2Put.Value!.VersionId && !version.IsDeleteMarker);
        Assert.Contains(versions, version => version.VersionId == v1Put.Value!.VersionId && !version.IsDeleteMarker);
    }

    [Fact]
    public async Task DiskStorage_DeleteMarkerGetAndHeadRequestsPreserveDeleteMarkerFidelity()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-delete-marker-fidelity",
            EnableVersioning = true
        })).IsSuccess);

        await using var putStream = new MemoryStream(Encoding.UTF8.GetBytes("current version"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt",
            Content = putStream,
            ContentType = "text/plain"
        });
        Assert.True(putResult.IsSuccess);

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        var deleteMarkerVersionId = Assert.IsType<string>(deleteCurrent.Value!.VersionId);

        var deleteMarkerVersion = Assert.Single(await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Prefix = "docs/history.txt"
        }).Where(static version => version.IsDeleteMarker).ToArrayAsync());

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt"
        });

        Assert.False(currentGet.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, currentGet.Error!.Code);
        Assert.True(currentGet.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, currentGet.Error.VersionId);
        Assert.Null(currentGet.Error.LastModifiedUtc);

        var currentHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt"
        });

        Assert.False(currentHead.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, currentHead.Error!.Code);
        Assert.True(currentHead.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, currentHead.Error.VersionId);
        Assert.Null(currentHead.Error.LastModifiedUtc);

        var explicitGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt",
            VersionId = deleteMarkerVersionId
        });

        Assert.False(explicitGet.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.MethodNotAllowed, explicitGet.Error!.Code);
        Assert.True(explicitGet.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, explicitGet.Error.VersionId);
        Assert.Equal(deleteMarkerVersion.LastModifiedUtc, explicitGet.Error.LastModifiedUtc);

        var explicitHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "version-delete-marker-fidelity",
            Key = "docs/history.txt",
            VersionId = deleteMarkerVersionId
        });

        Assert.False(explicitHead.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.MethodNotAllowed, explicitHead.Error!.Code);
        Assert.True(explicitHead.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, explicitHead.Error.VersionId);
        Assert.Equal(deleteMarkerVersion.LastModifiedUtc, explicitHead.Error.LastModifiedUtc);
        Assert.Equal(deleteMarkerVersionId, deleteMarkerVersion.VersionId);
    }

    [Fact]
    public async Task DiskStorage_DeletingCurrentDeleteMarkerByVersionId_RestoresPreviousVersion()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "version-delete-marker-restore",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);

        var deleteMarkerVersion = deleteCurrent.Value.VersionId;
        var deleteMarkerDelete = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt",
            VersionId = deleteMarkerVersion
        });

        Assert.True(deleteMarkerDelete.IsSuccess);
        Assert.True(deleteMarkerDelete.Value!.IsDeleteMarker);
        Assert.NotNull(deleteMarkerDelete.Value.CurrentObject);
        Assert.Equal(v2Put.Value!.VersionId, deleteMarkerDelete.Value.CurrentObject!.VersionId);

        var currentGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "version-delete-marker-restore",
            Key = "docs/history.txt"
        });

        Assert.True(currentGet.IsSuccess);
        await using var currentResponse = currentGet.Value!;
        using var reader = new StreamReader(currentResponse.Content, Encoding.UTF8);
        Assert.Equal("version two", await reader.ReadToEndAsync());
        Assert.Equal(v2Put.Value.VersionId, currentResponse.Object.VersionId);
        Assert.NotEqual(v1Put.Value!.VersionId, currentResponse.Object.VersionId);
    }

    [Fact]
    public async Task DiskStorage_RejectsPathTraversalKeys()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "safe" });

        await using var uploadStream = new MemoryStream([1, 2, 3]);

        await Assert.ThrowsAsync<ArgumentException>(async () => {
            await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "safe",
                Key = "../escape.txt",
                Content = uploadStream
            });
        });
    }

    [Fact]
    public async Task DiskStorage_SupportsRangeRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "docs",
            Key = "range.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "range.txt",
            Range = new ObjectRange
            {
                Start = 6,
                End = 15
            }
        });

        Assert.True(getResult.IsSuccess);
        await using var response = getResult.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
        var content = await reader.ReadToEndAsync();

        Assert.Equal("integrated", content);
        Assert.NotNull(response.Range);
        Assert.Equal(6, response.Range!.Start);
        Assert.Equal(15, response.Range.End);
        Assert.Equal(10, response.Object.ContentLength);
        Assert.Equal(19, response.TotalContentLength);
    }

    [Fact]
    public async Task DiskStorage_HonorsConditionalRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "docs",
            Key = "conditions.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        var currentETag = putResult.Value!.ETag;

        var ifMatchResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "conditions.txt",
            IfMatchETag = $"\"{currentETag}\""
        });

        Assert.True(ifMatchResult.IsSuccess);
        await using (var response = ifMatchResult.Value!) {
            Assert.False(response.IsNotModified);
        }

        var ifNoneMatchResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "conditions.txt",
            IfNoneMatchETag = $"\"{currentETag}\""
        });

        Assert.True(ifNoneMatchResult.IsSuccess);
        await using (var response = ifNoneMatchResult.Value!) {
            Assert.True(response.IsNotModified);
        }

        var failedIfMatch = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "conditions.txt",
            IfMatchETag = "\"different\""
        });

        Assert.False(failedIfMatch.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedIfMatch.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PaginatesObjectsUsingContinuationToken()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        foreach (var key in new[] { "a.txt", "b.txt", "c.txt" }) {
            await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes(key));
            var putResult = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "docs",
                Key = key,
                Content = uploadStream,
                ContentType = "text/plain"
            });
            Assert.True(putResult.IsSuccess);
        }

        var firstPage = await storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "docs",
            PageSize = 2
        }).ToArrayAsync();

        Assert.Equal(["a.txt", "b.txt"], firstPage.Select(static item => item.Key).ToArray());

        var secondPage = await storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "docs",
            ContinuationToken = firstPage[^1].Key,
            PageSize = 2
        }).ToArrayAsync();

        Assert.Equal(["c.txt"], secondPage.Select(static item => item.Key).ToArray());
    }

    [Fact]
    public async Task DiskStorage_HonorsDateBasedConditionalGetRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "docs",
            Key = "dates.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        var lastModifiedUtc = putResult.Value!.LastModifiedUtc;

        var notModifiedResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "dates.txt",
            IfModifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        });

        Assert.True(notModifiedResult.IsSuccess);
        await using (var response = notModifiedResult.Value!) {
            Assert.True(response.IsNotModified);
        }

        var failedPrecondition = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "dates.txt",
            IfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        });

        Assert.False(failedPrecondition.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedPrecondition.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_GetObject_IfMatchTakesPrecedenceOverIfUnmodifiedSince()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "docs" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello integrated s3"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "docs",
            Key = "precedence.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);
        var currentETag = $"\"{putResult.Value!.ETag}\"";
        var lastModifiedUtc = putResult.Value.LastModifiedUtc;

        var result = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "precedence.txt",
            IfMatchETag = currentETag,
            IfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        });

        Assert.True(result.IsSuccess);
        await using var response = result.Value!;
        Assert.False(response.IsNotModified);
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal("hello integrated s3", await reader.ReadToEndAsync());

        var failedMixedConditions = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "docs",
            Key = "precedence.txt",
            IfMatchETag = "\"different\"",
            IfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        });

        Assert.False(failedMixedConditions.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedMixedConditions.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_GetAndHeadObject_RejectServerSideEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "encrypted-reads"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("read me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "encrypted-reads",
            Key = "docs/object.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "encrypted-reads",
            Key = "docs/object.txt",
            ServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(getResult.IsSuccess);
        AssertUnsupportedServerSideEncryption(getResult.Error, "encrypted-reads", "docs/object.txt");

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "encrypted-reads",
            Key = "docs/object.txt",
            ServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(headResult.IsSuccess);
        AssertUnsupportedServerSideEncryption(headResult.Error, "encrypted-reads", "docs/object.txt");

        var plainHeadResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "encrypted-reads",
            Key = "docs/object.txt"
        });

        Assert.True(plainHeadResult.IsSuccess);
        Assert.Equal(putResult.Value!.VersionId, plainHeadResult.Value!.VersionId);
    }

    [Fact]
    public async Task DiskStorage_CopiesObjectsAndPreservesMetadata()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["origin"] = "tests"
            },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["copied"] = "true"
            }
        });

        var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt"
        });

        Assert.True(copyResult.IsSuccess);
        Assert.Equal("text/plain", copyResult.Value!.ContentType);
        Assert.Equal("tests", copyResult.Value.Metadata!["origin"]);
        Assert.Equal("true", copyResult.Value.Tags!["copied"]);
        Assert.Equal(ComputeSha256Base64("copy me"), copyResult.Value.Checksums!["sha256"]);
        Assert.Equal(ComputeCrc32cBase64("copy me"), copyResult.Value!.Checksums!["crc32c"]);

        var downloaded = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "target",
            Key = "docs/copied.txt"
        });

        Assert.True(downloaded.IsSuccess);
        await using var response = downloaded.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal("copy me", await reader.ReadToEndAsync());
        Assert.Equal("tests", response.Object.Metadata!["origin"]);
        Assert.Equal("true", response.Object.Tags!["copied"]);
        Assert.Equal(ComputeSha256Base64("copy me"), response.Object.Checksums!["sha256"]);
        Assert.Equal(ComputeCrc32cBase64("copy me"), response.Object.Checksums!["crc32c"]);
        Assert.NotEqual(putResult.Value!.BucketName, copyResult.Value.BucketName);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_SelfCopyWithMetadataDirectiveReplace_UpdatesMtimeMetadataWithoutChangingContent()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["mtime"] = "1700000000",
                ["source-only"] = "remove-me"
            }
        });

        Assert.True(putResult.IsSuccess);

        var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "source",
            DestinationKey = "docs/source.txt",
            MetadataDirective = CopyObjectMetadataDirective.Replace,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["mtime"] = "1712345678",
                ["updated-by"] = "rclone"
            }
        });

        Assert.True(copyResult.IsSuccess);
        Assert.Equal("text/plain", copyResult.Value!.ContentType);
        Assert.NotNull(copyResult.Value.Metadata);
        Assert.Equal("1712345678", copyResult.Value.Metadata["mtime"]);
        Assert.Equal("rclone", copyResult.Value.Metadata["updated-by"]);
        Assert.False(copyResult.Value.Metadata.ContainsKey("source-only"));
        Assert.Equal(ComputeSha256Base64("copy me"), copyResult.Value.Checksums!["sha256"]);
        Assert.Equal(ComputeCrc32cBase64("copy me"), copyResult.Value.Checksums["crc32c"]);
        Assert.NotEqual(putResult.Value!.VersionId, copyResult.Value.VersionId);

        var downloaded = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt"
        });

        Assert.True(downloaded.IsSuccess);
        await using var response = downloaded.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal("copy me", await reader.ReadToEndAsync());
        Assert.Equal("text/plain", response.Object.ContentType);
        Assert.NotNull(response.Object.Metadata);
        Assert.Equal("1712345678", response.Object.Metadata["mtime"]);
        Assert.Equal("rclone", response.Object.Metadata["updated-by"]);
        Assert.False(response.Object.Metadata.ContainsKey("source-only"));
    }

    [Fact]
    public async Task DiskStorage_CopyObject_WithChecksumAlgorithm_RecomputesRequestedDestinationChecksum()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";
        // The first (non-final) part must be at least the S3 minimum of 5 MiB.
        var part1Payload = new string('a', 5 * 1024 * 1024);
        const string part2Payload = "world";
        var fullPayload = part1Payload + part2Payload;

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "source",
            Key = sourceKey,
            ContentType = "text/plain",
            ChecksumAlgorithm = "SHA256"
        });

        Assert.True(initiateResult.IsSuccess);

        var part1Checksum = ComputeSha256Base64(part1Payload);
        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes(part1Payload));
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "source",
            Key = sourceKey,
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = part1Stream,
            ChecksumAlgorithm = "SHA256",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = part1Checksum
            }
        });

        var part2Checksum = ComputeSha256Base64(part2Payload);
        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes(part2Payload));
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "source",
            Key = sourceKey,
            UploadId = initiateResult.Value.UploadId,
            PartNumber = 2,
            Content = part2Stream,
            ChecksumAlgorithm = "SHA256",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = part2Checksum
            }
        });

        Assert.True(part1.IsSuccess);
        Assert.True(part2.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "source",
            Key = sourceKey,
            UploadId = initiateResult.Value.UploadId,
            Parts = [part1.Value!, part2.Value!]
        });

        Assert.True(completeResult.IsSuccess);
        Assert.NotNull(completeResult.Value!.Checksums);
        Assert.Equal(part1Checksum, part1.Value!.Checksums!["sha256"]);
        Assert.Equal(part2Checksum, part2.Value!.Checksums!["sha256"]);
        Assert.True(completeResult.Value.Checksums.ContainsKey("sha256"));
        Assert.False(completeResult.Value.Checksums.ContainsKey("crc32c"));

        var expectedCopyChecksum = ComputeCrc32cBase64(fullPayload);
        var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = sourceKey,
            DestinationBucketName = "target",
            DestinationKey = destinationKey,
            ChecksumAlgorithm = "CRC32C"
        });

        Assert.True(copyResult.IsSuccess);
        Assert.NotNull(copyResult.Value!.Checksums);
        Assert.Single(copyResult.Value.Checksums);
        Assert.Equal(expectedCopyChecksum, copyResult.Value.Checksums["crc32c"]);
        Assert.False(copyResult.Value.Checksums.ContainsKey("sha256"));

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "target",
            Key = destinationKey
        });

        Assert.True(getResult.IsSuccess);
        await using var response = getResult.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal(fullPayload, await reader.ReadToEndAsync());
        Assert.NotNull(response.Object.Checksums);
        Assert.Single(response.Object.Checksums!);
        Assert.Equal(expectedCopyChecksum, response.Object.Checksums["crc32c"]);
        Assert.False(response.Object.Checksums.ContainsKey("sha256"));
    }

    [Fact]
    public async Task DiskStorage_CopyObject_WithReplaceTaggingDirective_UsesReplacementTags()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "source",
                ["owner"] = "original"
            }
        });

        Assert.True(putResult.IsSuccess);

        var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt",
            TaggingDirective = ObjectTaggingDirective.Replace,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "target",
                ["owner"] = "copilot"
            }
        });

        Assert.True(copyResult.IsSuccess);
        Assert.Equal("target", copyResult.Value!.Tags!["environment"]);
        Assert.Equal("copilot", copyResult.Value.Tags["owner"]);

        var getTagsResult = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "target",
            Key = "docs/copied.txt"
        });

        Assert.True(getTagsResult.IsSuccess);
        Assert.Equal("target", getTagsResult.Value!.Tags["environment"]);
        Assert.Equal("copilot", getTagsResult.Value.Tags["owner"]);
        Assert.DoesNotContain("original", getTagsResult.Value.Tags.Values);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_WithReplaceTaggingDirective_RejectsInvalidTagCharacters()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "copy-invalid-source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "copy-invalid-target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "copy-invalid-source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "copy-invalid-source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "copy-invalid-target",
            DestinationKey = "docs/copied.txt",
            TaggingDirective = ObjectTaggingDirective.Replace,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "blue,green"
            }
        });

        Assert.False(copyResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidTag, copyResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_RejectsServerSideEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var sourceEncryptedCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/source-encrypted.txt",
            SourceServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(sourceEncryptedCopy.IsSuccess);
        AssertUnsupportedServerSideEncryption(sourceEncryptedCopy.Error, "source", "docs/source.txt");

        var destinationEncryptedCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/destination-encrypted.txt",
            DestinationServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(destinationEncryptedCopy.IsSuccess);
        AssertUnsupportedServerSideEncryption(destinationEncryptedCopy.Error, "target", "docs/destination-encrypted.txt");

        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/source-encrypted.txt"
        })).IsSuccess);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/destination-encrypted.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_HonorsSourcePreconditions()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        var failedCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt",
            SourceIfMatchETag = "\"different\""
        });

        Assert.False(failedCopy.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedCopy.Error!.Code);

        var failedIfNoneMatchCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/copied.txt",
            SourceIfNoneMatchETag = $"\"{putResult.Value!.ETag}\""
        });

        Assert.False(failedIfNoneMatchCopy.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedIfNoneMatchCopy.Error!.Code);

        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/copied.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_SourceIfMatchTakesPrecedenceOverSourceIfUnmodifiedSince()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);
        var currentETag = $"\"{putResult.Value!.ETag}\"";
        var lastModifiedUtc = putResult.Value.LastModifiedUtc;

        var successfulCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/precedence-copy.txt",
            SourceIfMatchETag = currentETag,
            SourceIfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        });

        Assert.True(successfulCopy.IsSuccess);

        var copiedObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "target",
            Key = "docs/precedence-copy.txt"
        });

        Assert.True(copiedObject.IsSuccess);
        await using (var response = copiedObject.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8);
            Assert.Equal("copy me", await reader.ReadToEndAsync());
        }

        var failedMixedConditions = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/blocked-copy.txt",
            SourceIfMatchETag = "\"different\"",
            SourceIfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        });

        Assert.False(failedMixedConditions.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedMixedConditions.Error!.Code);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/blocked-copy.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_SourceIfNoneMatchTakesPrecedenceOverSourceIfModifiedSince()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);
        var currentETag = $"\"{putResult.Value!.ETag}\"";
        var lastModifiedUtc = putResult.Value.LastModifiedUtc;

        var failedMixedConditions = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/blocked-copy.txt",
            SourceIfNoneMatchETag = currentETag,
            SourceIfModifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        });

        Assert.False(failedMixedConditions.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedMixedConditions.Error!.Code);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/blocked-copy.txt"
        })).IsSuccess);

        var successfulCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/precedence-copy.txt",
            SourceIfNoneMatchETag = "\"different\"",
            SourceIfModifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        });

        Assert.True(successfulCopy.IsSuccess);

        var copiedObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "target",
            Key = "docs/precedence-copy.txt"
        });

        Assert.True(copiedObject.IsSuccess);
        await using (var response = copiedObject.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8);
            Assert.Equal("copy me", await reader.ReadToEndAsync());
        }
    }

    [Fact]
    public async Task DiskStorage_CopyObject_SourceVersionIdCanTargetHistoricalVersionsAndRejectDeleteMarkers()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "source",
            EnableVersioning = true
        })).IsSuccess);
        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "target"
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        Assert.True(v1Put.IsSuccess);
        Assert.True(v2Put.IsSuccess);
        Assert.NotEqual(v1Put.Value!.VersionId, v2Put.Value!.VersionId);

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "source",
            Key = "docs/source.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);

        var deleteMarkerVersionId = Assert.IsType<string>(deleteCurrent.Value!.VersionId);
        var deleteMarkerVersion = Assert.Single(await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "source",
            Prefix = "docs/source.txt"
        }).Where(static version => version.IsDeleteMarker).ToArrayAsync());

        var historicalCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            SourceVersionId = v1Put.Value.VersionId,
            DestinationBucketName = "target",
            DestinationKey = "docs/historical-copy.txt"
        });

        Assert.True(historicalCopy.IsSuccess);

        var historicalCopyGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "target",
            Key = "docs/historical-copy.txt"
        });

        Assert.True(historicalCopyGet.IsSuccess);
        await using (var response = historicalCopyGet.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8);
            Assert.Equal("version one", await reader.ReadToEndAsync());
        }

        var currentCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target",
            DestinationKey = "docs/current-copy.txt"
        });

        Assert.False(currentCopy.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.ObjectNotFound, currentCopy.Error!.Code);
        Assert.True(currentCopy.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, currentCopy.Error.VersionId);
        Assert.Null(currentCopy.Error.LastModifiedUtc);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/current-copy.txt"
        })).IsSuccess);

        var explicitCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source",
            SourceKey = "docs/source.txt",
            SourceVersionId = deleteMarkerVersionId,
            DestinationBucketName = "target",
            DestinationKey = "docs/versioned-copy.txt"
        });

        Assert.False(explicitCopy.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.MethodNotAllowed, explicitCopy.Error!.Code);
        Assert.True(explicitCopy.Error.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, explicitCopy.Error.VersionId);
        Assert.Equal(deleteMarkerVersion.LastModifiedUtc, explicitCopy.Error.LastModifiedUtc);
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "target",
            Key = "docs/versioned-copy.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_ObjectTags_RoundTripAndFlowThroughHeadObject()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "tags" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("tag me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "tags",
            Key = "docs/tagged.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "tags",
            Key = "docs/tagged.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "test",
                ["owner"] = "copilot"
            }
        });

        Assert.True(putTagsResult.IsSuccess);
        Assert.Equal("test", putTagsResult.Value!.Tags["environment"]);

        var getTagsResult = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "tags",
            Key = "docs/tagged.txt"
        });

        Assert.True(getTagsResult.IsSuccess);
        Assert.Equal("copilot", getTagsResult.Value!.Tags["owner"]);

        var headObjectResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "tags",
            Key = "docs/tagged.txt"
        });

        Assert.True(headObjectResult.IsSuccess);
        Assert.Equal("test", headObjectResult.Value!.Tags!["environment"]);
    }

    [Fact]
    public async Task DiskStorage_PutObject_WithTags_PersistsOnWrite()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "tagged-put" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("tagged write"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "tagged-put",
            Key = "docs/object.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["café+owner"] = "München 𐐀 + copilot",
                ["release/team"] = "v1.0"
            }
        });

        Assert.True(putResult.IsSuccess);
        Assert.Equal("München 𐐀 + copilot", putResult.Value!.Tags!["café+owner"]);

        var getTagsResult = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "tagged-put",
            Key = "docs/object.txt"
        });

        Assert.True(getTagsResult.IsSuccess);
        Assert.Equal("v1.0", getTagsResult.Value!.Tags["release/team"]);
    }

    [Fact]
    public async Task DiskStorage_PutObject_RejectsInvalidTagCharacters()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "invalid-put-tags"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("tagged payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "invalid-put-tags",
            Key = "docs/tagged.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "blue,green"
            }
        });

        Assert.False(putResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidTag, putResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PutObjectTags_RejectsInvalidTagSets()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "invalid-tags"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("tagged payload"));
        Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "invalid-tags",
            Key = "docs/tagged.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        })).IsSuccess);

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "invalid-tags",
            Key = "docs/tagged.txt",
            Tags = Enumerable.Range(0, 11).ToDictionary(
                static index => $"tag-{index}",
                static index => $"value-{index}",
                StringComparer.Ordinal)
        });

        Assert.False(putTagsResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidTag, putTagsResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PutObjectTags_RejectsInvalidTagCharacters()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "invalid-tag-characters"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("tagged payload"));
        Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "invalid-tag-characters",
            Key = "docs/tagged.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        })).IsSuccess);

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "invalid-tag-characters",
            Key = "docs/tagged.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "blue,green"
            }
        });

        Assert.False(putTagsResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidTag, putTagsResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_DeleteObjectTags_ClearsCurrentAndHistoricalVersionTagsIndependently()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "delete-tags",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });
        Assert.True(v1Put.IsSuccess);

        var putV1Tags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            VersionId = v1Put.Value!.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["generation"] = "one"
            }
        });
        Assert.True(putV1Tags.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, putV1Tags.Value!.VersionId);

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });
        Assert.True(v2Put.IsSuccess);

        var putCurrentTags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["generation"] = "two"
            }
        });
        Assert.True(putCurrentTags.IsSuccess);
        Assert.Equal(v2Put.Value!.VersionId, putCurrentTags.Value!.VersionId);

        var deleteCurrentTags = await storageService.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt"
        });
        Assert.True(deleteCurrentTags.IsSuccess);
        Assert.Equal(v2Put.Value.VersionId, deleteCurrentTags.Value!.VersionId);
        Assert.Empty(deleteCurrentTags.Value.Tags);

        var currentTags = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt"
        });
        Assert.True(currentTags.IsSuccess);
        Assert.Equal(v2Put.Value.VersionId, currentTags.Value!.VersionId);
        Assert.Empty(currentTags.Value.Tags);

        var historicalTagsBeforeDelete = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            VersionId = v1Put.Value.VersionId
        });
        Assert.True(historicalTagsBeforeDelete.IsSuccess);
        Assert.Equal("one", historicalTagsBeforeDelete.Value!.Tags["generation"]);

        var deleteHistoricalTags = await storageService.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            VersionId = v1Put.Value.VersionId
        });
        Assert.True(deleteHistoricalTags.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, deleteHistoricalTags.Value!.VersionId);
        Assert.Empty(deleteHistoricalTags.Value.Tags);

        var historicalTagsAfterDelete = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "delete-tags",
            Key = "docs/tagged.txt",
            VersionId = v1Put.Value.VersionId
        });
        Assert.True(historicalTagsAfterDelete.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, historicalTagsAfterDelete.Value!.VersionId);
        Assert.Empty(historicalTagsAfterDelete.Value.Tags);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_CompletesObjectAndPreservesMetadata()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt",
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "multipart"
            },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["upload"] = "multipart"
            }
        });

        Assert.True(initiateResult.IsSuccess);

        // The first (non-final) part must be at least the S3 minimum of 5 MiB.
        var part1Payload = new string('a', 5 * 1024 * 1024);
        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes(part1Payload));
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = part1Stream
        });

        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes("world"));
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 2,
            Content = part2Stream
        });

        Assert.True(part1.IsSuccess);
        Assert.True(part2.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value!.UploadId,
            Parts = [part1.Value!, part2.Value!]
        });

        Assert.True(completeResult.IsSuccess);
        Assert.Equal("text/plain", completeResult.Value!.ContentType);
        Assert.Equal("multipart", completeResult.Value.Metadata!["source"]);
        Assert.Equal("multipart", completeResult.Value.Tags!["upload"]);
        var multipartChecksum = Assert.IsType<string>(completeResult.Value.Checksums!["sha256"]);
        Assert.False(string.IsNullOrWhiteSpace(multipartChecksum));

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "multipart",
            Key = "docs/assembled.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using var response = getResult.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal(part1Payload + "world", await reader.ReadToEndAsync());
        Assert.Equal("multipart", response.Object.Metadata!["source"]);
        Assert.Equal("multipart", response.Object.Tags!["upload"]);
        Assert.Equal(multipartChecksum, response.Object.Checksums!["sha256"]);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_RejectsUndersizedNonFinalPartWithEntityTooSmall()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-small" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-small",
            Key = "docs/tiny.bin",
            ContentType = "application/octet-stream"
        });

        Assert.True(initiateResult.IsSuccess);

        // Part 1 is a non-final part smaller than the S3 minimum of 5 MiB.
        await using var part1Stream = new MemoryStream(new byte[1024]);
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-small",
            Key = "docs/tiny.bin",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = part1Stream
        });

        await using var part2Stream = new MemoryStream(new byte[1024]);
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-small",
            Key = "docs/tiny.bin",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 2,
            Content = part2Stream
        });

        Assert.True(part1.IsSuccess);
        Assert.True(part2.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-small",
            Key = "docs/tiny.bin",
            UploadId = initiateResult.Value!.UploadId,
            Parts = [part1.Value!, part2.Value!]
        });

        Assert.False(completeResult.IsSuccess);
        Assert.Equal(StorageErrorCode.MultipartConflict, completeResult.Error!.Code);
        Assert.Equal(400, completeResult.Error.SuggestedHttpStatusCode);
        Assert.Contains("minimum", completeResult.Error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'1'", completeResult.Error.Message, StringComparison.Ordinal);

        // The object must not have been created.
        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "multipart-small",
            Key = "docs/tiny.bin"
        });

        Assert.False(headResult.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, headResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_AllowsSmallFinalPartWhenEarlierPartsMeetMinimum()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-lastsmall" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-lastsmall",
            Key = "docs/assembled.bin",
            ContentType = "application/octet-stream"
        });

        Assert.True(initiateResult.IsSuccess);

        // Part 1 exactly meets the 5 MiB minimum; part 2 (the last part) is deliberately tiny.
        var firstPartBytes = new byte[5 * 1024 * 1024];
        await using var part1Stream = new MemoryStream(firstPartBytes);
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-lastsmall",
            Key = "docs/assembled.bin",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = part1Stream
        });

        await using var part2Stream = new MemoryStream(new byte[16]);
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-lastsmall",
            Key = "docs/assembled.bin",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 2,
            Content = part2Stream
        });

        Assert.True(part1.IsSuccess);
        Assert.True(part2.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-lastsmall",
            Key = "docs/assembled.bin",
            UploadId = initiateResult.Value!.UploadId,
            Parts = [part1.Value!, part2.Value!]
        });

        Assert.True(completeResult.IsSuccess);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "multipart-lastsmall",
            Key = "docs/assembled.bin"
        });

        Assert.True(getResult.IsSuccess);
        await using var response = getResult.Value!;
        Assert.Equal((5 * 1024 * 1024) + 16, response.Object.ContentLength);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_WithCrc32cChecksum_ComputesCompositeChecksum()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-crc32c" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-crc32c",
            Key = "docs/assembled.txt",
            ContentType = "text/plain",
            ChecksumAlgorithm = "CRC32C"
        });

        Assert.True(initiateResult.IsSuccess);

        // The first (non-final) part must be at least the S3 minimum of 5 MiB.
        var part1Payload = new string('a', 5 * 1024 * 1024);
        var part1Checksum = ComputeCrc32cBase64(part1Payload);
        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes(part1Payload));
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-crc32c",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = part1Stream,
            ChecksumAlgorithm = "CRC32C",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["crc32c"] = part1Checksum
            }
        });

        const string part2Payload = "world";
        var part2Checksum = ComputeCrc32cBase64(part2Payload);
        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes(part2Payload));
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-crc32c",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value.UploadId,
            PartNumber = 2,
            Content = part2Stream,
            ChecksumAlgorithm = "CRC32C",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["crc32c"] = part2Checksum
            }
        });

        Assert.True(part1.IsSuccess);
        Assert.True(part2.IsSuccess);
        Assert.Equal(part1Checksum, part1.Value!.Checksums!["crc32c"]);
        Assert.Equal(part2Checksum, part2.Value!.Checksums!["crc32c"]);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-crc32c",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value!.UploadId,
            Parts = [part1.Value!, part2.Value!]
        });

        Assert.True(completeResult.IsSuccess);
        var compositeChecksum = ComputeMultipartCrc32cBase64(part1Checksum, part2Checksum);
        Assert.Equal(compositeChecksum, completeResult.Value!.Checksums!["crc32c"]);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "multipart-crc32c",
            Key = "docs/assembled.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using var response = getResult.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal(part1Payload + "world", await reader.ReadToEndAsync());
        Assert.Equal(compositeChecksum, response.Object.Checksums!["crc32c"]);
    }

    [Fact]
    public async Task DiskStorage_InitiateMultipartUpload_RejectsInvalidTagCharacters()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-invalid-tags" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-invalid-tags",
            Key = "docs/assembled.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["upload"] = "blue,green"
            }
        });

        Assert.False(initiateResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidTag, initiateResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_UploadPartCopy_CopiesHistoricalRangeAndCompletesMultipartObject()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-copy-source" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-copy-destination" });

        var versioningResult = await storageService.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "multipart-copy-source",
            Status = BucketVersioningStatus.Enabled
        });
        Assert.True(versioningResult.IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("hello world"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "multipart-copy-source",
            Key = "docs/source.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });
        Assert.True(v1Put.IsSuccess);

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("goodbye world"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "multipart-copy-source",
            Key = "docs/source.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });
        Assert.True(v2Put.IsSuccess);
        Assert.NotEqual(v1Put.Value!.VersionId, v2Put.Value!.VersionId);

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-copy-destination",
            Key = "docs/copied.txt",
            ChecksumAlgorithm = "SHA256"
        });
        Assert.True(initiateResult.IsSuccess);

        var copiedPart = await storageService.UploadPartCopyAsync(new UploadPartCopyRequest
        {
            BucketName = "multipart-copy-destination",
            Key = "docs/copied.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            SourceBucketName = "multipart-copy-source",
            SourceKey = "docs/source.txt",
            SourceVersionId = v1Put.Value.VersionId,
            SourceIfMatchETag = v1Put.Value.ETag,
            SourceRange = new ObjectRange
            {
                Start = 6,
                End = 10
            }
        });

        Assert.True(copiedPart.IsSuccess);
        Assert.Equal(1, copiedPart.Value!.PartNumber);
        Assert.Equal(5, copiedPart.Value.ContentLength);
        Assert.Equal(ComputeSha256Base64("world"), copiedPart.Value.Checksums!["sha256"]);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-copy-destination",
            Key = "docs/copied.txt",
            UploadId = initiateResult.Value.UploadId,
            Parts = [copiedPart.Value]
        });

        Assert.True(completeResult.IsSuccess);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "multipart-copy-destination",
            Key = "docs/copied.txt"
        });
        Assert.True(getResult.IsSuccess);
        await using var copiedResponse = getResult.Value!;
        using var copiedReader = new StreamReader(copiedResponse.Content, Encoding.UTF8);
        Assert.Equal("world", await copiedReader.ReadToEndAsync());
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_UploadPartCopy_HonorsSourcePreconditions()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-copy-preconditions" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-copy-preconditions-destination" });

        await using var sourceStream = new MemoryStream(Encoding.UTF8.GetBytes("copy preconditions"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "multipart-copy-preconditions",
            Key = "docs/source.txt",
            Content = sourceStream,
            ContentType = "text/plain"
        });
        Assert.True(putResult.IsSuccess);

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-copy-preconditions-destination",
            Key = "docs/copied.txt"
        });
        Assert.True(initiateResult.IsSuccess);

        var copiedPart = await storageService.UploadPartCopyAsync(new UploadPartCopyRequest
        {
            BucketName = "multipart-copy-preconditions-destination",
            Key = "docs/copied.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            SourceBucketName = "multipart-copy-preconditions",
            SourceKey = "docs/source.txt",
            SourceIfMatchETag = "\"different\""
        });

        Assert.False(copiedPart.IsSuccess);
        Assert.Equal(StorageErrorCode.PreconditionFailed, copiedPart.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_RejectsServerSideEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-sse" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-sse",
            Key = "docs/assembled.txt",
            ServerSideEncryption = CreateServerSideEncryptionSettings()
        });

        Assert.False(initiateResult.IsSuccess);
        AssertUnsupportedServerSideEncryption(initiateResult.Error, "multipart-sse", "docs/assembled.txt");

        var uploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-sse"
        }).ToArrayAsync();

        Assert.Empty(uploads);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_CanCopyPartRangeWithPreconditions()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-copy" });

        var sourcePut = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/source.txt",
            Content = new MemoryStream(Encoding.UTF8.GetBytes("0123456789")),
            ContentType = "text/plain"
        });
        Assert.True(sourcePut.IsSuccess);

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt",
            ContentType = "text/plain",
            ChecksumAlgorithm = "SHA256"
        });
        Assert.True(initiateResult.IsSuccess);

        var copiedPart = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            CopySourceBucketName = "multipart-copy",
            CopySourceKey = "docs/source.txt",
            CopySourceIfMatchETag = sourcePut.Value!.ETag,
            CopySourceRange = new ObjectRange
            {
                Start = 2,
                End = 6
            }
        });

        Assert.True(copiedPart.IsSuccess);
        Assert.False(string.IsNullOrWhiteSpace(copiedPart.Value!.Checksums!["sha256"]));

        var failedPart = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt",
            UploadId = initiateResult.Value.UploadId,
            PartNumber = 2,
            CopySourceBucketName = "multipart-copy",
            CopySourceKey = "docs/source.txt",
            CopySourceIfMatchETag = "\"different\""
        });

        Assert.False(failedPart.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.PreconditionFailed, failedPart.Error!.Code);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt",
            UploadId = initiateResult.Value.UploadId,
            Parts = [copiedPart.Value]
        });

        Assert.True(completeResult.IsSuccess);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "multipart-copy",
            Key = "docs/copied.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using var response = getResult.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        Assert.Equal("23456", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_CanBeAborted()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-abort" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-abort",
            Key = "docs/aborted.txt"
        });

        Assert.True(initiateResult.IsSuccess);

        await using var partStream = new MemoryStream(Encoding.UTF8.GetBytes("temporary"));
        var uploadPartResult = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-abort",
            Key = "docs/aborted.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = partStream
        });

        Assert.True(uploadPartResult.IsSuccess);

        var abortResult = await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "multipart-abort",
            Key = "docs/aborted.txt",
            UploadId = initiateResult.Value.UploadId
        });

        Assert.True(abortResult.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-abort",
            Key = "docs/aborted.txt",
            UploadId = initiateResult.Value.UploadId,
            Parts = [uploadPartResult.Value!]
        });

        Assert.False(completeResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.NoSuchUpload, completeResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_AbortTwice_IsIdempotent()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-abort-twice" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-abort-twice",
            Key = "docs/aborted.txt"
        });
        Assert.True(initiateResult.IsSuccess);

        await using var partStream = new MemoryStream(Encoding.UTF8.GetBytes("temporary"));
        var uploadPartResult = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-abort-twice",
            Key = "docs/aborted.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = partStream
        });
        Assert.True(uploadPartResult.IsSuccess);

        var firstAbort = await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "multipart-abort-twice",
            Key = "docs/aborted.txt",
            UploadId = initiateResult.Value.UploadId
        });
        Assert.True(firstAbort.IsSuccess);

        // A repeated Abort on an upload whose directory was already removed must not throw a
        // DirectoryNotFoundException; it should return a deterministic NoSuchUpload failure.
        var secondAbort = await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "multipart-abort-twice",
            Key = "docs/aborted.txt",
            UploadId = initiateResult.Value.UploadId
        });

        Assert.False(secondAbort.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.NoSuchUpload, secondAbort.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_AbortAfterComplete_IsIdempotent()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-abort-after-complete" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-abort-after-complete",
            Key = "docs/assembled.txt"
        });
        Assert.True(initiateResult.IsSuccess);

        await using var partStream = new MemoryStream(Encoding.UTF8.GetBytes("completed"));
        var uploadPartResult = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-abort-after-complete",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = partStream
        });
        Assert.True(uploadPartResult.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-abort-after-complete",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value.UploadId,
            Parts = [uploadPartResult.Value!]
        });
        Assert.True(completeResult.IsSuccess);

        // Complete already cleaned up the upload directory. A follow-up Abort (e.g. a client retry)
        // must be a benign no-op returning NoSuchUpload rather than throwing on the missing directory.
        var abortResult = await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "multipart-abort-after-complete",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value.UploadId
        });

        Assert.False(abortResult.IsSuccess);
        Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.NoSuchUpload, abortResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ConcurrentAbortAndComplete_NeverThrowsAndStaysConsistent()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-abort-race" });

        // Many iterations to shake out interleavings between a lock-holding Complete and a
        // concurrent Abort on the same bucket/key/uploadId. Each iteration must terminate with a
        // clean StorageResult on both operations — never a thrown DirectoryNotFoundException/500.
        for (var iteration = 0; iteration < 60; iteration++)
        {
            var key = $"docs/race-{iteration}.txt";

            var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            {
                BucketName = "multipart-abort-race",
                Key = key
            });
            Assert.True(initiateResult.IsSuccess);
            var uploadId = initiateResult.Value!.UploadId;

            await using (var partStream = new MemoryStream(Encoding.UTF8.GetBytes($"payload-{iteration}")))
            {
                var uploadPartResult = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
                {
                    BucketName = "multipart-abort-race",
                    Key = key,
                    UploadId = uploadId,
                    PartNumber = 1,
                    Content = partStream
                });
                Assert.True(uploadPartResult.IsSuccess);

                var completeTask = Task.Run(async () => await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
                {
                    BucketName = "multipart-abort-race",
                    Key = key,
                    UploadId = uploadId,
                    Parts = [uploadPartResult.Value!]
                }));

                var abortTask = Task.Run(async () => await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                {
                    BucketName = "multipart-abort-race",
                    Key = key,
                    UploadId = uploadId
                }));

                // Neither operation may throw; both must resolve to a StorageResult. If either threw,
                // Task.WhenAll rethrows here and the test fails, which is precisely the bug we guard.
                var completeResult = await completeTask;
                var abortResult = await abortTask;

                // Every outcome must be either a clean success or a clean, well-defined failure.
                Assert.True(
                    completeResult.IsSuccess
                        || completeResult.Error!.Code is IntegratedS3.Abstractions.Errors.StorageErrorCode.NoSuchUpload
                            or IntegratedS3.Abstractions.Errors.StorageErrorCode.MultipartConflict
                            or IntegratedS3.Abstractions.Errors.StorageErrorCode.InvalidPart,
                    $"Unexpected Complete outcome: {completeResult.Error?.Code}");
                Assert.True(
                    abortResult.IsSuccess
                        || abortResult.Error!.Code == IntegratedS3.Abstractions.Errors.StorageErrorCode.NoSuchUpload,
                    $"Unexpected Abort outcome: {abortResult.Error?.Code}");
            }

            // A follow-up Abort of the same upload id must always be a clean no-op (NoSuchUpload),
            // proving no orphaned upload directory/state survived the race.
            var trailingAbort = await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = "multipart-abort-race",
                Key = key,
                UploadId = uploadId
            });
            Assert.False(trailingAbort.IsSuccess);
            Assert.Equal(IntegratedS3.Abstractions.Errors.StorageErrorCode.NoSuchUpload, trailingAbort.Error!.Code);
        }
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploads_AppliesPrefixMarkersAndPageSize()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-list" });

        var firstUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-list",
            Key = "docs/alpha.txt"
        });
        await Task.Delay(2);

        var secondUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-list",
            Key = "docs/alpha.txt"
        });
        await Task.Delay(2);

        var thirdUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-list",
            Key = "docs/beta.txt"
        });

        await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-list",
            Key = "videos/clip.txt"
        });

        Assert.True(firstUpload.IsSuccess);
        Assert.True(secondUpload.IsSuccess);
        Assert.True(thirdUpload.IsSuccess);

        var firstPage = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-list",
            Prefix = "docs/",
            PageSize = 2
        }).ToArrayAsync();

        Assert.Collection(
            firstPage,
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal(firstUpload.Value!.UploadId, upload.UploadId);
            },
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal(secondUpload.Value!.UploadId, upload.UploadId);
            });

        var secondPage = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-list",
            Prefix = "docs/",
            KeyMarker = "docs/alpha.txt",
            UploadIdMarker = secondUpload.Value!.UploadId
        }).ToArrayAsync();

        var remainingUpload = Assert.Single(secondPage);
        Assert.Equal("docs/beta.txt", remainingUpload.Key);
        Assert.Equal(thirdUpload.Value!.UploadId, remainingUpload.UploadId);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListParts_AppliesPartNumberMarkerAndPageSize()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-parts" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-parts",
            Key = "docs/assembled.txt",
            ChecksumAlgorithm = "SHA256"
        });

        Assert.True(initiateResult.IsSuccess);

        async Task<MultipartUploadPart> UploadPartAsync(int partNumber, string payload)
        {
            var checksum = ComputeSha256Base64(payload);
            await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
            var result = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
            {
                BucketName = "multipart-parts",
                Key = "docs/assembled.txt",
                UploadId = initiateResult.Value!.UploadId,
                PartNumber = partNumber,
                Content = stream,
                ChecksumAlgorithm = "SHA256",
                Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["sha256"] = checksum
                }
            });

            Assert.True(result.IsSuccess);
            return result.Value!;
        }

        _ = await UploadPartAsync(3, "three");
        _ = await UploadPartAsync(1, "one");
        var secondPart = await UploadPartAsync(2, "two");

        var parts = await storageService.ListMultipartUploadPartsAsync(new ListMultipartUploadPartsRequest
        {
            BucketName = "multipart-parts",
            Key = "docs/assembled.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumberMarker = 1,
            PageSize = 2
        }).ToArrayAsync();

        Assert.Equal([2, 3], parts.Select(static part => part.PartNumber).ToArray());
        Assert.Equal(secondPart.ETag, parts[0].ETag);
        Assert.Equal(3, parts[0].ContentLength);
        Assert.Equal(ComputeSha256Base64("two"), parts[0].Checksums!["sha256"]);
        Assert.Equal(ComputeSha256Base64("three"), parts[1].Checksums!["sha256"]);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploads_ReturnsEmptySequenceWhenBucketHasNoUploads()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-empty" });

        var uploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-empty"
        }).ToArrayAsync();

        Assert.Empty(uploads);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploads_ExcludesCompletedAndAbortedUploads()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-filtered" });

        var pendingUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/pending.txt"
        });
        var completedUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/completed.txt"
        });
        var abortedUpload = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/aborted.txt"
        });

        Assert.True(pendingUpload.IsSuccess);
        Assert.True(completedUpload.IsSuccess);
        Assert.True(abortedUpload.IsSuccess);

        await using var completedPartStream = new MemoryStream(Encoding.UTF8.GetBytes("complete"));
        var completedPart = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/completed.txt",
            UploadId = completedUpload.Value!.UploadId,
            PartNumber = 1,
            Content = completedPartStream
        });

        await using var abortedPartStream = new MemoryStream(Encoding.UTF8.GetBytes("abort"));
        var abortedPart = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/aborted.txt",
            UploadId = abortedUpload.Value!.UploadId,
            PartNumber = 1,
            Content = abortedPartStream
        });

        Assert.True(completedPart.IsSuccess);
        Assert.True(abortedPart.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/completed.txt",
            UploadId = completedUpload.Value.UploadId,
            Parts = [completedPart.Value!]
        });
        Assert.True(completeResult.IsSuccess);

        var abortResult = await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "multipart-filtered",
            Key = "docs/aborted.txt",
            UploadId = abortedUpload.Value.UploadId
        });
        Assert.True(abortResult.IsSuccess);

        var uploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-filtered"
        }).ToArrayAsync();

        var remainingUpload = Assert.Single(uploads);
        Assert.Equal("docs/pending.txt", remainingUpload.Key);
        Assert.Equal(pendingUpload.Value!.UploadId, remainingUpload.UploadId);
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploads_UsesPlatformStateStoreWithoutSidecars()
    {
        await using var fixture = new DiskStorageFixture(services => {
            services.AddSingleton<InMemoryMultipartStateStore>();
            services.AddSingleton<IStorageMultipartStateStore>(static serviceProvider => serviceProvider.GetRequiredService<InMemoryMultipartStateStore>());
        });

        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        var multipartStateStore = fixture.Services.GetRequiredService<InMemoryMultipartStateStore>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-platform-list" });

        var baseInitiatedAtUtc = DateTimeOffset.UtcNow;
        await multipartStateStore.UpsertMultipartUploadStateAsync("test-disk", new MultipartUploadState
        {
            BucketName = "multipart-platform-list",
            Key = "docs/alpha.txt",
            UploadId = "upload-002",
            InitiatedAtUtc = baseInitiatedAtUtc.AddSeconds(1)
        });
        await multipartStateStore.UpsertMultipartUploadStateAsync("test-disk", new MultipartUploadState
        {
            BucketName = "multipart-platform-list",
            Key = "docs/alpha.txt",
            UploadId = "upload-001",
            InitiatedAtUtc = baseInitiatedAtUtc
        });
        await multipartStateStore.UpsertMultipartUploadStateAsync("test-disk", new MultipartUploadState
        {
            BucketName = "multipart-platform-list",
            Key = "docs/nested/beta.txt",
            UploadId = "upload-003",
            InitiatedAtUtc = baseInitiatedAtUtc.AddSeconds(2)
        });

        var uploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-platform-list",
            Prefix = "docs/"
        }).ToArrayAsync();

        Assert.Collection(
            uploads,
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal("upload-001", upload.UploadId);
            },
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal("upload-002", upload.UploadId);
            },
            upload => {
                Assert.Equal("docs/nested/beta.txt", upload.Key);
                Assert.Equal("upload-003", upload.UploadId);
            });
    }

    [Fact]
    public async Task DiskStorage_MultipartUpload_ListMultipartUploadParts_AppliesMarkersAndPageSize()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-parts" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-parts",
            Key = "docs/parts.txt",
            ChecksumAlgorithm = "sha256"
        });

        Assert.True(initiateResult.IsSuccess);

        var partPayloads = new Dictionary<int, string>
        {
            [1] = "alpha",
            [2] = "bravo",
            [3] = "charlie"
        };

        foreach (var (partNumber, payload) in partPayloads) {
            await using var partStream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
            var uploadPartResult = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
            {
                BucketName = "multipart-parts",
                Key = "docs/parts.txt",
                UploadId = initiateResult.Value!.UploadId,
                PartNumber = partNumber,
                Content = partStream,
                ChecksumAlgorithm = "sha256",
                Checksums = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["sha256"] = ComputeSha256Base64(payload)
                }
            });

            Assert.True(uploadPartResult.IsSuccess);
        }

        var firstPage = await storageService.ListMultipartUploadPartsAsync(new ListMultipartUploadPartsRequest
        {
            BucketName = "multipart-parts",
            Key = "docs/parts.txt",
            UploadId = initiateResult.Value!.UploadId,
            PageSize = 2
        }).ToArrayAsync();

        Assert.Collection(
            firstPage,
            part => {
                Assert.Equal(1, part.PartNumber);
                Assert.Equal(ComputeSha256Base64(partPayloads[1]), part.Checksums!["sha256"]);
            },
            part => {
                Assert.Equal(2, part.PartNumber);
                Assert.Equal(ComputeSha256Base64(partPayloads[2]), part.Checksums!["sha256"]);
            });

        var secondPage = await storageService.ListMultipartUploadPartsAsync(new ListMultipartUploadPartsRequest
        {
            BucketName = "multipart-parts",
            Key = "docs/parts.txt",
            UploadId = initiateResult.Value!.UploadId,
            PartNumberMarker = 2
        }).ToArrayAsync();

        var remainingPart = Assert.Single(secondPage);
        Assert.Equal(3, remainingPart.PartNumber);
        Assert.Equal(ComputeSha256Base64(partPayloads[3]), remainingPart.Checksums!["sha256"]);
    }

    [Fact]
    public async Task DiskStorage_UsesRegisteredPlatformObjectStateStoreForMetadataAndTags()
    {
        await using var fixture = new DiskStorageFixture(services => {
            services.AddSingleton<IStorageObjectStateStore, InMemoryObjectStateStore>();
        });

        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "external-state" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("hello external state"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "external-state",
            Key = "docs/external.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["owner"] = "platform-store"
            }
        });

        Assert.True(putResult.IsSuccess);

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "external-state",
            Key = "docs/external.txt",
            Tags = new Dictionary<string, string>
            {
                ["environment"] = "test"
            }
        });

        Assert.True(putTagsResult.IsSuccess);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "external-state",
            Key = "docs/external.txt"
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            Assert.Equal("platform-store", response.Object.Metadata!["owner"]);
            Assert.Equal("test", response.Object.Tags!["environment"]);
            Assert.Equal(ComputeSha256Base64("hello external state"), response.Object.Checksums!["sha256"]);
        }

        var supportState = await storageService.GetSupportStateDescriptorAsync();
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.PlatformManaged, supportState.ObjectMetadata);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.PlatformManaged, supportState.ObjectTags);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.PlatformManaged, supportState.Checksums);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.NotApplicable, supportState.AccessControl);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.NotApplicable, supportState.Retention);
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.NotApplicable, supportState.ServerSideEncryption);

        var capabilities = await storageService.GetCapabilitiesAsync();
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageCapabilitySupport.Unsupported, capabilities.ServerSideEncryption);
        Assert.Empty(capabilities.ServerSideEncryptionDetails.Variants);

        var providerMode = await storageService.GetProviderModeAsync();
        Assert.Equal(IntegratedS3.Abstractions.Models.StorageProviderMode.Hybrid, providerMode);

        var objectLocation = await storageService.GetObjectLocationDescriptorAsync();
        Assert.Equal(IntegratedS3.Abstractions.Models.StorageObjectAccessMode.ProxyStream, objectLocation.DefaultAccessMode);
        Assert.Equal([IntegratedS3.Abstractions.Models.StorageObjectAccessMode.ProxyStream], objectLocation.SupportedAccessModes);

        var sidecarPath = Path.Combine(fixture.RootPath, "external-state", "docs", "external.txt.integrateds3.json");
        Assert.False(File.Exists(sidecarPath));
    }

    [Fact]
    public async Task DiskStorage_UsesRegisteredPlatformObjectStateStoreForHistoricalVersionsAndDeleteMarkers()
    {
        await using var fixture = new DiskStorageFixture(services => {
            services.AddSingleton<IStorageObjectStateStore, InMemoryObjectStateStore>();
        });

        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "external-version-state",
            EnableVersioning = true
        });

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "external-version-state",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "external-version-state",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "external-version-state",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);

        var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "external-version-state",
            Prefix = "docs/history.txt"
        }).ToArrayAsync();

        Assert.Equal(3, versions.Length);
        Assert.Contains(versions, version => version.VersionId == v1Put.Value!.VersionId);
        Assert.Contains(versions, version => version.VersionId == v2Put.Value!.VersionId);
        Assert.Contains(versions, static version => version.IsDeleteMarker && version.IsLatest);

        var versionMetadataFiles = Directory.Exists(Path.Combine(fixture.RootPath, "external-version-state", ".integrateds3-versions"))
            ? Directory.EnumerateFiles(Path.Combine(fixture.RootPath, "external-version-state", ".integrateds3-versions"), "*.integrateds3.json", SearchOption.AllDirectories).ToArray()
            : [];

        Assert.Empty(versionMetadataFiles);
    }

    [Fact]
    public async Task DiskStorage_MigratesLegacyCurrentObjectStateIntoRegisteredStoreAcrossRestarts()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "legacy-current-state"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("legacy current payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["owner"] = "legacy"
            }
        });

        Assert.True(putResult.IsSuccess);

        var putTagsResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "migration"
            }
        });

        Assert.True(putTagsResult.IsSuccess);

        var versionId = putResult.Value!.VersionId;
        var checksum = putResult.Value.Checksums!["sha256"];
        var metadataSidecarPath = Path.Combine(fixture.RootPath, "legacy-current-state", "docs", "current.txt.integrateds3.json");
        Assert.True(File.Exists(metadataSidecarPath));

        var stateStore = new InMemoryObjectStateStore();
        await fixture.RestartAsync(services => {
            services.AddSingleton<IStorageObjectStateStore>(stateStore);
        });

        storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var migratedHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt"
        });

        Assert.True(migratedHead.IsSuccess);
        Assert.Equal(versionId, migratedHead.Value!.VersionId);
        Assert.Equal("legacy", migratedHead.Value.Metadata!["owner"]);
        Assert.Equal("migration", migratedHead.Value.Tags!["environment"]);
        Assert.Equal(checksum, migratedHead.Value.Checksums!["sha256"]);
        Assert.False(File.Exists(metadataSidecarPath));

        var migratedState = await stateStore.GetObjectInfoAsync("test-disk", "legacy-current-state", "docs/current.txt", versionId);
        Assert.NotNull(migratedState);
        Assert.Equal(versionId, migratedState!.VersionId);
        Assert.True(migratedState.IsLatest);
        Assert.False(migratedState.IsDeleteMarker);
        Assert.Equal("legacy", migratedState.Metadata!["owner"]);
        Assert.Equal("migration", migratedState.Tags!["environment"]);
        Assert.Equal(checksum, migratedState.Checksums!["sha256"]);

        await fixture.RestartAsync(services => {
            services.AddSingleton<IStorageObjectStateStore>(stateStore);
        });

        storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var updatedTags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "platform-managed",
                ["phase"] = "after-restart"
            }
        });

        Assert.True(updatedTags.IsSuccess);
        Assert.Equal(versionId, updatedTags.Value!.VersionId);

        var rereadHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "legacy-current-state",
            Key = "docs/current.txt"
        });

        Assert.True(rereadHead.IsSuccess);
        Assert.Equal(versionId, rereadHead.Value!.VersionId);
        Assert.Equal("legacy", rereadHead.Value.Metadata!["owner"]);
        Assert.Equal("platform-managed", rereadHead.Value.Tags!["environment"]);
        Assert.Equal("after-restart", rereadHead.Value.Tags["phase"]);
        Assert.Equal(checksum, rereadHead.Value.Checksums!["sha256"]);
        Assert.False(File.Exists(metadataSidecarPath));
    }

    [Fact]
    public async Task DiskStorage_MigratesLegacyVersionHistoryAndDeleteMarkerStateIntoRegisteredStoreAcrossRestarts()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "legacy-version-state",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("legacy version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["generation"] = "one"
            }
        });

        Assert.True(v1Put.IsSuccess);

        var v1Tags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            VersionId = v1Put.Value!.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["release"] = "one"
            }
        });

        Assert.True(v1Tags.IsSuccess);

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("legacy version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["generation"] = "two"
            }
        });

        Assert.True(v2Put.IsSuccess);

        var v2Tags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["release"] = "two"
            }
        });

        Assert.True(v2Tags.IsSuccess);

        var deleteCurrent = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt"
        });

        Assert.True(deleteCurrent.IsSuccess);
        Assert.True(deleteCurrent.Value!.IsDeleteMarker);

        var deleteMarkerVersionId = deleteCurrent.Value.VersionId;
        var currentDeleteMarkerSidecar = Path.Combine(fixture.RootPath, "legacy-version-state", "docs", "history.txt.integrateds3.json");
        var archivedV1Sidecar = Path.Combine(fixture.RootPath, "legacy-version-state", ".integrateds3-versions", "docs", "history.txt", v1Put.Value.VersionId!, "content.integrateds3.json");
        var archivedV2Sidecar = Path.Combine(fixture.RootPath, "legacy-version-state", ".integrateds3-versions", "docs", "history.txt", v2Put.Value!.VersionId!, "content.integrateds3.json");

        Assert.True(File.Exists(currentDeleteMarkerSidecar));
        Assert.True(File.Exists(archivedV1Sidecar));
        Assert.True(File.Exists(archivedV2Sidecar));

        var stateStore = new InMemoryObjectStateStore();
        await fixture.RestartAsync(services => {
            services.AddSingleton<IStorageObjectStateStore>(stateStore);
        });

        storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "legacy-version-state",
            Prefix = "docs/history.txt"
        }).ToArrayAsync();

        Assert.Equal(3, versions.Length);

        var deleteMarkerVersion = Assert.Single(versions, version => version.VersionId == deleteMarkerVersionId);
        Assert.True(deleteMarkerVersion.IsDeleteMarker);
        Assert.True(deleteMarkerVersion.IsLatest);

        var migratedV1 = Assert.Single(versions, version => version.VersionId == v1Put.Value.VersionId);
        Assert.False(migratedV1.IsDeleteMarker);
        Assert.False(migratedV1.IsLatest);
        Assert.Equal("one", migratedV1.Metadata!["generation"]);
        Assert.Equal("one", migratedV1.Tags!["release"]);
        Assert.Equal(v1Put.Value.Checksums!["sha256"], migratedV1.Checksums!["sha256"]);

        var migratedV2 = Assert.Single(versions, version => version.VersionId == v2Put.Value.VersionId);
        Assert.False(migratedV2.IsDeleteMarker);
        Assert.False(migratedV2.IsLatest);
        Assert.Equal("two", migratedV2.Metadata!["generation"]);
        Assert.Equal("two", migratedV2.Tags!["release"]);
        Assert.Equal(v2Put.Value.Checksums!["sha256"], migratedV2.Checksums!["sha256"]);

        Assert.False(File.Exists(currentDeleteMarkerSidecar));
        Assert.False(File.Exists(archivedV1Sidecar));
        Assert.False(File.Exists(archivedV2Sidecar));

        var updatedHistoricalTags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["release"] = "one-migrated",
                ["migration"] = "complete"
            }
        });

        Assert.True(updatedHistoricalTags.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, updatedHistoricalTags.Value!.VersionId);

        var historicalHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId
        });

        Assert.True(historicalHead.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, historicalHead.Value!.VersionId);
        Assert.Equal("one", historicalHead.Value.Metadata!["generation"]);
        Assert.Equal("one-migrated", historicalHead.Value.Tags!["release"]);
        Assert.Equal("complete", historicalHead.Value.Tags["migration"]);
        Assert.Equal(v1Put.Value.Checksums!["sha256"], historicalHead.Value.Checksums!["sha256"]);

        await fixture.RestartAsync(services => {
            services.AddSingleton<IStorageObjectStateStore>(stateStore);
        });

        storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        var deleteMarkerDelete = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt",
            VersionId = deleteMarkerVersionId
        });

        Assert.True(deleteMarkerDelete.IsSuccess);
        Assert.True(deleteMarkerDelete.Value!.IsDeleteMarker);
        Assert.NotNull(deleteMarkerDelete.Value.CurrentObject);
        Assert.Equal(v2Put.Value.VersionId, deleteMarkerDelete.Value.CurrentObject!.VersionId);

        var currentHead = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "legacy-version-state",
            Key = "docs/history.txt"
        });

        Assert.True(currentHead.IsSuccess);
        Assert.Equal(v2Put.Value.VersionId, currentHead.Value!.VersionId);
        Assert.Equal("two", currentHead.Value.Metadata!["generation"]);
        Assert.Equal("two", currentHead.Value.Tags!["release"]);
        Assert.Equal(v2Put.Value.Checksums!["sha256"], currentHead.Value.Checksums!["sha256"]);
        Assert.False(File.Exists(currentDeleteMarkerSidecar));
        Assert.False(File.Exists(archivedV2Sidecar));

        var currentStoreState = await stateStore.GetObjectInfoAsync("test-disk", "legacy-version-state", "docs/history.txt");
        Assert.NotNull(currentStoreState);
        Assert.Equal(v2Put.Value.VersionId, currentStoreState!.VersionId);
        Assert.True(currentStoreState.IsLatest);
        Assert.False(currentStoreState.IsDeleteMarker);
        Assert.Equal("two", currentStoreState.Tags!["release"]);
        Assert.Equal(v2Put.Value.Checksums!["sha256"], currentStoreState.Checksums!["sha256"]);
    }

    [Fact]
    public async Task DiskStorage_UsesRegisteredPlatformMultipartStateStoreForUploadState()
    {
        await using var fixture = new DiskStorageFixture(services => {
            services.AddSingleton<InMemoryMultipartStateStore>();
            services.AddSingleton<IStorageMultipartStateStore>(static serviceProvider => serviceProvider.GetRequiredService<InMemoryMultipartStateStore>());
        });

        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        var multipartStateStore = fixture.Services.GetRequiredService<InMemoryMultipartStateStore>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "multipart-external" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-external",
            Key = "docs/assembled.txt",
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "external-multipart"
            },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["upload"] = "external"
            }
        });

        Assert.True(initiateResult.IsSuccess);

        var uploadId = initiateResult.Value!.UploadId;
        var uploadState = await multipartStateStore.GetMultipartUploadStateAsync("test-disk", "multipart-external", "docs/assembled.txt", uploadId);
        Assert.NotNull(uploadState);
        Assert.Equal("text/plain", uploadState!.ContentType);
        Assert.Equal("external-multipart", uploadState.Metadata!["source"]);
        Assert.Equal("external", uploadState.Tags!["upload"]);

        var listedUploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "multipart-external"
        }).ToArrayAsync();
        var listedUpload = Assert.Single(listedUploads);
        Assert.Equal(uploadId, listedUpload.UploadId);
        Assert.Equal("docs/assembled.txt", listedUpload.Key);

        // The first (non-final) part must be at least the S3 minimum of 5 MiB.
        var part1Payload = new string('a', 5 * 1024 * 1024);
        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes(part1Payload));
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-external",
            Key = "docs/assembled.txt",
            UploadId = uploadId,
            PartNumber = 1,
            Content = part1Stream
        });

        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes("world"));
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "multipart-external",
            Key = "docs/assembled.txt",
            UploadId = uploadId,
            PartNumber = 2,
            Content = part2Stream
        });

        Assert.True(part1.IsSuccess);
        Assert.True(part2.IsSuccess);

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "multipart-external",
            Key = "docs/assembled.txt",
            UploadId = uploadId,
            Parts = [part1.Value!, part2.Value!]
        });

        Assert.True(completeResult.IsSuccess);
        Assert.Equal("text/plain", completeResult.Value!.ContentType);
        Assert.Equal("external-multipart", completeResult.Value.Metadata!["source"]);
        Assert.Equal("external", completeResult.Value.Tags!["upload"]);

        var removedState = await multipartStateStore.GetMultipartUploadStateAsync("test-disk", "multipart-external", "docs/assembled.txt", uploadId);
        Assert.Null(removedState);

        var supportState = await storageService.GetSupportStateDescriptorAsync();
        Assert.Equal(IntegratedS3.Abstractions.Capabilities.StorageSupportStateOwnership.PlatformManaged, supportState.MultipartState);
    }

    // --- Customer Encryption (SSE-C) Rejection ---

    [Fact]
    public async Task DiskStorage_PutObject_RejectsCustomerEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "ssec-put"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("ssec payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "ssec-put",
            Key = "docs/object.txt",
            Content = uploadStream,
            CustomerEncryption = CreateCustomerEncryptionSettings()
        });

        Assert.False(putResult.IsSuccess);
        AssertUnsupportedCustomerEncryption(putResult.Error, "ssec-put", "docs/object.txt");

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "ssec-put",
            Key = "docs/object.txt"
        });

        Assert.False(headResult.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, headResult.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_GetObject_RejectsCustomerEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "ssec-get"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("read me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "ssec-get",
            Key = "docs/object.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "ssec-get",
            Key = "docs/object.txt",
            CustomerEncryption = CreateCustomerEncryptionSettings()
        });

        Assert.False(getResult.IsSuccess);
        AssertUnsupportedCustomerEncryption(getResult.Error, "ssec-get", "docs/object.txt");
    }

    [Fact]
    public async Task DiskStorage_HeadObject_RejectsCustomerEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "ssec-head"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("head me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "ssec-head",
            Key = "docs/object.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "ssec-head",
            Key = "docs/object.txt",
            CustomerEncryption = CreateCustomerEncryptionSettings()
        });

        Assert.False(headResult.IsSuccess);
        AssertUnsupportedCustomerEncryption(headResult.Error, "ssec-head", "docs/object.txt");

        var plainHeadResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "ssec-head",
            Key = "docs/object.txt"
        });

        Assert.True(plainHeadResult.IsSuccess);
        Assert.Equal(putResult.Value!.VersionId, plainHeadResult.Value!.VersionId);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_RejectsSourceCustomerEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "ssec-src" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "ssec-dst" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "ssec-src",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var sourceEncryptedCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "ssec-src",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "ssec-dst",
            DestinationKey = "docs/source-encrypted.txt",
            SourceCustomerEncryption = CreateCustomerEncryptionSettings()
        });

        Assert.False(sourceEncryptedCopy.IsSuccess);
        AssertUnsupportedCustomerEncryption(sourceEncryptedCopy.Error, "ssec-src", "docs/source.txt");

        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "ssec-dst",
            Key = "docs/source-encrypted.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_RejectsDestinationCustomerEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "ssec-src2" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "ssec-dst2" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy me"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "ssec-src2",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        var destinationEncryptedCopy = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "ssec-src2",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "ssec-dst2",
            DestinationKey = "docs/destination-encrypted.txt",
            DestinationCustomerEncryption = CreateCustomerEncryptionSettings()
        });

        Assert.False(destinationEncryptedCopy.IsSuccess);
        AssertUnsupportedCustomerEncryption(destinationEncryptedCopy.Error, "ssec-dst2", "docs/destination-encrypted.txt");

        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "ssec-dst2",
            Key = "docs/destination-encrypted.txt"
        })).IsSuccess);
    }

    [Fact]
    public async Task DiskStorage_InitiateMultipartUpload_RejectsCustomerEncryptionRequests()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "ssec-multipart"
        })).IsSuccess);

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "ssec-multipart",
            Key = "docs/multipart.bin",
            ContentType = "application/octet-stream",
            CustomerEncryption = CreateCustomerEncryptionSettings()
        });

        Assert.False(initiateResult.IsSuccess);
        AssertUnsupportedCustomerEncryption(initiateResult.Error, "ssec-multipart", "docs/multipart.bin");
    }

    [Fact]
    public async Task DiskStorage_BackslashAndForwardSlashKeys_RemainDistinctObjects()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "key-encoding-backslash";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        })).IsSuccess);

        async Task PutAsync(string key, string payload)
        {
            await using var content = new MemoryStream(Encoding.UTF8.GetBytes(payload));
            Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                Content = content,
                ContentType = "text/plain"
            })).IsSuccess);
        }

        async Task<string> GetAsync(string key)
        {
            var getResult = await storageService.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = key
            });

            Assert.True(getResult.IsSuccess);
            await using var response = getResult.Value!;
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            return await reader.ReadToEndAsync();
        }

        await PutAsync(@"a\b", "backslash payload");
        await PutAsync("a/b", "slash payload");

        Assert.Equal("backslash payload", await GetAsync(@"a\b"));
        Assert.Equal("slash payload", await GetAsync("a/b"));

        var keys = await storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = bucketName
        }).Select(static objectInfo => objectInfo.Key).ToArrayAsync();

        Assert.Equal(2, keys.Length);
        Assert.Contains(@"a\b", keys);
        Assert.Contains("a/b", keys);

        Assert.True((await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = @"a\b"
        })).IsSuccess);

        Assert.Equal("slash payload", await GetAsync("a/b"));
        Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = bucketName,
            Key = @"a\b"
        })).IsSuccess);
    }

    [Theory]
    [InlineData("CON")]
    [InlineData("docs/aux.txt")]
    [InlineData("logs/nul/data.bin")]
    [InlineData("docs/report.")]
    [InlineData("docs/trailing ")]
    [InlineData("docs/100%/summary.txt")]
    public async Task DiskStorage_ReservedAndTrailingCharacterKeys_RoundTrip(string key)
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "key-encoding-reserved";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        })).IsSuccess);

        await using var content = new MemoryStream(Encoding.UTF8.GetBytes("reserved key payload"));
        Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            Content = content,
            ContentType = "text/plain"
        })).IsSuccess);

        var listedObject = Assert.Single(await storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = bucketName
        }).ToArrayAsync());
        Assert.Equal(key, listedObject.Key);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = key
        });

        Assert.True(getResult.IsSuccess);
        await using (var response = getResult.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            Assert.Equal("reserved key payload", await reader.ReadToEndAsync());
        }

        Assert.True((await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = key
        })).IsSuccess);

        Assert.Empty(await storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = bucketName
        }).ToArrayAsync());
    }

    [Fact]
    public async Task DiskStorage_CaseOnlyDifferingKeys_NeverAlias()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "key-encoding-casing";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        })).IsSuccess);

        await using var upperContent = new MemoryStream(Encoding.UTF8.GetBytes("uppercase payload"));
        Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = "Docs/Value.txt",
            Content = upperContent,
            ContentType = "text/plain"
        })).IsSuccess);

        await using var lowerContent = new MemoryStream(Encoding.UTF8.GetBytes("lowercase payload"));
        var lowerPut = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = "docs/value.txt",
            Content = lowerContent,
            ContentType = "text/plain"
        });

        // On a case-sensitive filesystem both keys coexist; on a case-insensitive one the
        // second write must be rejected instead of silently overwriting the first object.
        var upperGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = "Docs/Value.txt"
        });

        Assert.True(upperGet.IsSuccess);
        await using (var response = upperGet.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            Assert.Equal("uppercase payload", await reader.ReadToEndAsync());
        }

        if (lowerPut.IsSuccess) {
            var lowerGet = await storageService.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = "docs/value.txt"
            });

            Assert.True(lowerGet.IsSuccess);
            await using var response = lowerGet.Value!;
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            Assert.Equal("lowercase payload", await reader.ReadToEndAsync());
        }
        else {
            Assert.Equal(StorageErrorCode.UnsupportedCapability, lowerPut.Error!.Code);
            Assert.Contains("casing", lowerPut.Error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.False((await storageService.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = "docs/value.txt"
            })).IsSuccess);
        }
    }

    [Fact]
    public async Task DiskStorage_ConcurrentSameKeyPuts_PreserveEveryVersion()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "concurrent-put-versions";
        const string key = "docs/contended.txt";
        const int writerCount = 16;

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName,
            EnableVersioning = true
        })).IsSuccess);

        var versionIdsByPayload = new System.Collections.Concurrent.ConcurrentDictionary<string, string>(StringComparer.Ordinal);
        await Parallel.ForAsync(0, writerCount, async (writerIndex, cancellationToken) => {
            var payload = $"concurrent payload {writerIndex}";
            await using var content = new MemoryStream(Encoding.UTF8.GetBytes(payload));
            var putResult = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                Content = content,
                ContentType = "text/plain"
            }, cancellationToken);

            Assert.True(putResult.IsSuccess);
            Assert.True(versionIdsByPayload.TryAdd(payload, putResult.Value!.VersionId!));
        });

        Assert.Equal(writerCount, versionIdsByPayload.Values.Distinct(StringComparer.Ordinal).Count());

        var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = bucketName,
            Prefix = key
        }).ToArrayAsync();

        Assert.Equal(writerCount, versions.Length);
        Assert.Single(versions, static version => version.IsLatest);
        Assert.All(versions, static version => Assert.False(version.IsDeleteMarker));

        foreach (var (payload, versionId) in versionIdsByPayload) {
            var getResult = await storageService.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId
            });

            Assert.True(getResult.IsSuccess);
            await using var response = getResult.Value!;
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }

        // Every archived content file must belong to a surviving version record; a lost
        // update would leave an orphaned version directory (or drop a version) instead.
        var versionsRootPath = Path.Combine(fixture.RootPath, bucketName, ".integrateds3-versions");
        var archivedVersionDirectories = Directory.GetDirectories(versionsRootPath, "*", SearchOption.AllDirectories)
            .Where(static directory => File.Exists(Path.Combine(directory, "content")))
            .Select(static directory => Path.GetFileName(directory))
            .ToArray();

        Assert.Equal(writerCount - 1, archivedVersionDirectories.Length);
        Assert.All(archivedVersionDirectories, versionDirectory =>
            Assert.Contains(versionDirectory, versionIdsByPayload.Values));
    }

    [Fact]
    public async Task DiskStorage_ConcurrentBucketConfigurationUpdates_DoNotLoseEachOther()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "concurrent-bucket-config";
        const int updateRounds = 8;

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        })).IsSuccess);

        var taggingUpdates = Task.Run(async () => {
            for (var round = 0; round < updateRounds; round++) {
                Assert.True((await storageService.PutBucketTaggingAsync(new PutBucketTaggingRequest
                {
                    BucketName = bucketName,
                    Tags = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["round"] = round.ToString()
                    }
                })).IsSuccess);
            }
        });

        var loggingUpdates = Task.Run(async () => {
            for (var round = 0; round < updateRounds; round++) {
                Assert.True((await storageService.PutBucketLoggingAsync(new PutBucketLoggingRequest
                {
                    BucketName = bucketName,
                    TargetBucket = "audit-bucket",
                    TargetPrefix = $"round-{round}/"
                })).IsSuccess);
            }
        });

        var accelerateUpdates = Task.Run(async () => {
            for (var round = 0; round < updateRounds; round++) {
                Assert.True((await storageService.PutBucketAccelerateAsync(new PutBucketAccelerateRequest
                {
                    BucketName = bucketName,
                    Status = BucketAccelerateStatus.Enabled
                })).IsSuccess);
            }
        });

        await Task.WhenAll(taggingUpdates, loggingUpdates, accelerateUpdates);

        // Without serialized read-modify-write cycles one writer's final state could drop
        // another configuration section entirely.
        var tagging = await storageService.GetBucketTaggingAsync(bucketName);
        Assert.True(tagging.IsSuccess);
        Assert.Equal((updateRounds - 1).ToString(), tagging.Value!.Tags["round"]);

        var logging = await storageService.GetBucketLoggingAsync(bucketName);
        Assert.True(logging.IsSuccess);
        Assert.Equal("audit-bucket", logging.Value!.TargetBucket);
        Assert.Equal($"round-{updateRounds - 1}/", logging.Value.TargetPrefix);

        var accelerate = await storageService.GetBucketAccelerateAsync(bucketName);
        Assert.True(accelerate.IsSuccess);
        Assert.Equal(BucketAccelerateStatus.Enabled, accelerate.Value!.Status);
    }

    [Fact]
    public async Task DiskStorage_PutObjectLockConfiguration_RequiresVersioningEnabled()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "object-lock-needs-versioning";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName
        })).IsSuccess);

        var putObjectLock = await storageService.PutObjectLockConfigurationAsync(new PutObjectLockConfigurationRequest
        {
            BucketName = bucketName,
            ObjectLockEnabled = true,
            DefaultRetention = new ObjectLockDefaultRetention
            {
                Mode = ObjectRetentionMode.Compliance,
                Days = 1
            }
        });

        Assert.False(putObjectLock.IsSuccess);
        // AWS surfaces this precondition as InvalidBucketState (not OperationAborted / VersionConflict).
        Assert.Equal(StorageErrorCode.InvalidBucketState, putObjectLock.Error!.Code);
        Assert.Equal(409, putObjectLock.Error.SuggestedHttpStatusCode);
        Assert.Contains("versioning", putObjectLock.Error.Message, StringComparison.OrdinalIgnoreCase);

        var getObjectLock = await storageService.GetObjectLockConfigurationAsync(bucketName);
        Assert.False(getObjectLock.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectLockConfigurationNotFound, getObjectLock.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_DeleteObjectVersion_InsideDefaultRetentionWindow_IsRefused()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "object-lock-retention";
        const string key = "docs/locked.txt";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName,
            EnableVersioning = true
        })).IsSuccess);

        Assert.True((await storageService.PutObjectLockConfigurationAsync(new PutObjectLockConfigurationRequest
        {
            BucketName = bucketName,
            ObjectLockEnabled = true,
            DefaultRetention = new ObjectLockDefaultRetention
            {
                Mode = ObjectRetentionMode.Compliance,
                Days = 1
            }
        })).IsSuccess);

        string versionId;
        await using (var content = new MemoryStream(Encoding.UTF8.GetBytes("protected payload"))) {
            var putResult = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                Content = content,
                ContentType = "text/plain"
            });

            Assert.True(putResult.IsSuccess);
            versionId = putResult.Value!.VersionId!;
        }

        var versionDelete = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        });

        Assert.False(versionDelete.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectLocked, versionDelete.Error!.Code);
        Assert.Equal(403, versionDelete.Error.SuggestedHttpStatusCode);
        Assert.Equal(versionId, versionDelete.Error.VersionId);

        // A delete without a version id only writes a delete marker and never destroys
        // content, so object lock must not block it.
        var markerDelete = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = key
        });

        Assert.True(markerDelete.IsSuccess);
        Assert.True(markerDelete.Value!.IsDeleteMarker);

        var lockedGet = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        });

        Assert.True(lockedGet.IsSuccess);
        await using (var response = lockedGet.Value!) {
            using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
            Assert.Equal("protected payload", await reader.ReadToEndAsync());
        }

        var lockedVersionStillDenied = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        });

        Assert.False(lockedVersionStillDenied.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectLocked, lockedVersionStillDenied.Error!.Code);
    }

    [Fact]
    public async Task DiskStorage_PermanentDeleteOfLockedObject_AfterVersioningSuspended_IsRefused()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        const string bucketName = "object-lock-suspended";
        const string key = "docs/still-locked.txt";

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = bucketName,
            EnableVersioning = true
        })).IsSuccess);

        Assert.True((await storageService.PutObjectLockConfigurationAsync(new PutObjectLockConfigurationRequest
        {
            BucketName = bucketName,
            ObjectLockEnabled = true,
            DefaultRetention = new ObjectLockDefaultRetention
            {
                Mode = ObjectRetentionMode.Compliance,
                Days = 1
            }
        })).IsSuccess);

        await using (var content = new MemoryStream(Encoding.UTF8.GetBytes("suspended payload"))) {
            Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                Content = content,
                ContentType = "text/plain"
            })).IsSuccess);
        }

        Assert.True((await storageService.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            Status = BucketVersioningStatus.Suspended
        })).IsSuccess);

        // With versioning suspended a delete without a version id becomes a permanent
        // delete, so the retention window must still refuse it.
        var permanentDelete = await storageService.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = key
        });

        Assert.False(permanentDelete.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectLocked, permanentDelete.Error!.Code);
        Assert.Equal(403, permanentDelete.Error.SuggestedHttpStatusCode);

        var stillReadable = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = key
        });

        Assert.True(stillReadable.IsSuccess);
        await using var stillReadableResponse = stillReadable.Value!;
        using var stillReadableReader = new StreamReader(stillReadableResponse.Content, Encoding.UTF8, leaveOpen: false);
        Assert.Equal("suspended payload", await stillReadableReader.ReadToEndAsync());
    }

    private static void AssertUnsupportedServerSideEncryption(StorageError? error, string bucketName, string objectKey)
    {
        var actual = Assert.IsType<StorageError>(error);
        Assert.Equal(StorageErrorCode.UnsupportedCapability, actual.Code);
        Assert.Equal(bucketName, actual.BucketName);
        Assert.Equal(objectKey, actual.ObjectKey);
        Assert.Equal(501, actual.SuggestedHttpStatusCode);
        Assert.Contains("server-side encryption", actual.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static void AssertUnsupportedCustomerEncryption(StorageError? error, string bucketName, string objectKey)
    {
        var actual = Assert.IsType<StorageError>(error);
        Assert.Equal(StorageErrorCode.UnsupportedCapability, actual.Code);
        Assert.Equal(bucketName, actual.BucketName);
        Assert.Equal(objectKey, actual.ObjectKey);
        Assert.Equal(501, actual.SuggestedHttpStatusCode);
        Assert.Contains("customer-provided encryption", actual.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static ObjectServerSideEncryptionSettings CreateServerSideEncryptionSettings()
    {
        return new ObjectServerSideEncryptionSettings
        {
            Algorithm = ObjectServerSideEncryptionAlgorithm.Kms,
            KeyId = "disk-provider-test-key",
            Context = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant"] = "tests"
            }
        };
    }

    private static ObjectCustomerEncryptionSettings CreateCustomerEncryptionSettings()
    {
        var keyBytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(keyBytes);
        return new ObjectCustomerEncryptionSettings
        {
            Algorithm = "AES256",
            Key = Convert.ToBase64String(keyBytes),
            KeyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(keyBytes))
        };
    }

    private static ServiceProvider CreateDiskStorageServiceProvider(string rootPath)
    {
        var services = new ServiceCollection();
        services.AddDiskStorage(new DiskStorageOptions
        {
            ProviderName = "test-disk",
            RootPath = rootPath,
            CreateRootDirectory = true
        });

        return services.BuildServiceProvider();
    }

    private static string GetBucketMetadataPath(string rootPath, string bucketName)
    {
        return Path.Combine(rootPath, bucketName, ".integrateds3.bucket.json");
    }

    // ---- Regression tests for issue #147: multipart error family must map to distinct S3 codes ----

    private static async Task<(string UploadId, MultipartUploadPart Part1, MultipartUploadPart Part2)> SeedTwoPartMultipartUploadAsync(
        IStorageBackend storageService,
        string bucketName,
        string key)
    {
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = bucketName });

        var initiate = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentType = "text/plain"
        });
        Assert.True(initiate.IsSuccess);

        // The first (non-final) part must be at least the S3 minimum of 5 MiB so that
        // completion reaches the caller's intended validation (missing part, ETag mismatch, ...).
        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes(new string('a', 5 * 1024 * 1024)));
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = bucketName,
            Key = key,
            UploadId = initiate.Value!.UploadId,
            PartNumber = 1,
            Content = part1Stream
        });
        Assert.True(part1.IsSuccess);

        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes("world"));
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = bucketName,
            Key = key,
            UploadId = initiate.Value.UploadId,
            PartNumber = 2,
            Content = part2Stream
        });
        Assert.True(part2.IsSuccess);

        return (initiate.Value.UploadId, part1.Value!, part2.Value!);
    }

    [Fact]
    public async Task DiskStorage_Complete_MissingPart_ReturnsInvalidPart()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        var (uploadId, part1, _) = await SeedTwoPartMultipartUploadAsync(storageService, "mp-invalidpart-missing", "docs/obj.txt");

        // Reference a part number (3) that was never uploaded.
        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "mp-invalidpart-missing",
            Key = "docs/obj.txt",
            UploadId = uploadId,
            Parts =
            [
                part1,
                new MultipartUploadPart { PartNumber = 3, ETag = "\"deadbeef\"", ContentLength = 0, LastModifiedUtc = default }
            ]
        });

        Assert.False(completeResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidPart, completeResult.Error!.Code);
        Assert.Equal(400, completeResult.Error.SuggestedHttpStatusCode);
    }

    [Fact]
    public async Task DiskStorage_Complete_ETagMismatch_ReturnsInvalidPart()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        var (uploadId, part1, part2) = await SeedTwoPartMultipartUploadAsync(storageService, "mp-invalidpart-etag", "docs/obj.txt");

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "mp-invalidpart-etag",
            Key = "docs/obj.txt",
            UploadId = uploadId,
            Parts =
            [
                part1,
                new MultipartUploadPart { PartNumber = part2.PartNumber, ETag = "\"0000000000000000000000000000ffff\"", ContentLength = 0, LastModifiedUtc = default }
            ]
        });

        Assert.False(completeResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidPart, completeResult.Error!.Code);
        Assert.Equal(400, completeResult.Error.SuggestedHttpStatusCode);
    }

    [Fact]
    public async Task DiskStorage_Complete_PartsNotAscending_ReturnsInvalidPartOrder()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        var (uploadId, part1, part2) = await SeedTwoPartMultipartUploadAsync(storageService, "mp-partorder-desc", "docs/obj.txt");

        // Parts supplied in descending order must be rejected, not silently re-sorted.
        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "mp-partorder-desc",
            Key = "docs/obj.txt",
            UploadId = uploadId,
            Parts = [part2, part1]
        });

        Assert.False(completeResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidPartOrder, completeResult.Error!.Code);
        Assert.Equal(400, completeResult.Error.SuggestedHttpStatusCode);
    }

    [Fact]
    public async Task DiskStorage_Complete_DuplicatePartNumbers_ReturnsInvalidPartOrder()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        var (uploadId, part1, _) = await SeedTwoPartMultipartUploadAsync(storageService, "mp-partorder-dup", "docs/obj.txt");

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "mp-partorder-dup",
            Key = "docs/obj.txt",
            UploadId = uploadId,
            Parts = [part1, part1]
        });

        Assert.False(completeResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidPartOrder, completeResult.Error!.Code);
        Assert.Equal(400, completeResult.Error.SuggestedHttpStatusCode);
    }

    [Fact]
    public async Task DiskStorage_UploadPart_PartNumberAboveMaximum_ReturnsInvalidArgument()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "mp-partrange" });

        var initiate = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "mp-partrange",
            Key = "docs/obj.txt"
        });
        Assert.True(initiate.IsSuccess);

        await using var partStream = new MemoryStream(Encoding.UTF8.GetBytes("payload"));
        var uploadResult = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "mp-partrange",
            Key = "docs/obj.txt",
            UploadId = initiate.Value!.UploadId,
            PartNumber = 10001,
            Content = partStream
        });

        Assert.False(uploadResult.IsSuccess);
        Assert.Equal(StorageErrorCode.InvalidArgument, uploadResult.Error!.Code);
        Assert.Equal(400, uploadResult.Error.SuggestedHttpStatusCode);
    }

    [Fact]
    public async Task DiskStorage_UploadPart_UnknownUploadId_ReturnsNoSuchUpload()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "mp-nosuchupload-uploadpart" });

        await using var partStream = new MemoryStream(Encoding.UTF8.GetBytes("payload"));
        var uploadResult = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "mp-nosuchupload-uploadpart",
            Key = "docs/obj.txt",
            UploadId = "00000000000000000000000000000000",
            PartNumber = 1,
            Content = partStream
        });

        Assert.False(uploadResult.IsSuccess);
        Assert.Equal(StorageErrorCode.NoSuchUpload, uploadResult.Error!.Code);
        Assert.Equal(404, uploadResult.Error.SuggestedHttpStatusCode);
    }

    [Fact]
    public async Task DiskStorage_Abort_UnknownUploadId_ReturnsNoSuchUpload()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "mp-nosuchupload-abort" });

        var abortResult = await storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "mp-nosuchupload-abort",
            Key = "docs/obj.txt",
            UploadId = "00000000000000000000000000000000"
        });

        Assert.False(abortResult.IsSuccess);
        Assert.Equal(StorageErrorCode.NoSuchUpload, abortResult.Error!.Code);
        Assert.Equal(404, abortResult.Error.SuggestedHttpStatusCode);
    }

    [Fact]
    public async Task DiskStorage_Complete_UnknownUploadId_ReturnsNoSuchUpload()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "mp-nosuchupload-complete" });

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "mp-nosuchupload-complete",
            Key = "docs/obj.txt",
            UploadId = "00000000000000000000000000000000",
            Parts = [new MultipartUploadPart { PartNumber = 1, ETag = "\"deadbeef\"", ContentLength = 0, LastModifiedUtc = default }]
        });

        Assert.False(completeResult.IsSuccess);
        Assert.Equal(StorageErrorCode.NoSuchUpload, completeResult.Error!.Code);
        Assert.Equal(404, completeResult.Error.SuggestedHttpStatusCode);
    }

    // ---- Regression tests for issue #120: TOCTOU delete/overwrite races on lock-free reads ----

    // Locates the on-disk content file for a freshly-Put object by finding the sibling of the
    // metadata sidecar (content path == sidecar path minus the metadata suffix). This lets the
    // test reproduce the exact TOCTOU window the provider races: metadata still present, but the
    // content file deleted out from under the lock-free read/copy-source open.
    private static string FindObjectContentPath(string rootPath, string bucketName)
    {
        var bucketPath = Path.Combine(rootPath, bucketName);
        const string metadataSuffix = ".integrateds3.json";
        var sidecar = Directory
            .EnumerateFiles(bucketPath, "*" + metadataSuffix, SearchOption.AllDirectories)
            .First(path => !Path.GetFileName(path).StartsWith(".integrateds3.", StringComparison.Ordinal));
        return sidecar[..^metadataSuffix.Length];
    }

    [Fact]
    public async Task DiskStorage_GetObject_ContentFileDeletedAfterResolve_ReturnsNoSuchKeyNot500()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "toctou-get" });

        await using (var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("racy content"))) {
            var put = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "toctou-get",
                Key = "race.txt",
                Content = uploadStream,
                ContentType = "text/plain"
            });
            Assert.True(put.IsSuccess);
        }

        // Simulate a concurrent DeleteObject landing between ResolveStoredObjectAsync and the
        // lock-free content-file open: remove only the content file, leaving metadata intact so
        // the object still resolves. Before the fix the open threw FileNotFoundException, which
        // surfaced as an InternalError (500); after the fix it must translate to NoSuchKey (404).
        var contentPath = FindObjectContentPath(fixture.RootPath, "toctou-get");
        File.Delete(contentPath);

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "toctou-get",
            Key = "race.txt"
        });

        Assert.False(getResult.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, getResult.Error!.Code);
        Assert.Equal(404, getResult.Error.SuggestedHttpStatusCode);
    }

    [Fact]
    public async Task DiskStorage_CopyObject_SourceContentFileDeletedAfterResolve_ReturnsNoSuchKeyNot500()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "toctou-copy" });

        await using (var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy source"))) {
            var put = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "toctou-copy",
                Key = "src.txt",
                Content = uploadStream,
                ContentType = "text/plain"
            });
            Assert.True(put.IsSuccess);
        }

        // CopyObject opens the source content file while holding only the destination lock, never
        // the source lock. Deleting the source content file (metadata intact) reproduces that race;
        // the losing copy must return NoSuchKey (404) rather than an InternalError (500).
        var sourceContentPath = FindObjectContentPath(fixture.RootPath, "toctou-copy");
        File.Delete(sourceContentPath);

        var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "toctou-copy",
            SourceKey = "src.txt",
            DestinationBucketName = "toctou-copy",
            DestinationKey = "dst.txt"
        });

        Assert.False(copyResult.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, copyResult.Error!.Code);
        Assert.Equal(404, copyResult.Error.SuggestedHttpStatusCode);
    }

    [Fact]
    public async Task DiskStorage_GetObject_ReaderSharesDeleteSoUnderlyingFileCanBeRemovedWhileOpen()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "fileshare-delete" });

        await using (var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("still readable"))) {
            var put = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "fileshare-delete",
                Key = "open.txt",
                Content = uploadStream,
                ContentType = "text/plain"
            });
            Assert.True(put.IsSuccess);
        }

        var getResult = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "fileshare-delete",
            Key = "open.txt"
        });
        Assert.True(getResult.IsSuccess);

        var contentPath = FindObjectContentPath(fixture.RootPath, "fileshare-delete");

        await using var response = getResult.Value!;

        // The read stream must have been opened with FileShare.Delete: on Windows, deleting (or a
        // writer's File.Move overwrite of) the underlying file while a reader holds the handle only
        // succeeds when the reader shares Delete. Without it this File.Delete throws IOException.
        var exception = Record.Exception(() => File.Delete(contentPath));
        Assert.Null(exception);

        // The already-open reader keeps its handle and can still stream the full content even after
        // the directory entry is unlinked.
        using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
        Assert.Equal("still readable", await reader.ReadToEndAsync());
    }

    private static string ComputeContentMd5Hex(byte[] content)
    {
        return Convert.ToHexStringLower(System.Security.Cryptography.MD5.HashData(content));
    }

    private static string ComputeContentMd5Hex(string content)
    {
        return ComputeContentMd5Hex(Encoding.UTF8.GetBytes(content));
    }

    [Fact]
    public async Task DiskStorage_PutObject_ETagIsContentMd5Hex()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "etag-md5" });

        const string payload = "hello integrated s3";
        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "etag-md5",
            Key = "docs/hello.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);

        // The single-part ETag must be the lowercase-hex MD5 of the content (S3 contract), not the
        // old "<length:x>-<mtimeTicks:x>" string. This is a known-content -> known-MD5 assertion.
        var expectedETag = ComputeContentMd5Hex(payload);
        Assert.Equal(expectedETag, putResult.Value!.ETag);

        // HeadObject must return the same persisted content ETag.
        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "etag-md5",
            Key = "docs/hello.txt"
        });
        Assert.True(headResult.IsSuccess);
        Assert.Equal(expectedETag, headResult.Value!.ETag);
    }

    [Fact]
    public async Task DiskStorage_ObjectETag_IsStableAcrossFilesystemMtimeChange()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "etag-mtime" });

        const string payload = "content that must keep its etag";
        await using (var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes(payload))) {
            var putResult = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "etag-mtime",
                Key = "docs/stable.txt",
                Content = uploadStream,
                ContentType = "text/plain"
            });
            Assert.True(putResult.IsSuccess);
        }

        var expectedETag = ComputeContentMd5Hex(payload);

        // Simulate any external process (backup, AV, replication/copy) resetting the content file's
        // last-write time. With a content-derived ETag the value must not move; the old size+mtime
        // ETag would change here and break conditional requests and integrity checks.
        var contentPath = FindObjectContentPath(fixture.RootPath, "etag-mtime");
        File.SetLastWriteTimeUtc(contentPath, new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "etag-mtime",
            Key = "docs/stable.txt"
        });

        Assert.True(headResult.IsSuccess);
        Assert.Equal(expectedETag, headResult.Value!.ETag);
    }

    [Fact]
    public async Task DiskStorage_UploadPart_ETagIsPartContentMd5Hex()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "part-etag" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "part-etag",
            Key = "docs/assembled.bin",
            ContentType = "application/octet-stream"
        });
        Assert.True(initiateResult.IsSuccess);

        var partBytes = new byte[5 * 1024 * 1024];
        for (var i = 0; i < partBytes.Length; i++) {
            partBytes[i] = (byte)(i % 251);
        }

        await using var partStream = new MemoryStream(partBytes);
        var part = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "part-etag",
            Key = "docs/assembled.bin",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = partStream
        });

        Assert.True(part.IsSuccess);
        Assert.Equal(ComputeContentMd5Hex(partBytes), part.Value!.ETag);
    }

    [Fact]
    public async Task DiskStorage_CompleteMultipart_ETagIsCompositeMd5WithPartCount()
    {
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "composite-etag" });

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "composite-etag",
            Key = "docs/assembled.bin",
            ContentType = "application/octet-stream"
        });
        Assert.True(initiateResult.IsSuccess);

        // Two parts: the first must meet the 5 MiB S3 minimum, the last may be small.
        var part1Bytes = new byte[5 * 1024 * 1024];
        for (var i = 0; i < part1Bytes.Length; i++) {
            part1Bytes[i] = (byte)(i % 97);
        }

        var part2Bytes = Encoding.UTF8.GetBytes("final tiny part");

        await using var part1Stream = new MemoryStream(part1Bytes);
        var part1 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "composite-etag",
            Key = "docs/assembled.bin",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 1,
            Content = part1Stream
        });

        await using var part2Stream = new MemoryStream(part2Bytes);
        var part2 = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "composite-etag",
            Key = "docs/assembled.bin",
            UploadId = initiateResult.Value!.UploadId,
            PartNumber = 2,
            Content = part2Stream
        });

        Assert.True(part1.IsSuccess);
        Assert.True(part2.IsSuccess);

        // Touch a part file's mtime before completion: with content-derived part ETags this must not
        // produce a spurious InvalidPart, because part validation no longer depends on mtime.
        var partsDirectory = Directory
            .EnumerateDirectories(Path.Combine(fixture.RootPath, ".integrateds3-multipart"), "parts", SearchOption.AllDirectories)
            .FirstOrDefault();
        if (partsDirectory is not null) {
            foreach (var partFile in Directory.EnumerateFiles(partsDirectory, "*.part")) {
                File.SetLastWriteTimeUtc(partFile, new DateTime(2001, 2, 3, 4, 5, 6, DateTimeKind.Utc));
            }
        }

        var completeResult = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "composite-etag",
            Key = "docs/assembled.bin",
            UploadId = initiateResult.Value!.UploadId,
            Parts = [part1.Value!, part2.Value!]
        });

        Assert.True(completeResult.IsSuccess);

        // Expected composite ETag: hex(MD5(concat(part MD5 bytes)))-<partCount>.
        var concatenatedPartMd5 = System.Security.Cryptography.MD5.HashData(part1Bytes)
            .Concat(System.Security.Cryptography.MD5.HashData(part2Bytes))
            .ToArray();
        var expectedETag = $"{Convert.ToHexStringLower(System.Security.Cryptography.MD5.HashData(concatenatedPartMd5))}-2";

        Assert.Equal(expectedETag, completeResult.Value!.ETag);
        Assert.Matches(@"^[0-9a-f]{32}-\d+$", completeResult.Value.ETag!);

        // The persisted composite ETag must survive a re-read via HeadObject.
        var headResult = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "composite-etag",
            Key = "docs/assembled.bin"
        });
        Assert.True(headResult.IsSuccess);
        Assert.Equal(expectedETag, headResult.Value!.ETag);
    }

    [Fact]
    public async Task DiskStorage_ConcurrentSamePartNumberUploads_EachETagMatchesOwnBytesAndPersistedPartIsWhole()
    {
        // Regression for #124: two concurrent UploadPart calls for the same (uploadId, partNumber)
        // with different payloads race on the shared, part-number-keyed final path. Previously the
        // response ETag/checksum/length was derived by re-reading that shared path AFTER File.Move,
        // so one caller could return metadata describing the OTHER caller's bytes (or a torn mix).
        // The fix captures each call's metadata from its own exclusively-owned temp file before the
        // atomic publish, so each response always describes the bytes that call wrote, and the
        // persisted part is always exactly one caller's complete bytes (never torn).
        await using var fixture = new DiskStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageBackend>();
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "same-part-race" });

        // Many iterations to reliably hit the interleaving window between the two publishes and the
        // (previously) shared-path re-reads.
        for (var iteration = 0; iteration < 80; iteration++)
        {
            var key = $"docs/race-{iteration}.txt";
            var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            {
                BucketName = "same-part-race",
                Key = key,
                ChecksumAlgorithm = "SHA256"
            });
            Assert.True(initiateResult.IsSuccess);
            var uploadId = initiateResult.Value!.UploadId;

            // Two distinct payloads of different lengths, so a torn/other-caller read is detectable
            // via ETag, sha256, AND ContentLength.
            var payloadA = $"AAAA-payload-alpha-{iteration}-{new string('a', 37)}";
            var payloadB = $"BB-payload-beta-{iteration}";
            var bytesA = Encoding.UTF8.GetBytes(payloadA);
            var bytesB = Encoding.UTF8.GetBytes(payloadB);
            var checksumA = ComputeSha256Base64(payloadA);
            var checksumB = ComputeSha256Base64(payloadB);
            var expectedEtagA = Convert.ToHexStringLower(System.Security.Cryptography.MD5.HashData(bytesA));
            var expectedEtagB = Convert.ToHexStringLower(System.Security.Cryptography.MD5.HashData(bytesB));

            var uploadA = Task.Run(async () =>
            {
                await using var stream = new MemoryStream(bytesA);
                return await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
                {
                    BucketName = "same-part-race",
                    Key = key,
                    UploadId = uploadId,
                    PartNumber = 1,
                    Content = stream,
                    ChecksumAlgorithm = "SHA256",
                    Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["sha256"] = checksumA
                    }
                });
            });

            var uploadB = Task.Run(async () =>
            {
                await using var stream = new MemoryStream(bytesB);
                return await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
                {
                    BucketName = "same-part-race",
                    Key = key,
                    UploadId = uploadId,
                    PartNumber = 1,
                    Content = stream,
                    ChecksumAlgorithm = "SHA256",
                    Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["sha256"] = checksumB
                    }
                });
            });

            var resultA = await uploadA;
            var resultB = await uploadB;

            Assert.True(resultA.IsSuccess, $"Upload A failed: {resultA.Error?.Code}");
            Assert.True(resultB.IsSuccess, $"Upload B failed: {resultB.Error?.Code}");

            // Core invariant: each response describes THIS call's own bytes — ETag, checksum, and
            // length — regardless of who published last. On the pre-fix code, a lost race makes one
            // of these assertions observe the other caller's bytes.
            Assert.Equal(expectedEtagA, resultA.Value!.ETag);
            Assert.Equal(bytesA.Length, resultA.Value.ContentLength);
            Assert.Equal(ComputeSha256Base64(payloadA), resultA.Value.Checksums!["sha256"]);

            Assert.Equal(expectedEtagB, resultB.Value!.ETag);
            Assert.Equal(bytesB.Length, resultB.Value.ContentLength);
            Assert.Equal(ComputeSha256Base64(payloadB), resultB.Value.Checksums!["sha256"]);

            // The persisted part is exactly one caller's whole bytes (atomic rename => never torn).
            // Completing with the winning caller's returned part must succeed and reproduce that
            // caller's exact payload; the losing caller's ETag no longer matches the persisted part.
            var completeWithA = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = "same-part-race",
                Key = key,
                UploadId = uploadId,
                Parts = [resultA.Value]
            });

            string winnerPayload;
            if (completeWithA.IsSuccess)
            {
                winnerPayload = payloadA;
            }
            else
            {
                // A lost: its ETag must not match the persisted part; B won and must complete cleanly.
                Assert.Equal(StorageErrorCode.InvalidPart, completeWithA.Error!.Code);
                var completeWithB = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
                {
                    BucketName = "same-part-race",
                    Key = key,
                    UploadId = uploadId,
                    Parts = [resultB.Value]
                });
                Assert.True(completeWithB.IsSuccess, $"Complete with B failed: {completeWithB.Error?.Code}");
                winnerPayload = payloadB;
            }

            var getResult = await storageService.GetObjectAsync(new GetObjectRequest
            {
                BucketName = "same-part-race",
                Key = key
            });
            Assert.True(getResult.IsSuccess);
            await using (var response = getResult.Value!)
            {
                using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
                var content = await reader.ReadToEndAsync();
                // Persisted object is exactly one caller's whole payload — never a torn mix.
                Assert.Equal(winnerPayload, content);
            }
        }
    }
}

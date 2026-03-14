using System.Xml.Linq;
using IntegratedS3.Protocol;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class S3XmlResponseWriterTests
{
    [Fact]
    public void XmlResponses_EmitCanonicalS3NamespaceOnAllElements()
    {
        var timestamp = new DateTimeOffset(2026, 03, 14, 09, 00, 00, TimeSpan.Zero);

        var responses = new (string RootName, string Xml)[]
        {
            ("VersioningConfiguration", S3XmlResponseWriter.WriteBucketVersioningConfiguration(new S3BucketVersioningConfiguration
            {
                Status = "Enabled"
            })),
            ("CORSConfiguration", S3XmlResponseWriter.WriteCorsConfiguration(new S3CorsConfiguration
            {
                Rules =
                [
                    new S3CorsRule
                    {
                        Id = "rule-1",
                        AllowedOrigins = ["https://example.com"],
                        AllowedMethods = ["GET"],
                        AllowedHeaders = ["authorization"],
                        ExposeHeaders = ["etag"],
                        MaxAgeSeconds = 60
                    }
                ]
            })),
            ("Error", S3XmlResponseWriter.WriteError(new S3ErrorResponse
            {
                Code = "AccessDenied",
                Message = "Denied.",
                Resource = "/bucket/key",
                RequestId = "request-1",
                BucketName = "bucket",
                Key = "key"
            })),
            ("CopyObjectResult", S3XmlResponseWriter.WriteCopyObjectResult(new S3CopyObjectResult
            {
                ETag = "etag-1",
                LastModifiedUtc = timestamp,
                ChecksumSha256 = "checksum-sha256",
                ChecksumType = "FULL_OBJECT"
            })),
            ("InitiateMultipartUploadResult", S3XmlResponseWriter.WriteInitiateMultipartUploadResult(new S3InitiateMultipartUploadResult
            {
                Bucket = "bucket",
                Key = "key",
                UploadId = "upload-1",
                ChecksumAlgorithm = "SHA256"
            })),
            ("CompleteMultipartUploadResult", S3XmlResponseWriter.WriteCompleteMultipartUploadResult(new S3CompleteMultipartUploadResult
            {
                Location = "https://example.test/bucket/key",
                Bucket = "bucket",
                Key = "key",
                ETag = "etag-2",
                ChecksumSha256 = "checksum-sha256",
                ChecksumType = "COMPOSITE"
            })),
            ("ListBucketResult", S3XmlResponseWriter.WriteListBucketResult(new S3ListBucketResult
            {
                Name = "bucket",
                Prefix = "docs/",
                Delimiter = "/",
                StartAfter = "docs/a.txt",
                ContinuationToken = "token-1",
                NextContinuationToken = "token-2",
                KeyCount = 1,
                MaxKeys = 1000,
                IsTruncated = true,
                Contents =
                [
                    new S3ListBucketObject
                    {
                        Key = "docs/a.txt",
                        ETag = "etag-3",
                        Size = 5,
                        LastModifiedUtc = timestamp,
                        StorageClass = "STANDARD"
                    }
                ],
                CommonPrefixes =
                [
                    new S3ListBucketCommonPrefix
                    {
                        Prefix = "docs/sub/"
                    }
                ]
            })),
            ("ListVersionsResult", S3XmlResponseWriter.WriteListObjectVersionsResult(new S3ListObjectVersionsResult
            {
                Name = "bucket",
                Prefix = "docs/",
                Delimiter = "/",
                KeyMarker = "docs/a.txt",
                VersionIdMarker = "version-1",
                NextKeyMarker = "docs/b.txt",
                NextVersionIdMarker = "version-2",
                MaxKeys = 2,
                IsTruncated = true,
                Versions =
                [
                    new S3ObjectVersionEntry
                    {
                        Key = "docs/a.txt",
                        VersionId = "version-1",
                        IsLatest = true,
                        ETag = "etag-4",
                        Size = 10,
                        LastModifiedUtc = timestamp,
                        StorageClass = "STANDARD"
                    },
                    new S3ObjectVersionEntry
                    {
                        Key = "docs/deleted.txt",
                        VersionId = "version-2",
                        IsDeleteMarker = true,
                        LastModifiedUtc = timestamp
                    }
                ],
                CommonPrefixes =
                [
                    new S3ListBucketCommonPrefix
                    {
                        Prefix = "docs/archive/"
                    }
                ]
            })),
            ("ListMultipartUploadsResult", S3XmlResponseWriter.WriteListMultipartUploadsResult(new S3ListMultipartUploadsResult
            {
                Bucket = "bucket",
                Prefix = "docs/",
                Delimiter = "/",
                KeyMarker = "docs/a.txt",
                UploadIdMarker = "upload-1",
                NextKeyMarker = "docs/b.txt",
                NextUploadIdMarker = "upload-2",
                MaxUploads = 2,
                IsTruncated = true,
                Uploads =
                [
                    new S3MultipartUploadEntry
                    {
                        Key = "docs/a.txt",
                        UploadId = "upload-1",
                        InitiatedAtUtc = timestamp,
                        StorageClass = "STANDARD",
                        ChecksumAlgorithm = "SHA256"
                    }
                ],
                CommonPrefixes =
                [
                    new S3ListBucketCommonPrefix
                    {
                        Prefix = "docs/archive/"
                    }
                ]
            })),
            ("ListAllMyBucketsResult", S3XmlResponseWriter.WriteListAllMyBucketsResult(new S3ListAllMyBucketsResult
            {
                Owner = new S3BucketOwner
                {
                    Id = "owner-1",
                    DisplayName = "Test Owner"
                },
                Buckets =
                [
                    new S3BucketListEntry
                    {
                        Name = "bucket",
                        CreationDateUtc = timestamp
                    }
                ]
            })),
            ("DeleteResult", S3XmlResponseWriter.WriteDeleteObjectsResult(new S3DeleteObjectsResult
            {
                Deleted =
                [
                    new S3DeletedObjectResult
                    {
                        Key = "docs/a.txt",
                        VersionId = "version-1",
                        DeleteMarker = true,
                        DeleteMarkerVersionId = "delete-marker-1"
                    }
                ],
                Errors =
                [
                    new S3DeleteObjectError
                    {
                        Key = "docs/b.txt",
                        VersionId = "version-2",
                        Code = "AccessDenied",
                        Message = "Denied."
                    }
                ]
            })),
            ("Tagging", S3XmlResponseWriter.WriteObjectTagging(new S3ObjectTagging
            {
                TagSet =
                [
                    new S3ObjectTag
                    {
                        Key = "env",
                        Value = "test"
                    }
                ]
            }))
        };

        foreach (var (rootName, xml) in responses) {
            var document = XDocument.Parse(xml);

            S3XmlTestHelper.AssertRoot(document, rootName);
            Assert.All(document.Root!.DescendantsAndSelf(), static element =>
                Assert.Equal(S3XmlTestHelper.CanonicalS3Namespace, element.Name.NamespaceName));
        }
    }
}

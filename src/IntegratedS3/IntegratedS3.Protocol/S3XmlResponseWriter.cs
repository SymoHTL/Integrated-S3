using System.Globalization;
using System.Text;
using System.Xml;
using IntegratedS3.Abstractions.Responses;

namespace IntegratedS3.Protocol;

/// <summary>Writes S3 XML response bodies from typed DTO instances.</summary>
public static class S3XmlResponseWriter
{
    private const string CanonicalS3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/";

    /// <summary>Writes a BucketLocation as an XML response body.</summary>
    /// <param name="response">The <see cref="S3BucketLocationResponse"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteBucketLocation(S3BucketLocationResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "LocationConstraint");

        if (!string.IsNullOrWhiteSpace(response.LocationConstraint)) {
            xmlWriter.WriteString(response.LocationConstraint);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "LocationConstraint");
    }

    /// <summary>Writes a BucketVersioningConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3BucketVersioningConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteBucketVersioningConfiguration(S3BucketVersioningConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "VersioningConfiguration");

        if (!string.IsNullOrWhiteSpace(response.Status)) {
            xmlWriter.WriteElementString("Status", response.Status);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "VersioningConfiguration");
    }

    /// <summary>Writes a BucketEncryptionConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3BucketEncryptionConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteBucketEncryptionConfiguration(S3BucketEncryptionConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ServerSideEncryptionConfiguration");

        foreach (var rule in response.Rules) {
            xmlWriter.WriteStartElement("Rule");
            xmlWriter.WriteStartElement("ApplyServerSideEncryptionByDefault");

            if (!string.IsNullOrWhiteSpace(rule.DefaultEncryption.SseAlgorithm)) {
                xmlWriter.WriteElementString("SSEAlgorithm", rule.DefaultEncryption.SseAlgorithm);
            }

            if (!string.IsNullOrWhiteSpace(rule.DefaultEncryption.KmsMasterKeyId)) {
                xmlWriter.WriteElementString("KMSMasterKeyID", rule.DefaultEncryption.KmsMasterKeyId);
            }

            xmlWriter.WriteEndElement();

            if (rule.BucketKeyEnabled is { } bucketKeyEnabled) {
                xmlWriter.WriteElementString("BucketKeyEnabled", XmlConvert.ToString(bucketKeyEnabled));
            }

            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ServerSideEncryptionConfiguration");
    }

    /// <summary>Writes a CorsConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3CorsConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteCorsConfiguration(S3CorsConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "CORSConfiguration");

        foreach (var rule in response.Rules) {
            xmlWriter.WriteStartElement("CORSRule");

            if (!string.IsNullOrWhiteSpace(rule.Id)) {
                xmlWriter.WriteElementString("ID", rule.Id);
            }

            foreach (var allowedOrigin in rule.AllowedOrigins) {
                xmlWriter.WriteElementString("AllowedOrigin", allowedOrigin);
            }

            foreach (var allowedMethod in rule.AllowedMethods) {
                xmlWriter.WriteElementString("AllowedMethod", allowedMethod);
            }

            foreach (var allowedHeader in rule.AllowedHeaders) {
                xmlWriter.WriteElementString("AllowedHeader", allowedHeader);
            }

            if (rule.MaxAgeSeconds is { } maxAgeSeconds) {
                xmlWriter.WriteElementString("MaxAgeSeconds", maxAgeSeconds.ToString(CultureInfo.InvariantCulture));
            }

            foreach (var exposeHeader in rule.ExposeHeaders) {
                xmlWriter.WriteElementString("ExposeHeader", exposeHeader);
            }

            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "CORSConfiguration");
    }

    /// <summary>Writes an Error as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ErrorResponse"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteError(S3ErrorResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "Error");
        xmlWriter.WriteElementString("Code", response.Code);
        xmlWriter.WriteElementString("Message", response.Message);

        if (!string.IsNullOrWhiteSpace(response.Resource)) {
            xmlWriter.WriteElementString("Resource", response.Resource);
        }

        if (!string.IsNullOrWhiteSpace(response.RequestId)) {
            xmlWriter.WriteElementString("RequestId", response.RequestId);
        }

        if (!string.IsNullOrWhiteSpace(response.BucketName)) {
            xmlWriter.WriteElementString("BucketName", response.BucketName);
        }

        if (!string.IsNullOrWhiteSpace(response.Key)) {
            xmlWriter.WriteElementString("Key", response.Key);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "Error");
    }

    /// <summary>Writes a CopyObjectResult as an XML response body.</summary>
    /// <param name="response">The <see cref="S3CopyObjectResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteCopyObjectResult(S3CopyObjectResult response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return WriteCopyResultCore("CopyObjectResult", response);
    }

    public static string WriteCopyPartResult(S3CopyObjectResult response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return WriteCopyResultCore("CopyPartResult", response);
    }

    private static string WriteCopyResultCore(string rootElementName, S3CopyObjectResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, rootElementName);
        xmlWriter.WriteElementString("LastModified", FormatTimestamp(response.LastModifiedUtc));
        xmlWriter.WriteElementString("ETag", QuoteETag(response.ETag));

        if (!string.IsNullOrWhiteSpace(response.ChecksumCrc32)) {
            xmlWriter.WriteElementString("ChecksumCRC32", response.ChecksumCrc32);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumCrc32c)) {
            xmlWriter.WriteElementString("ChecksumCRC32C", response.ChecksumCrc32c);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumCrc64Nvme)) {
            xmlWriter.WriteElementString("ChecksumCRC64NVME", response.ChecksumCrc64Nvme);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumSha1)) {
            xmlWriter.WriteElementString("ChecksumSHA1", response.ChecksumSha1);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumSha256)) {
            xmlWriter.WriteElementString("ChecksumSHA256", response.ChecksumSha256);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumType)) {
            xmlWriter.WriteElementString("ChecksumType", response.ChecksumType);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, rootElementName);
    }

    /// <summary>Writes an InitiateMultipartUploadResult as an XML response body.</summary>
    /// <param name="response">The <see cref="S3InitiateMultipartUploadResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteInitiateMultipartUploadResult(S3InitiateMultipartUploadResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "InitiateMultipartUploadResult");
        xmlWriter.WriteElementString("Bucket", response.Bucket);
        xmlWriter.WriteElementString("Key", response.Key);
        xmlWriter.WriteElementString("UploadId", response.UploadId);
        if (!string.IsNullOrWhiteSpace(response.ChecksumAlgorithm)) {
            xmlWriter.WriteElementString("ChecksumAlgorithm", response.ChecksumAlgorithm);
        }
        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "InitiateMultipartUploadResult");
    }

    /// <summary>Writes a CompleteMultipartUploadResult as an XML response body.</summary>
    /// <param name="response">The <see cref="S3CompleteMultipartUploadResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteCompleteMultipartUploadResult(S3CompleteMultipartUploadResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "CompleteMultipartUploadResult");

        if (!string.IsNullOrWhiteSpace(response.Location)) {
            xmlWriter.WriteElementString("Location", response.Location);
        }

        xmlWriter.WriteElementString("Bucket", response.Bucket);
        xmlWriter.WriteElementString("Key", response.Key);
        xmlWriter.WriteElementString("ETag", QuoteETag(response.ETag));

        if (!string.IsNullOrWhiteSpace(response.ChecksumCrc32)) {
            xmlWriter.WriteElementString("ChecksumCRC32", response.ChecksumCrc32);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumCrc32c)) {
            xmlWriter.WriteElementString("ChecksumCRC32C", response.ChecksumCrc32c);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumCrc64Nvme)) {
            xmlWriter.WriteElementString("ChecksumCRC64NVME", response.ChecksumCrc64Nvme);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumSha1)) {
            xmlWriter.WriteElementString("ChecksumSHA1", response.ChecksumSha1);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumSha256)) {
            xmlWriter.WriteElementString("ChecksumSHA256", response.ChecksumSha256);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumType)) {
            xmlWriter.WriteElementString("ChecksumType", response.ChecksumType);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "CompleteMultipartUploadResult");
    }

    /// <summary>Writes a ListBucketResult as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ListBucketResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteListBucketResult(S3ListBucketResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "ListBucketResult");
        xmlWriter.WriteElementString("Name", response.Name);
        xmlWriter.WriteElementString("Prefix", EncodeS3ListValue(response.Prefix ?? string.Empty, response.EncodingType));

        if (response.IsV2) {
            if (!string.IsNullOrWhiteSpace(response.Delimiter)) {
                xmlWriter.WriteElementString("Delimiter", EncodeS3ListValue(response.Delimiter, response.EncodingType));
            }

            if (!string.IsNullOrWhiteSpace(response.StartAfter)) {
                xmlWriter.WriteElementString("StartAfter", EncodeS3ListValue(response.StartAfter, response.EncodingType));
            }

            if (!string.IsNullOrWhiteSpace(response.ContinuationToken)) {
                xmlWriter.WriteElementString("ContinuationToken", response.ContinuationToken);
            }

            if (!string.IsNullOrWhiteSpace(response.NextContinuationToken)) {
                xmlWriter.WriteElementString("NextContinuationToken", response.NextContinuationToken);
            }

            xmlWriter.WriteElementString("KeyCount", response.KeyCount.ToString(CultureInfo.InvariantCulture));
        }
        else {
            xmlWriter.WriteElementString("Marker", EncodeS3ListValue(response.Marker ?? string.Empty, response.EncodingType));

            if (!string.IsNullOrWhiteSpace(response.Delimiter)) {
                xmlWriter.WriteElementString("Delimiter", EncodeS3ListValue(response.Delimiter, response.EncodingType));
            }

            if (!string.IsNullOrWhiteSpace(response.NextMarker)) {
                xmlWriter.WriteElementString("NextMarker", EncodeS3ListValue(response.NextMarker, response.EncodingType));
            }
        }

        xmlWriter.WriteElementString("MaxKeys", response.MaxKeys.ToString(CultureInfo.InvariantCulture));
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");

        foreach (var content in response.Contents) {
            xmlWriter.WriteStartElement("Contents");
            xmlWriter.WriteElementString("Key", EncodeS3ListValue(content.Key, response.EncodingType));
            xmlWriter.WriteElementString("LastModified", FormatTimestamp(content.LastModifiedUtc));
            xmlWriter.WriteElementString("ETag", QuoteETag(content.ETag ?? string.Empty));
            xmlWriter.WriteElementString("Size", content.Size.ToString(CultureInfo.InvariantCulture));
            xmlWriter.WriteElementString("StorageClass", content.StorageClass);

            if (content.Owner is not null) {
                WriteOwner(xmlWriter, "Owner", content.Owner);
            }

            xmlWriter.WriteEndElement();
        }

        foreach (var commonPrefix in response.CommonPrefixes) {
            xmlWriter.WriteStartElement("CommonPrefixes");
            xmlWriter.WriteElementString("Prefix", EncodeS3ListValue(commonPrefix.Prefix, response.EncodingType));
            xmlWriter.WriteEndElement();
        }

        if (!string.IsNullOrWhiteSpace(response.EncodingType)) {
            xmlWriter.WriteElementString("EncodingType", response.EncodingType);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ListBucketResult");
    }

    /// <summary>Writes a ListObjectVersionsResult as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ListObjectVersionsResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteListObjectVersionsResult(S3ListObjectVersionsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "ListVersionsResult");
        xmlWriter.WriteElementString("Name", response.Name);
        xmlWriter.WriteElementString("Prefix", EncodeS3ListValue(response.Prefix ?? string.Empty, response.EncodingType));

        if (!string.IsNullOrWhiteSpace(response.Delimiter)) {
            xmlWriter.WriteElementString("Delimiter", EncodeS3ListValue(response.Delimiter, response.EncodingType));
        }

        if (!string.IsNullOrWhiteSpace(response.KeyMarker)) {
            xmlWriter.WriteElementString("KeyMarker", EncodeS3ListValue(response.KeyMarker, response.EncodingType));
        }

        if (!string.IsNullOrWhiteSpace(response.VersionIdMarker)) {
            xmlWriter.WriteElementString("VersionIdMarker", response.VersionIdMarker);
        }

        if (!string.IsNullOrWhiteSpace(response.NextKeyMarker)) {
            xmlWriter.WriteElementString("NextKeyMarker", EncodeS3ListValue(response.NextKeyMarker, response.EncodingType));
        }

        if (!string.IsNullOrWhiteSpace(response.NextVersionIdMarker)) {
            xmlWriter.WriteElementString("NextVersionIdMarker", response.NextVersionIdMarker);
        }

        xmlWriter.WriteElementString("MaxKeys", response.MaxKeys.ToString(CultureInfo.InvariantCulture));
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");

        foreach (var version in response.Versions) {
            xmlWriter.WriteStartElement(version.IsDeleteMarker ? "DeleteMarker" : "Version");
            xmlWriter.WriteElementString("Key", EncodeS3ListValue(version.Key, response.EncodingType));
            xmlWriter.WriteElementString("VersionId", version.VersionId);
            xmlWriter.WriteElementString("IsLatest", version.IsLatest ? "true" : "false");
            xmlWriter.WriteElementString("LastModified", FormatTimestamp(version.LastModifiedUtc));

            if (!version.IsDeleteMarker) {
                xmlWriter.WriteElementString("ETag", QuoteETag(version.ETag ?? string.Empty));
                xmlWriter.WriteElementString("Size", version.Size.ToString(CultureInfo.InvariantCulture));
                xmlWriter.WriteElementString("StorageClass", version.StorageClass);
            }

            xmlWriter.WriteEndElement();
        }

        foreach (var commonPrefix in response.CommonPrefixes) {
            xmlWriter.WriteStartElement("CommonPrefixes");
            xmlWriter.WriteElementString("Prefix", EncodeS3ListValue(commonPrefix.Prefix, response.EncodingType));
            xmlWriter.WriteEndElement();
        }

        if (!string.IsNullOrWhiteSpace(response.EncodingType)) {
            xmlWriter.WriteElementString("EncodingType", response.EncodingType);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ListVersionsResult");
    }

    /// <summary>Writes a ListMultipartUploadsResult as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ListMultipartUploadsResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteListMultipartUploadsResult(S3ListMultipartUploadsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "ListMultipartUploadsResult");
        xmlWriter.WriteElementString("Bucket", response.Bucket);
        xmlWriter.WriteElementString("KeyMarker", EncodeS3ListValue(response.KeyMarker ?? string.Empty, response.EncodingType));
        xmlWriter.WriteElementString("UploadIdMarker", response.UploadIdMarker ?? string.Empty);
        xmlWriter.WriteElementString("Prefix", EncodeS3ListValue(response.Prefix ?? string.Empty, response.EncodingType));

        if (!string.IsNullOrWhiteSpace(response.Delimiter)) {
            xmlWriter.WriteElementString("Delimiter", EncodeS3ListValue(response.Delimiter, response.EncodingType));
        }

        if (!string.IsNullOrWhiteSpace(response.NextKeyMarker)) {
            xmlWriter.WriteElementString("NextKeyMarker", EncodeS3ListValue(response.NextKeyMarker, response.EncodingType));
        }

        if (!string.IsNullOrWhiteSpace(response.NextUploadIdMarker)) {
            xmlWriter.WriteElementString("NextUploadIdMarker", response.NextUploadIdMarker);
        }

        if (!string.IsNullOrWhiteSpace(response.EncodingType)) {
            xmlWriter.WriteElementString("EncodingType", response.EncodingType);
        }

        xmlWriter.WriteElementString("MaxUploads", response.MaxUploads.ToString(CultureInfo.InvariantCulture));
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");

        foreach (var upload in response.Uploads) {
            xmlWriter.WriteStartElement("Upload");
            xmlWriter.WriteElementString("Key", EncodeS3ListValue(upload.Key, response.EncodingType));
            xmlWriter.WriteElementString("UploadId", upload.UploadId);

            if (upload.Initiator is not null) {
                WriteOwner(xmlWriter, "Initiator", upload.Initiator);
            }

            if (upload.Owner is not null) {
                WriteOwner(xmlWriter, "Owner", upload.Owner);
            }

            xmlWriter.WriteElementString("Initiated", FormatTimestamp(upload.InitiatedAtUtc));

            if (!string.IsNullOrWhiteSpace(upload.ChecksumAlgorithm)) {
                xmlWriter.WriteElementString("ChecksumAlgorithm", upload.ChecksumAlgorithm);
            }

            xmlWriter.WriteElementString("StorageClass", upload.StorageClass);
            xmlWriter.WriteEndElement();
        }

        foreach (var commonPrefix in response.CommonPrefixes) {
            xmlWriter.WriteStartElement("CommonPrefixes");
            xmlWriter.WriteElementString("Prefix", EncodeS3ListValue(commonPrefix.Prefix, response.EncodingType));
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ListMultipartUploadsResult");
    }

    /// <summary>Writes a ListPartsResult as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ListPartsResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteListPartsResult(S3ListPartsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "ListPartsResult");
        xmlWriter.WriteElementString("Bucket", response.Bucket);
        xmlWriter.WriteElementString("Key", EncodeS3ListValue(response.Key, response.EncodingType));
        xmlWriter.WriteElementString("UploadId", response.UploadId);

        if (response.Initiator is not null) {
            WriteOwner(xmlWriter, "Initiator", response.Initiator);
        }

        if (response.Owner is not null) {
            WriteOwner(xmlWriter, "Owner", response.Owner);
        }

        xmlWriter.WriteElementString("PartNumberMarker", response.PartNumberMarker.ToString(CultureInfo.InvariantCulture));

        if (response.NextPartNumberMarker.HasValue) {
            xmlWriter.WriteElementString("NextPartNumberMarker", response.NextPartNumberMarker.Value.ToString(CultureInfo.InvariantCulture));
        }

        xmlWriter.WriteElementString("MaxParts", response.MaxParts.ToString(CultureInfo.InvariantCulture));
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");

        if (!string.IsNullOrWhiteSpace(response.StorageClass)) {
            xmlWriter.WriteElementString("StorageClass", response.StorageClass);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumAlgorithm)) {
            xmlWriter.WriteElementString("ChecksumAlgorithm", response.ChecksumAlgorithm);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumType)) {
            xmlWriter.WriteElementString("ChecksumType", response.ChecksumType);
        }

        if (!string.IsNullOrWhiteSpace(response.EncodingType)) {
            xmlWriter.WriteElementString("EncodingType", response.EncodingType);
        }

        foreach (var part in response.Parts) {
            xmlWriter.WriteStartElement("Part");
            xmlWriter.WriteElementString("PartNumber", part.PartNumber.ToString(CultureInfo.InvariantCulture));
            xmlWriter.WriteElementString("LastModified", FormatTimestamp(part.LastModifiedUtc));
            xmlWriter.WriteElementString("ETag", QuoteETag(part.ETag));
            xmlWriter.WriteElementString("Size", part.Size.ToString(CultureInfo.InvariantCulture));

            if (!string.IsNullOrWhiteSpace(part.ChecksumCrc32)) {
                xmlWriter.WriteElementString("ChecksumCRC32", part.ChecksumCrc32);
            }

            if (!string.IsNullOrWhiteSpace(part.ChecksumCrc32c)) {
                xmlWriter.WriteElementString("ChecksumCRC32C", part.ChecksumCrc32c);
            }

            if (!string.IsNullOrWhiteSpace(part.ChecksumCrc64Nvme)) {
                xmlWriter.WriteElementString("ChecksumCRC64NVME", part.ChecksumCrc64Nvme);
            }

            if (!string.IsNullOrWhiteSpace(part.ChecksumSha1)) {
                xmlWriter.WriteElementString("ChecksumSHA1", part.ChecksumSha1);
            }

            if (!string.IsNullOrWhiteSpace(part.ChecksumSha256)) {
                xmlWriter.WriteElementString("ChecksumSHA256", part.ChecksumSha256);
            }

            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ListPartsResult");
    }

    /// <summary>Writes a ListAllMyBucketsResult as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ListAllMyBucketsResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteListAllMyBucketsResult(S3ListAllMyBucketsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "ListAllMyBucketsResult");

        WriteOwner(xmlWriter, "Owner", response.Owner);

        xmlWriter.WriteStartElement("Buckets");
        foreach (var bucket in response.Buckets) {
            xmlWriter.WriteStartElement("Bucket");
            xmlWriter.WriteElementString("Name", bucket.Name);
            xmlWriter.WriteElementString("CreationDate", FormatTimestamp(bucket.CreationDateUtc));
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ListAllMyBucketsResult");
    }

    /// <summary>Writes a DeleteObjectsResult as an XML response body.</summary>
    /// <param name="response">The <see cref="S3DeleteObjectsResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteDeleteObjectsResult(S3DeleteObjectsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "DeleteResult");

        foreach (var deleted in response.Deleted) {
            xmlWriter.WriteStartElement("Deleted");
            xmlWriter.WriteElementString("Key", deleted.Key);

            if (!string.IsNullOrWhiteSpace(deleted.VersionId)) {
                xmlWriter.WriteElementString("VersionId", deleted.VersionId);
            }

            if (deleted.DeleteMarker) {
                xmlWriter.WriteElementString("DeleteMarker", "true");
            }

            if (!string.IsNullOrWhiteSpace(deleted.DeleteMarkerVersionId)) {
                xmlWriter.WriteElementString("DeleteMarkerVersionId", deleted.DeleteMarkerVersionId);
            }

            xmlWriter.WriteEndElement();
        }

        foreach (var error in response.Errors) {
            xmlWriter.WriteStartElement("Error");
            xmlWriter.WriteElementString("Key", error.Key);

            if (!string.IsNullOrWhiteSpace(error.VersionId)) {
                xmlWriter.WriteElementString("VersionId", error.VersionId);
            }

            xmlWriter.WriteElementString("Code", error.Code);
            xmlWriter.WriteElementString("Message", error.Message);
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "DeleteResult");
    }

    /// <summary>Writes an ObjectTagging as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ObjectTagging"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteObjectTagging(S3ObjectTagging response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "Tagging");
        xmlWriter.WriteStartElement("TagSet");

        foreach (var tag in response.TagSet.OrderBy(static tag => tag.Key, StringComparer.Ordinal)) {
            xmlWriter.WriteStartElement("Tag");
            xmlWriter.WriteElementString("Key", tag.Key);
            xmlWriter.WriteElementString("Value", tag.Value);
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "Tagging");
    }

    /// <summary>Writes an ObjectRetention as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ObjectRetention"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteObjectRetention(S3ObjectRetention response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("Retention");

        if (!string.IsNullOrWhiteSpace(response.Mode)) {
            xmlWriter.WriteElementString("Mode", response.Mode);
        }

        if (response.RetainUntilDateUtc is { } retainUntilDateUtc) {
            xmlWriter.WriteElementString("RetainUntilDate", XmlConvert.ToString(retainUntilDateUtc.UtcDateTime, XmlDateTimeSerializationMode.Utc));
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "Retention");
    }

    /// <summary>Writes an ObjectLegalHold as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ObjectLegalHold"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteObjectLegalHold(S3ObjectLegalHold response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("LegalHold");

        if (!string.IsNullOrWhiteSpace(response.Status)) {
            xmlWriter.WriteElementString("Status", response.Status);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "LegalHold");
    }

    /// <summary>Writes a GetObjectAttributesResponse as an XML response body.</summary>
    /// <param name="response">The <see cref="GetObjectAttributesResponse"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteGetObjectAttributesResponse(GetObjectAttributesResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("GetObjectAttributesResponse");

        if (response.ETag is not null)
        {
            xmlWriter.WriteElementString("ETag", response.ETag);
        }

        if (response.Checksums is { Count: > 0 })
        {
            xmlWriter.WriteStartElement("Checksum");
            foreach (var (algo, value) in response.Checksums)
            {
                var elementName = algo.ToLowerInvariant() switch
                {
                    "crc32" => "ChecksumCRC32",
                    "crc32c" => "ChecksumCRC32C",
                    "sha1" => "ChecksumSHA1",
                    "sha256" => "ChecksumSHA256",
                    "crc64nvme" => "ChecksumCRC64NVME",
                    _ => null
                };
                if (elementName is not null)
                {
                    xmlWriter.WriteElementString(elementName, value);
                }
            }
            xmlWriter.WriteEndElement();
        }

        if (response.ObjectParts is not null)
        {
            xmlWriter.WriteStartElement("ObjectParts");
            xmlWriter.WriteElementString("TotalPartsCount", response.ObjectParts.TotalPartsCount.ToString(CultureInfo.InvariantCulture));
            if (response.ObjectParts.PartNumberMarker.HasValue)
                xmlWriter.WriteElementString("PartNumberMarker", response.ObjectParts.PartNumberMarker.Value.ToString(CultureInfo.InvariantCulture));
            if (response.ObjectParts.NextPartNumberMarker.HasValue)
                xmlWriter.WriteElementString("NextPartNumberMarker", response.ObjectParts.NextPartNumberMarker.Value.ToString(CultureInfo.InvariantCulture));
            if (response.ObjectParts.MaxParts.HasValue)
                xmlWriter.WriteElementString("MaxParts", response.ObjectParts.MaxParts.Value.ToString(CultureInfo.InvariantCulture));
            xmlWriter.WriteElementString("IsTruncated", response.ObjectParts.IsTruncated ? "true" : "false");

            if (response.ObjectParts.Parts is { Count: > 0 })
            {
                foreach (var part in response.ObjectParts.Parts)
                {
                    xmlWriter.WriteStartElement("Part");
                    xmlWriter.WriteElementString("PartNumber", part.PartNumber.ToString(CultureInfo.InvariantCulture));
                    xmlWriter.WriteElementString("Size", part.Size.ToString(CultureInfo.InvariantCulture));
                    if (part.ChecksumCrc32 is not null)
                        xmlWriter.WriteElementString("ChecksumCRC32", part.ChecksumCrc32);
                    if (part.ChecksumCrc32C is not null)
                        xmlWriter.WriteElementString("ChecksumCRC32C", part.ChecksumCrc32C);
                    if (part.ChecksumSha1 is not null)
                        xmlWriter.WriteElementString("ChecksumSHA1", part.ChecksumSha1);
                    if (part.ChecksumSha256 is not null)
                        xmlWriter.WriteElementString("ChecksumSHA256", part.ChecksumSha256);
                    if (part.ChecksumCrc64Nvme is not null)
                        xmlWriter.WriteElementString("ChecksumCRC64NVME", part.ChecksumCrc64Nvme);
                    xmlWriter.WriteEndElement();
                }
            }
            xmlWriter.WriteEndElement();
        }

        if (response.StorageClass is not null)
        {
            xmlWriter.WriteElementString("StorageClass", response.StorageClass);
        }

        if (response.ObjectSize.HasValue)
        {
            xmlWriter.WriteElementString("ObjectSize", response.ObjectSize.Value.ToString(CultureInfo.InvariantCulture));
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "GetObjectAttributesResponse");
    }

    /// <summary>Writes an AccessControlPolicy as an XML response body.</summary>
    /// <param name="response">The <see cref="S3AccessControlPolicy"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteAccessControlPolicy(S3AccessControlPolicy response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        WriteStartRootElement(xmlWriter, "AccessControlPolicy");
        xmlWriter.WriteStartElement("Owner");
        xmlWriter.WriteElementString("ID", response.Owner.Id);
        if (!string.IsNullOrWhiteSpace(response.Owner.DisplayName)) {
            xmlWriter.WriteElementString("DisplayName", response.Owner.DisplayName);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteStartElement("AccessControlList");

        foreach (var grant in response.Grants) {
            xmlWriter.WriteStartElement("Grant");
            xmlWriter.WriteStartElement("Grantee");
            xmlWriter.WriteAttributeString("xmlns", "xsi", null, "http://www.w3.org/2001/XMLSchema-instance");
            xmlWriter.WriteAttributeString("xsi", "type", "http://www.w3.org/2001/XMLSchema-instance", grant.Grantee.Type);

            if (!string.IsNullOrWhiteSpace(grant.Grantee.Id)) {
                xmlWriter.WriteElementString("ID", grant.Grantee.Id);
            }

            if (!string.IsNullOrWhiteSpace(grant.Grantee.DisplayName)) {
                xmlWriter.WriteElementString("DisplayName", grant.Grantee.DisplayName);
            }

            if (!string.IsNullOrWhiteSpace(grant.Grantee.Uri)) {
                xmlWriter.WriteElementString("URI", grant.Grantee.Uri);
            }

            xmlWriter.WriteEndElement();
            xmlWriter.WriteElementString("Permission", grant.Permission);
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "AccessControlPolicy");
    }

    /// <summary>Writes a BucketTagging as an XML response body.</summary>
    /// <param name="response">The <see cref="S3BucketTagging"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteBucketTagging(S3BucketTagging response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("Tagging");
        xmlWriter.WriteStartElement("TagSet");

        foreach (var tag in response.TagSet) {
            xmlWriter.WriteStartElement("Tag");
            xmlWriter.WriteElementString("Key", tag.Key);
            xmlWriter.WriteElementString("Value", tag.Value);
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "Tagging");
    }

    /// <summary>Writes a BucketLoggingStatus as an XML response body.</summary>
    /// <param name="response">The <see cref="S3BucketLoggingStatus"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteBucketLoggingStatus(S3BucketLoggingStatus response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("BucketLoggingStatus");

        if (response.LoggingEnabled is { } loggingEnabled) {
            xmlWriter.WriteStartElement("LoggingEnabled");
            xmlWriter.WriteElementString("TargetBucket", loggingEnabled.TargetBucket);
            xmlWriter.WriteElementString("TargetPrefix", loggingEnabled.TargetPrefix);
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "BucketLoggingStatus");
    }

    /// <summary>Writes a WebsiteConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3WebsiteConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteWebsiteConfiguration(S3WebsiteConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("WebsiteConfiguration");

        if (response.RedirectAllRequestsTo is { } redirect) {
            xmlWriter.WriteStartElement("RedirectAllRequestsTo");
            xmlWriter.WriteElementString("HostName", redirect.HostName);
            if (!string.IsNullOrWhiteSpace(redirect.Protocol)) {
                xmlWriter.WriteElementString("Protocol", redirect.Protocol);
            }
            xmlWriter.WriteEndElement();
        }

        if (response.IndexDocument is { } indexDocument) {
            xmlWriter.WriteStartElement("IndexDocument");
            xmlWriter.WriteElementString("Suffix", indexDocument.Suffix);
            xmlWriter.WriteEndElement();
        }

        if (response.ErrorDocument is { } errorDocument) {
            xmlWriter.WriteStartElement("ErrorDocument");
            xmlWriter.WriteElementString("Key", errorDocument.Key);
            xmlWriter.WriteEndElement();
        }

        if (response.RoutingRules.Count > 0) {
            xmlWriter.WriteStartElement("RoutingRules");
            foreach (var rule in response.RoutingRules) {
                xmlWriter.WriteStartElement("RoutingRule");

                if (rule.Condition is { } condition) {
                    xmlWriter.WriteStartElement("Condition");
                    if (!string.IsNullOrWhiteSpace(condition.KeyPrefixEquals)) {
                        xmlWriter.WriteElementString("KeyPrefixEquals", condition.KeyPrefixEquals);
                    }
                    if (condition.HttpErrorCodeReturnedEquals is { } httpErrorCode) {
                        xmlWriter.WriteElementString("HttpErrorCodeReturnedEquals", httpErrorCode.ToString(CultureInfo.InvariantCulture));
                    }
                    xmlWriter.WriteEndElement();
                }

                xmlWriter.WriteStartElement("Redirect");
                if (!string.IsNullOrWhiteSpace(rule.Redirect.HostName)) {
                    xmlWriter.WriteElementString("HostName", rule.Redirect.HostName);
                }
                if (!string.IsNullOrWhiteSpace(rule.Redirect.Protocol)) {
                    xmlWriter.WriteElementString("Protocol", rule.Redirect.Protocol);
                }
                if (!string.IsNullOrWhiteSpace(rule.Redirect.ReplaceKeyPrefixWith)) {
                    xmlWriter.WriteElementString("ReplaceKeyPrefixWith", rule.Redirect.ReplaceKeyPrefixWith);
                }
                if (!string.IsNullOrWhiteSpace(rule.Redirect.ReplaceKeyWith)) {
                    xmlWriter.WriteElementString("ReplaceKeyWith", rule.Redirect.ReplaceKeyWith);
                }
                if (rule.Redirect.HttpRedirectCode is { } httpRedirectCode) {
                    xmlWriter.WriteElementString("HttpRedirectCode", httpRedirectCode.ToString(CultureInfo.InvariantCulture));
                }
                xmlWriter.WriteEndElement();

                xmlWriter.WriteEndElement();
            }
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "WebsiteConfiguration");
    }

    /// <summary>Writes a RequestPaymentConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3RequestPaymentConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteRequestPaymentConfiguration(S3RequestPaymentConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("RequestPaymentConfiguration");
        xmlWriter.WriteElementString("Payer", response.Payer);
        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "RequestPaymentConfiguration");
    }

    /// <summary>Writes an AccelerateConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3AccelerateConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteAccelerateConfiguration(S3AccelerateConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("AccelerateConfiguration");

        if (!string.IsNullOrWhiteSpace(response.Status)) {
            xmlWriter.WriteElementString("Status", response.Status);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "AccelerateConfiguration");
    }

    /// <summary>Writes a LifecycleConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3LifecycleConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteLifecycleConfiguration(S3LifecycleConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("LifecycleConfiguration");

        foreach (var rule in response.Rules) {
            xmlWriter.WriteStartElement("Rule");

            if (!string.IsNullOrWhiteSpace(rule.Id)) {
                xmlWriter.WriteElementString("ID", rule.Id);
            }

            var hasFilterPrefix = !string.IsNullOrWhiteSpace(rule.FilterPrefix);
            var hasFilterTags = rule.FilterTags is { Count: > 0 };
            if (hasFilterPrefix || hasFilterTags) {
                xmlWriter.WriteStartElement("Filter");
                if (hasFilterPrefix && !hasFilterTags) {
                    xmlWriter.WriteElementString("Prefix", rule.FilterPrefix);
                } else if (!hasFilterPrefix && hasFilterTags && rule.FilterTags!.Count == 1) {
                    xmlWriter.WriteStartElement("Tag");
                    xmlWriter.WriteElementString("Key", rule.FilterTags![0].Key);
                    xmlWriter.WriteElementString("Value", rule.FilterTags[0].Value);
                    xmlWriter.WriteEndElement();
                } else {
                    xmlWriter.WriteStartElement("And");
                    if (hasFilterPrefix) {
                        xmlWriter.WriteElementString("Prefix", rule.FilterPrefix);
                    }
                    if (hasFilterTags) {
                        foreach (var tag in rule.FilterTags!) {
                            xmlWriter.WriteStartElement("Tag");
                            xmlWriter.WriteElementString("Key", tag.Key);
                            xmlWriter.WriteElementString("Value", tag.Value);
                            xmlWriter.WriteEndElement();
                        }
                    }
                    xmlWriter.WriteEndElement();
                }
                xmlWriter.WriteEndElement();
            } else {
                xmlWriter.WriteStartElement("Filter");
                xmlWriter.WriteElementString("Prefix", string.Empty);
                xmlWriter.WriteEndElement();
            }

            xmlWriter.WriteElementString("Status", rule.Status);

            if (rule.ExpirationDays is not null || rule.ExpirationDate is not null || rule.ExpiredObjectDeleteMarker is not null) {
                xmlWriter.WriteStartElement("Expiration");
                if (rule.ExpirationDays is { } expirationDays) {
                    xmlWriter.WriteElementString("Days", expirationDays.ToString(CultureInfo.InvariantCulture));
                }
                if (!string.IsNullOrWhiteSpace(rule.ExpirationDate)) {
                    xmlWriter.WriteElementString("Date", rule.ExpirationDate);
                }
                if (rule.ExpiredObjectDeleteMarker is { } expiredObjectDeleteMarker) {
                    xmlWriter.WriteElementString("ExpiredObjectDeleteMarker", expiredObjectDeleteMarker ? "true" : "false");
                }
                xmlWriter.WriteEndElement();
            }

            foreach (var transition in rule.Transitions) {
                xmlWriter.WriteStartElement("Transition");
                if (transition.Days is { } days) {
                    xmlWriter.WriteElementString("Days", days.ToString(CultureInfo.InvariantCulture));
                }
                if (!string.IsNullOrWhiteSpace(transition.Date)) {
                    xmlWriter.WriteElementString("Date", transition.Date);
                }
                xmlWriter.WriteElementString("StorageClass", transition.StorageClass);
                xmlWriter.WriteEndElement();
            }

            if (rule.NoncurrentVersionExpirationDays is { } noncurrentDays) {
                xmlWriter.WriteStartElement("NoncurrentVersionExpiration");
                xmlWriter.WriteElementString("NoncurrentDays", noncurrentDays.ToString(CultureInfo.InvariantCulture));
                xmlWriter.WriteEndElement();
            }

            foreach (var nvt in rule.NoncurrentVersionTransitions) {
                xmlWriter.WriteStartElement("NoncurrentVersionTransition");
                if (nvt.NoncurrentDays is { } nvtDays) {
                    xmlWriter.WriteElementString("NoncurrentDays", nvtDays.ToString(CultureInfo.InvariantCulture));
                }
                xmlWriter.WriteElementString("StorageClass", nvt.StorageClass);
                xmlWriter.WriteEndElement();
            }

            if (rule.AbortIncompleteMultipartUploadDaysAfterInitiation is { } abortDays) {
                xmlWriter.WriteStartElement("AbortIncompleteMultipartUpload");
                xmlWriter.WriteElementString("DaysAfterInitiation", abortDays.ToString(CultureInfo.InvariantCulture));
                xmlWriter.WriteEndElement();
            }

            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "LifecycleConfiguration");
    }

    /// <summary>Writes a ReplicationConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ReplicationConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteReplicationConfiguration(S3ReplicationConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ReplicationConfiguration");

        if (!string.IsNullOrWhiteSpace(response.Role)) {
            xmlWriter.WriteElementString("Role", response.Role);
        }

        foreach (var rule in response.Rules) {
            xmlWriter.WriteStartElement("Rule");

            if (!string.IsNullOrWhiteSpace(rule.Id)) {
                xmlWriter.WriteElementString("ID", rule.Id);
            }

            if (rule.Priority is { } priority) {
                xmlWriter.WriteElementString("Priority", priority.ToString(CultureInfo.InvariantCulture));
            }

            xmlWriter.WriteElementString("Status", rule.Status);

            if (!string.IsNullOrWhiteSpace(rule.FilterPrefix)) {
                xmlWriter.WriteStartElement("Filter");
                xmlWriter.WriteElementString("Prefix", rule.FilterPrefix);
                xmlWriter.WriteEndElement();
            }

            xmlWriter.WriteStartElement("Destination");
            xmlWriter.WriteElementString("Bucket", rule.Destination.Bucket);
            if (!string.IsNullOrWhiteSpace(rule.Destination.StorageClass)) {
                xmlWriter.WriteElementString("StorageClass", rule.Destination.StorageClass);
            }
            if (!string.IsNullOrWhiteSpace(rule.Destination.Account)) {
                xmlWriter.WriteElementString("Account", rule.Destination.Account);
            }
            xmlWriter.WriteEndElement();

            if (rule.DeleteMarkerReplication is { } deleteMarkerReplication) {
                xmlWriter.WriteStartElement("DeleteMarkerReplication");
                xmlWriter.WriteElementString("Status", deleteMarkerReplication ? "Enabled" : "Disabled");
                xmlWriter.WriteEndElement();
            }

            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ReplicationConfiguration");
    }

    /// <summary>Writes a NotificationConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3NotificationConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteNotificationConfiguration(S3NotificationConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("NotificationConfiguration");

        foreach (var topic in response.TopicConfigurations) {
            xmlWriter.WriteStartElement("TopicConfiguration");
            if (!string.IsNullOrWhiteSpace(topic.Id)) {
                xmlWriter.WriteElementString("Id", topic.Id);
            }
            xmlWriter.WriteElementString("Topic", topic.Topic);
            foreach (var evt in topic.Events) {
                xmlWriter.WriteElementString("Event", evt);
            }
            WriteNotificationFilter(xmlWriter, topic.Filter);
            xmlWriter.WriteEndElement();
        }

        foreach (var queue in response.QueueConfigurations) {
            xmlWriter.WriteStartElement("QueueConfiguration");
            if (!string.IsNullOrWhiteSpace(queue.Id)) {
                xmlWriter.WriteElementString("Id", queue.Id);
            }
            xmlWriter.WriteElementString("Queue", queue.Queue);
            foreach (var evt in queue.Events) {
                xmlWriter.WriteElementString("Event", evt);
            }
            WriteNotificationFilter(xmlWriter, queue.Filter);
            xmlWriter.WriteEndElement();
        }

        foreach (var cf in response.CloudFunctionConfigurations) {
            xmlWriter.WriteStartElement("CloudFunctionConfiguration");
            if (!string.IsNullOrWhiteSpace(cf.Id)) {
                xmlWriter.WriteElementString("Id", cf.Id);
            }
            xmlWriter.WriteElementString("CloudFunction", cf.CloudFunction);
            foreach (var evt in cf.Events) {
                xmlWriter.WriteElementString("Event", evt);
            }
            WriteNotificationFilter(xmlWriter, cf.Filter);
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "NotificationConfiguration");
    }

    /// <summary>Writes an ObjectLockConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ObjectLockConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteObjectLockConfiguration(S3ObjectLockConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ObjectLockConfiguration");

        if (!string.IsNullOrWhiteSpace(response.ObjectLockEnabled)) {
            xmlWriter.WriteElementString("ObjectLockEnabled", response.ObjectLockEnabled);
        }

        if (response.DefaultRetention is { } retention) {
            xmlWriter.WriteStartElement("Rule");
            xmlWriter.WriteStartElement("DefaultRetention");
            if (!string.IsNullOrWhiteSpace(retention.Mode)) {
                xmlWriter.WriteElementString("Mode", retention.Mode);
            }
            if (retention.Days is { } days) {
                xmlWriter.WriteElementString("Days", days.ToString(CultureInfo.InvariantCulture));
            }
            if (retention.Years is { } years) {
                xmlWriter.WriteElementString("Years", years.ToString(CultureInfo.InvariantCulture));
            }
            xmlWriter.WriteEndElement();
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ObjectLockConfiguration");
    }

    /// <summary>Writes an AnalyticsConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3AnalyticsConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteAnalyticsConfiguration(S3AnalyticsConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("AnalyticsConfiguration");
        xmlWriter.WriteElementString("Id", response.Id);

        var hasPrefix = !string.IsNullOrWhiteSpace(response.FilterPrefix);
        var hasTags = response.FilterTags is { Count: > 0 };
        if (hasPrefix || hasTags) {
            xmlWriter.WriteStartElement("Filter");
            if (hasPrefix && !hasTags) {
                xmlWriter.WriteElementString("Prefix", response.FilterPrefix);
            } else if (!hasPrefix && hasTags && response.FilterTags!.Count == 1) {
                xmlWriter.WriteStartElement("Tag");
                xmlWriter.WriteElementString("Key", response.FilterTags![0].Key);
                xmlWriter.WriteElementString("Value", response.FilterTags[0].Value);
                xmlWriter.WriteEndElement();
            } else {
                xmlWriter.WriteStartElement("And");
                if (hasPrefix) {
                    xmlWriter.WriteElementString("Prefix", response.FilterPrefix);
                }
                if (hasTags) {
                    foreach (var tag in response.FilterTags!) {
                        xmlWriter.WriteStartElement("Tag");
                        xmlWriter.WriteElementString("Key", tag.Key);
                        xmlWriter.WriteElementString("Value", tag.Value);
                        xmlWriter.WriteEndElement();
                    }
                }
                xmlWriter.WriteEndElement();
            }
            xmlWriter.WriteEndElement();
        }

        if (response.StorageClassAnalysis is { } storageClassAnalysis) {
            xmlWriter.WriteStartElement("StorageClassAnalysis");
            if (storageClassAnalysis.DataExport is { } dataExport) {
                xmlWriter.WriteStartElement("DataExport");
                xmlWriter.WriteElementString("OutputSchemaVersion", dataExport.OutputSchemaVersion);
                if (dataExport.Destination is { } destination) {
                    xmlWriter.WriteStartElement("Destination");
                    xmlWriter.WriteStartElement("S3BucketDestination");
                    xmlWriter.WriteElementString("Format", destination.Format);
                    if (!string.IsNullOrWhiteSpace(destination.BucketAccountId)) {
                        xmlWriter.WriteElementString("BucketAccountId", destination.BucketAccountId);
                    }
                    xmlWriter.WriteElementString("Bucket", destination.Bucket);
                    if (!string.IsNullOrWhiteSpace(destination.Prefix)) {
                        xmlWriter.WriteElementString("Prefix", destination.Prefix);
                    }
                    xmlWriter.WriteEndElement();
                    xmlWriter.WriteEndElement();
                }
                xmlWriter.WriteEndElement();
            }
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "AnalyticsConfiguration");
    }

    /// <summary>Writes a MetricsConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3MetricsConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteMetricsConfiguration(S3MetricsConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("MetricsConfiguration");
        xmlWriter.WriteElementString("Id", response.Id);

        if (response.Filter is { } filter) {
            xmlWriter.WriteStartElement("Filter");
            var hasPrefix = !string.IsNullOrWhiteSpace(filter.Prefix);
            var hasAccessPointArn = !string.IsNullOrWhiteSpace(filter.AccessPointArn);
            var hasTags = filter.Tags.Count > 0;
            var multipleConditions = (hasPrefix ? 1 : 0) + (hasAccessPointArn ? 1 : 0) + (hasTags ? filter.Tags.Count : 0) > 1;

            if (multipleConditions) {
                xmlWriter.WriteStartElement("And");
            }

            if (hasPrefix) {
                xmlWriter.WriteElementString("Prefix", filter.Prefix);
            }
            if (hasAccessPointArn) {
                xmlWriter.WriteElementString("AccessPointArn", filter.AccessPointArn);
            }
            foreach (var tag in filter.Tags) {
                xmlWriter.WriteStartElement("Tag");
                xmlWriter.WriteElementString("Key", tag.Key);
                xmlWriter.WriteElementString("Value", tag.Value);
                xmlWriter.WriteEndElement();
            }

            if (multipleConditions) {
                xmlWriter.WriteEndElement();
            }

            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "MetricsConfiguration");
    }

    /// <summary>Writes an InventoryConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3InventoryConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteInventoryConfiguration(S3InventoryConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("InventoryConfiguration");
        xmlWriter.WriteElementString("Id", response.Id);
        xmlWriter.WriteElementString("IsEnabled", response.IsEnabled ? "true" : "false");
        xmlWriter.WriteElementString("IncludedObjectVersions", response.IncludedObjectVersions);

        if (response.Filter is { } filter) {
            xmlWriter.WriteStartElement("Filter");
            if (!string.IsNullOrWhiteSpace(filter.Prefix)) {
                xmlWriter.WriteElementString("Prefix", filter.Prefix);
            }
            xmlWriter.WriteEndElement();
        }

        if (response.Destination is { } dest) {
            xmlWriter.WriteStartElement("Destination");
            if (dest.S3BucketDestination is { } s3Dest) {
                xmlWriter.WriteStartElement("S3BucketDestination");
                xmlWriter.WriteElementString("Format", s3Dest.Format);
                if (!string.IsNullOrWhiteSpace(s3Dest.AccountId)) {
                    xmlWriter.WriteElementString("AccountId", s3Dest.AccountId);
                }
                xmlWriter.WriteElementString("Bucket", s3Dest.Bucket);
                if (!string.IsNullOrWhiteSpace(s3Dest.Prefix)) {
                    xmlWriter.WriteElementString("Prefix", s3Dest.Prefix);
                }
                xmlWriter.WriteEndElement();
            }
            xmlWriter.WriteEndElement();
        }

        if (response.Schedule is { } schedule) {
            xmlWriter.WriteStartElement("Schedule");
            xmlWriter.WriteElementString("Frequency", schedule.Frequency);
            xmlWriter.WriteEndElement();
        }

        if (response.OptionalFields.Count > 0) {
            xmlWriter.WriteStartElement("OptionalFields");
            foreach (var field in response.OptionalFields) {
                xmlWriter.WriteElementString("Field", field);
            }
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "InventoryConfiguration");
    }

    /// <summary>Writes an IntelligentTieringConfiguration as an XML response body.</summary>
    /// <param name="response">The <see cref="S3IntelligentTieringConfiguration"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteIntelligentTieringConfiguration(S3IntelligentTieringConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("IntelligentTieringConfiguration");
        xmlWriter.WriteElementString("Id", response.Id);
        xmlWriter.WriteElementString("Status", response.Status);

        if (response.Filter is { } filter) {
            xmlWriter.WriteStartElement("Filter");
            var hasPrefix = !string.IsNullOrWhiteSpace(filter.Prefix);
            var hasTags = filter.Tags.Count > 0;
            var multipleConditions = (hasPrefix ? 1 : 0) + (hasTags ? filter.Tags.Count : 0) > 1;

            if (multipleConditions) {
                xmlWriter.WriteStartElement("And");
            }

            if (hasPrefix) {
                xmlWriter.WriteElementString("Prefix", filter.Prefix);
            }
            foreach (var tag in filter.Tags) {
                xmlWriter.WriteStartElement("Tag");
                xmlWriter.WriteElementString("Key", tag.Key);
                xmlWriter.WriteElementString("Value", tag.Value);
                xmlWriter.WriteEndElement();
            }

            if (multipleConditions) {
                xmlWriter.WriteEndElement();
            }

            xmlWriter.WriteEndElement();
        }

        foreach (var tiering in response.Tierings) {
            xmlWriter.WriteStartElement("Tiering");
            xmlWriter.WriteElementString("AccessTier", tiering.AccessTier);
            xmlWriter.WriteElementString("Days", tiering.Days.ToString(CultureInfo.InvariantCulture));
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "IntelligentTieringConfiguration");
    }

    /// <summary>Writes a ListAnalyticsConfigurations result as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ListAnalyticsConfigurationsResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteListAnalyticsConfigurations(S3ListAnalyticsConfigurationsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListBucketAnalyticsConfigurationsResult");
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");
        if (!string.IsNullOrWhiteSpace(response.ContinuationToken)) {
            xmlWriter.WriteElementString("ContinuationToken", response.ContinuationToken);
        }
        if (!string.IsNullOrWhiteSpace(response.NextContinuationToken)) {
            xmlWriter.WriteElementString("NextContinuationToken", response.NextContinuationToken);
        }

        foreach (var config in response.AnalyticsConfigurations) {
            WriteAnalyticsConfigurationElement(xmlWriter, config);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ListBucketAnalyticsConfigurationsResult");
    }

    /// <summary>Writes a ListMetricsConfigurations result as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ListMetricsConfigurationsResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteListMetricsConfigurations(S3ListMetricsConfigurationsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListMetricsConfigurationsResult");
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");
        if (!string.IsNullOrWhiteSpace(response.ContinuationToken)) {
            xmlWriter.WriteElementString("ContinuationToken", response.ContinuationToken);
        }
        if (!string.IsNullOrWhiteSpace(response.NextContinuationToken)) {
            xmlWriter.WriteElementString("NextContinuationToken", response.NextContinuationToken);
        }

        foreach (var config in response.MetricsConfigurations) {
            WriteMetricsConfigurationElement(xmlWriter, config);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ListMetricsConfigurationsResult");
    }

    /// <summary>Writes a ListInventoryConfigurations result as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ListInventoryConfigurationsResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteListInventoryConfigurations(S3ListInventoryConfigurationsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListInventoryConfigurationsResult");
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");
        if (!string.IsNullOrWhiteSpace(response.ContinuationToken)) {
            xmlWriter.WriteElementString("ContinuationToken", response.ContinuationToken);
        }
        if (!string.IsNullOrWhiteSpace(response.NextContinuationToken)) {
            xmlWriter.WriteElementString("NextContinuationToken", response.NextContinuationToken);
        }

        foreach (var config in response.InventoryConfigurations) {
            WriteInventoryConfigurationElement(xmlWriter, config);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ListInventoryConfigurationsResult");
    }

    /// <summary>Writes a ListIntelligentTieringConfigurations result as an XML response body.</summary>
    /// <param name="response">The <see cref="S3ListIntelligentTieringConfigurationsResult"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteListIntelligentTieringConfigurations(S3ListIntelligentTieringConfigurationsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListBucketIntelligentTieringConfigurationsResult");
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");
        if (!string.IsNullOrWhiteSpace(response.ContinuationToken)) {
            xmlWriter.WriteElementString("ContinuationToken", response.ContinuationToken);
        }
        if (!string.IsNullOrWhiteSpace(response.NextContinuationToken)) {
            xmlWriter.WriteElementString("NextContinuationToken", response.NextContinuationToken);
        }

        foreach (var config in response.IntelligentTieringConfigurations) {
            WriteIntelligentTieringConfigurationElement(xmlWriter, config);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "ListBucketIntelligentTieringConfigurationsResult");
    }

    private static void WriteAnalyticsConfigurationElement(XmlWriter xmlWriter, S3AnalyticsConfiguration config)
    {
        xmlWriter.WriteStartElement("AnalyticsConfiguration");
        xmlWriter.WriteElementString("Id", config.Id);

        var hasPrefix = !string.IsNullOrWhiteSpace(config.FilterPrefix);
        var hasTags = config.FilterTags is { Count: > 0 };
        if (hasPrefix || hasTags) {
            xmlWriter.WriteStartElement("Filter");
            if (hasPrefix && !hasTags) {
                xmlWriter.WriteElementString("Prefix", config.FilterPrefix);
            } else if (!hasPrefix && hasTags && config.FilterTags!.Count == 1) {
                xmlWriter.WriteStartElement("Tag");
                xmlWriter.WriteElementString("Key", config.FilterTags![0].Key);
                xmlWriter.WriteElementString("Value", config.FilterTags[0].Value);
                xmlWriter.WriteEndElement();
            } else {
                xmlWriter.WriteStartElement("And");
                if (hasPrefix) {
                    xmlWriter.WriteElementString("Prefix", config.FilterPrefix);
                }
                if (hasTags) {
                    foreach (var tag in config.FilterTags!) {
                        xmlWriter.WriteStartElement("Tag");
                        xmlWriter.WriteElementString("Key", tag.Key);
                        xmlWriter.WriteElementString("Value", tag.Value);
                        xmlWriter.WriteEndElement();
                    }
                }
                xmlWriter.WriteEndElement();
            }
            xmlWriter.WriteEndElement();
        }

        if (config.StorageClassAnalysis is { } storageClassAnalysis) {
            xmlWriter.WriteStartElement("StorageClassAnalysis");
            if (storageClassAnalysis.DataExport is { } dataExport) {
                xmlWriter.WriteStartElement("DataExport");
                xmlWriter.WriteElementString("OutputSchemaVersion", dataExport.OutputSchemaVersion);
                if (dataExport.Destination is { } destination) {
                    xmlWriter.WriteStartElement("Destination");
                    xmlWriter.WriteStartElement("S3BucketDestination");
                    xmlWriter.WriteElementString("Format", destination.Format);
                    if (!string.IsNullOrWhiteSpace(destination.BucketAccountId)) {
                        xmlWriter.WriteElementString("BucketAccountId", destination.BucketAccountId);
                    }
                    xmlWriter.WriteElementString("Bucket", destination.Bucket);
                    if (!string.IsNullOrWhiteSpace(destination.Prefix)) {
                        xmlWriter.WriteElementString("Prefix", destination.Prefix);
                    }
                    xmlWriter.WriteEndElement();
                    xmlWriter.WriteEndElement();
                }
                xmlWriter.WriteEndElement();
            }
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
    }

    private static void WriteMetricsConfigurationElement(XmlWriter xmlWriter, S3MetricsConfiguration config)
    {
        xmlWriter.WriteStartElement("MetricsConfiguration");
        xmlWriter.WriteElementString("Id", config.Id);

        if (config.Filter is { } filter) {
            xmlWriter.WriteStartElement("Filter");
            var hasPrefix = !string.IsNullOrWhiteSpace(filter.Prefix);
            var hasAccessPointArn = !string.IsNullOrWhiteSpace(filter.AccessPointArn);
            var hasTags = filter.Tags.Count > 0;
            var multipleConditions = (hasPrefix ? 1 : 0) + (hasAccessPointArn ? 1 : 0) + (hasTags ? filter.Tags.Count : 0) > 1;

            if (multipleConditions) {
                xmlWriter.WriteStartElement("And");
            }

            if (hasPrefix) {
                xmlWriter.WriteElementString("Prefix", filter.Prefix);
            }
            if (hasAccessPointArn) {
                xmlWriter.WriteElementString("AccessPointArn", filter.AccessPointArn);
            }
            foreach (var tag in filter.Tags) {
                xmlWriter.WriteStartElement("Tag");
                xmlWriter.WriteElementString("Key", tag.Key);
                xmlWriter.WriteElementString("Value", tag.Value);
                xmlWriter.WriteEndElement();
            }

            if (multipleConditions) {
                xmlWriter.WriteEndElement();
            }

            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
    }

    private static void WriteInventoryConfigurationElement(XmlWriter xmlWriter, S3InventoryConfiguration config)
    {
        xmlWriter.WriteStartElement("InventoryConfiguration");
        xmlWriter.WriteElementString("Id", config.Id);
        xmlWriter.WriteElementString("IsEnabled", config.IsEnabled ? "true" : "false");
        xmlWriter.WriteElementString("IncludedObjectVersions", config.IncludedObjectVersions);

        if (config.Filter is { } filter) {
            xmlWriter.WriteStartElement("Filter");
            if (!string.IsNullOrWhiteSpace(filter.Prefix)) {
                xmlWriter.WriteElementString("Prefix", filter.Prefix);
            }
            xmlWriter.WriteEndElement();
        }

        if (config.Destination is { } dest) {
            xmlWriter.WriteStartElement("Destination");
            if (dest.S3BucketDestination is { } s3Dest) {
                xmlWriter.WriteStartElement("S3BucketDestination");
                xmlWriter.WriteElementString("Format", s3Dest.Format);
                if (!string.IsNullOrWhiteSpace(s3Dest.AccountId)) {
                    xmlWriter.WriteElementString("AccountId", s3Dest.AccountId);
                }
                xmlWriter.WriteElementString("Bucket", s3Dest.Bucket);
                if (!string.IsNullOrWhiteSpace(s3Dest.Prefix)) {
                    xmlWriter.WriteElementString("Prefix", s3Dest.Prefix);
                }
                xmlWriter.WriteEndElement();
            }
            xmlWriter.WriteEndElement();
        }

        if (config.Schedule is { } schedule) {
            xmlWriter.WriteStartElement("Schedule");
            xmlWriter.WriteElementString("Frequency", schedule.Frequency);
            xmlWriter.WriteEndElement();
        }

        if (config.OptionalFields.Count > 0) {
            xmlWriter.WriteStartElement("OptionalFields");
            foreach (var field in config.OptionalFields) {
                xmlWriter.WriteElementString("Field", field);
            }
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
    }

    private static void WriteIntelligentTieringConfigurationElement(XmlWriter xmlWriter, S3IntelligentTieringConfiguration config)
    {
        xmlWriter.WriteStartElement("IntelligentTieringConfiguration");
        xmlWriter.WriteElementString("Id", config.Id);
        xmlWriter.WriteElementString("Status", config.Status);

        if (config.Filter is { } filter) {
            xmlWriter.WriteStartElement("Filter");
            var hasPrefix = !string.IsNullOrWhiteSpace(filter.Prefix);
            var hasTags = filter.Tags.Count > 0;
            var multipleConditions = (hasPrefix ? 1 : 0) + (hasTags ? filter.Tags.Count : 0) > 1;

            if (multipleConditions) {
                xmlWriter.WriteStartElement("And");
            }

            if (hasPrefix) {
                xmlWriter.WriteElementString("Prefix", filter.Prefix);
            }
            foreach (var tag in filter.Tags) {
                xmlWriter.WriteStartElement("Tag");
                xmlWriter.WriteElementString("Key", tag.Key);
                xmlWriter.WriteElementString("Value", tag.Value);
                xmlWriter.WriteEndElement();
            }

            if (multipleConditions) {
                xmlWriter.WriteEndElement();
            }

            xmlWriter.WriteEndElement();
        }

        foreach (var tiering in config.Tierings) {
            xmlWriter.WriteStartElement("Tiering");
            xmlWriter.WriteElementString("AccessTier", tiering.AccessTier);
            xmlWriter.WriteElementString("Days", tiering.Days.ToString(CultureInfo.InvariantCulture));
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
    }

    /// <summary>Writes a RestoreRequest as an XML response body.</summary>
    /// <param name="response">The <see cref="S3RestoreRequest"/> to serialize.</param>
    /// <returns>The XML string.</returns>
    public static string WriteRestoreRequest(S3RestoreRequest response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("RestoreRequest");

        if (response.Days is { } days) {
            xmlWriter.WriteElementString("Days", days.ToString(CultureInfo.InvariantCulture));
        }

        if (!string.IsNullOrWhiteSpace(response.GlacierJobTier)) {
            xmlWriter.WriteStartElement("GlacierJobParameters");
            xmlWriter.WriteElementString("Tier", response.GlacierJobTier);
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return InjectS3Namespace(builder, "RestoreRequest");
    }

    private static XmlWriterSettings CreateSettings()
    {
        return new XmlWriterSettings
        {
            Encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            OmitXmlDeclaration = false,
            Indent = false,
            NewLineHandling = NewLineHandling.None,
            Async = false
        };
    }

    private static void WriteStartRootElement(XmlWriter xmlWriter, string localName)
    {
        xmlWriter.WriteStartElement(string.Empty, localName, CanonicalS3Namespace);
    }

    private static string FormatTimestamp(DateTimeOffset value)
    {
        return value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);
    }

    private static string QuoteETag(string value)
    {
        return string.IsNullOrWhiteSpace(value)
            ? "\"\""
            : value.StartsWith('"') ? value : $"\"{value}\"";
    }

    private const string S3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/";

    private static string InjectS3Namespace(StringBuilder builder, string rootElement)
    {
        var xml = builder.ToString();
        return xml.Replace(
            string.Concat("<", rootElement, ">"),
            string.Concat("<", rootElement, " xmlns=\"", S3Namespace, "\">"));
    }

    private static void WriteOwner(XmlWriter xmlWriter, string elementName, S3BucketOwner owner)
    {
        ArgumentNullException.ThrowIfNull(xmlWriter);
        ArgumentException.ThrowIfNullOrWhiteSpace(elementName);
        ArgumentNullException.ThrowIfNull(owner);

        xmlWriter.WriteStartElement(elementName);
        xmlWriter.WriteElementString("ID", owner.Id);

        if (!string.IsNullOrWhiteSpace(owner.DisplayName)) {
            xmlWriter.WriteElementString("DisplayName", owner.DisplayName);
        }

        xmlWriter.WriteEndElement();
    }

    private static string EncodeS3ListValue(string value, string? encodingType)
    {
        ArgumentNullException.ThrowIfNull(value);

        return string.Equals(encodingType, "url", StringComparison.Ordinal)
            ? Uri.EscapeDataString(value)
            : value;
    }

    private static void WriteNotificationFilter(XmlWriter xmlWriter, S3NotificationFilterRuleSet? filter)
    {
        if (filter is null || filter.S3KeyRules.Count == 0) {
            return;
        }

        xmlWriter.WriteStartElement("Filter");
        xmlWriter.WriteStartElement("S3Key");
        foreach (var rule in filter.S3KeyRules) {
            xmlWriter.WriteStartElement("FilterRule");
            xmlWriter.WriteElementString("Name", rule.Name);
            xmlWriter.WriteElementString("Value", rule.Value);
            xmlWriter.WriteEndElement();
        }
        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndElement();
    }

    /// <summary>Writes a PostObjectResult as an XML response body.</summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="key">The object key.</param>
    /// <param name="etag">The optional ETag of the created object.</param>
    /// <returns>The XML string.</returns>
    public static string WritePostObjectResult(string bucketName, string key, string? etag)
    {
        var builder = new StringBuilder();
        using var stringWriter = new Utf8StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("PostResponse");
        xmlWriter.WriteElementString("Bucket", bucketName);
        xmlWriter.WriteElementString("Key", key);
        if (!string.IsNullOrWhiteSpace(etag)) {
            xmlWriter.WriteElementString("ETag", QuoteETag(etag));
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return builder.ToString();
    }

    /// <summary>StringWriter that reports UTF-8 encoding so XmlWriter emits the correct declaration.</summary>
    private sealed class Utf8StringWriter(StringBuilder sb, IFormatProvider formatProvider)
        : StringWriter(sb, formatProvider)
    {
        private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);
        public override Encoding Encoding => Utf8NoBom;
    }
}

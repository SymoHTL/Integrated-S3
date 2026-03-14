using System.Globalization;
using System.Text;
using System.Xml;

namespace IntegratedS3.Protocol;

public static class S3XmlResponseWriter
{
    public static string WriteBucketLocation(S3BucketLocationResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("LocationConstraint");

        if (!string.IsNullOrWhiteSpace(response.LocationConstraint)) {
            xmlWriter.WriteString(response.LocationConstraint);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return builder.ToString();
    }

    public static string WriteBucketVersioningConfiguration(S3BucketVersioningConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("VersioningConfiguration");

        if (!string.IsNullOrWhiteSpace(response.Status)) {
            xmlWriter.WriteElementString("Status", response.Status);
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return builder.ToString();
    }

    public static string WriteCorsConfiguration(S3CorsConfiguration response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("CORSConfiguration");

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

        return builder.ToString();
    }

    public static string WriteError(S3ErrorResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("Error");
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

        return builder.ToString();
    }

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
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement(rootElementName);
        xmlWriter.WriteElementString("LastModified", FormatTimestamp(response.LastModifiedUtc));
        xmlWriter.WriteElementString("ETag", QuoteETag(response.ETag));

        if (!string.IsNullOrWhiteSpace(response.ChecksumCrc32)) {
            xmlWriter.WriteElementString("ChecksumCRC32", response.ChecksumCrc32);
        }

        if (!string.IsNullOrWhiteSpace(response.ChecksumCrc32c)) {
            xmlWriter.WriteElementString("ChecksumCRC32C", response.ChecksumCrc32c);
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

        return builder.ToString();
    }

    public static string WriteInitiateMultipartUploadResult(S3InitiateMultipartUploadResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("InitiateMultipartUploadResult");
        xmlWriter.WriteElementString("Bucket", response.Bucket);
        xmlWriter.WriteElementString("Key", response.Key);
        xmlWriter.WriteElementString("UploadId", response.UploadId);
        if (!string.IsNullOrWhiteSpace(response.ChecksumAlgorithm)) {
            xmlWriter.WriteElementString("ChecksumAlgorithm", response.ChecksumAlgorithm);
        }
        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return builder.ToString();
    }

    public static string WriteCompleteMultipartUploadResult(S3CompleteMultipartUploadResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("CompleteMultipartUploadResult");

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

        return builder.ToString();
    }

    public static string WriteListBucketResult(S3ListBucketResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListBucketResult");
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

        return builder.ToString();
    }

    public static string WriteListObjectVersionsResult(S3ListObjectVersionsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListVersionsResult");
        xmlWriter.WriteElementString("Name", response.Name);
        xmlWriter.WriteElementString("Prefix", response.Prefix ?? string.Empty);

        if (!string.IsNullOrWhiteSpace(response.Delimiter)) {
            xmlWriter.WriteElementString("Delimiter", response.Delimiter);
        }

        if (!string.IsNullOrWhiteSpace(response.KeyMarker)) {
            xmlWriter.WriteElementString("KeyMarker", response.KeyMarker);
        }

        if (!string.IsNullOrWhiteSpace(response.VersionIdMarker)) {
            xmlWriter.WriteElementString("VersionIdMarker", response.VersionIdMarker);
        }

        if (!string.IsNullOrWhiteSpace(response.NextKeyMarker)) {
            xmlWriter.WriteElementString("NextKeyMarker", response.NextKeyMarker);
        }

        if (!string.IsNullOrWhiteSpace(response.NextVersionIdMarker)) {
            xmlWriter.WriteElementString("NextVersionIdMarker", response.NextVersionIdMarker);
        }

        xmlWriter.WriteElementString("MaxKeys", response.MaxKeys.ToString(CultureInfo.InvariantCulture));
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");

        foreach (var version in response.Versions) {
            xmlWriter.WriteStartElement(version.IsDeleteMarker ? "DeleteMarker" : "Version");
            xmlWriter.WriteElementString("Key", version.Key);
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
            xmlWriter.WriteElementString("Prefix", commonPrefix.Prefix);
            xmlWriter.WriteEndElement();
        }

        xmlWriter.WriteEndElement();
        xmlWriter.WriteEndDocument();
        xmlWriter.Flush();

        return builder.ToString();
    }

    public static string WriteListMultipartUploadsResult(S3ListMultipartUploadsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListMultipartUploadsResult");
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

        return builder.ToString();
    }

    public static string WriteListPartsResult(S3ListPartsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListPartsResult");
        xmlWriter.WriteElementString("Bucket", response.Bucket);
        xmlWriter.WriteElementString("Key", response.Key);
        xmlWriter.WriteElementString("UploadId", response.UploadId);
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

        return builder.ToString();
    }
    public static string WriteListAllMyBucketsResult(S3ListAllMyBucketsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListAllMyBucketsResult");

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

        return builder.ToString();
    }

    public static string WriteDeleteObjectsResult(S3DeleteObjectsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("DeleteResult");

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

        return builder.ToString();
    }

    public static string WriteObjectTagging(S3ObjectTagging response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("Tagging");
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

        return builder.ToString();
    }

    public static string WriteAccessControlPolicy(S3AccessControlPolicy response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("AccessControlPolicy");
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

        return builder.ToString();
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
}

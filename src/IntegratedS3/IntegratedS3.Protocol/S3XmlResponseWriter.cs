using System.Globalization;
using System.Text;
using System.Xml;

namespace IntegratedS3.Protocol;

public static class S3XmlResponseWriter
{
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

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("CopyObjectResult");
        xmlWriter.WriteElementString("LastModified", FormatTimestamp(response.LastModifiedUtc));
        xmlWriter.WriteElementString("ETag", QuoteETag(response.ETag));
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
        xmlWriter.WriteElementString("Prefix", response.Prefix ?? string.Empty);

        if (!string.IsNullOrWhiteSpace(response.Delimiter)) {
            xmlWriter.WriteElementString("Delimiter", response.Delimiter);
        }

        if (!string.IsNullOrWhiteSpace(response.StartAfter)) {
            xmlWriter.WriteElementString("StartAfter", response.StartAfter);
        }

        if (!string.IsNullOrWhiteSpace(response.ContinuationToken)) {
            xmlWriter.WriteElementString("ContinuationToken", response.ContinuationToken);
        }

        if (!string.IsNullOrWhiteSpace(response.NextContinuationToken)) {
            xmlWriter.WriteElementString("NextContinuationToken", response.NextContinuationToken);
        }

        xmlWriter.WriteElementString("KeyCount", response.KeyCount.ToString(CultureInfo.InvariantCulture));
        xmlWriter.WriteElementString("MaxKeys", response.MaxKeys.ToString(CultureInfo.InvariantCulture));
        xmlWriter.WriteElementString("IsTruncated", response.IsTruncated ? "true" : "false");

        foreach (var content in response.Contents) {
            xmlWriter.WriteStartElement("Contents");
            xmlWriter.WriteElementString("Key", content.Key);
            xmlWriter.WriteElementString("LastModified", FormatTimestamp(content.LastModifiedUtc));
            xmlWriter.WriteElementString("ETag", QuoteETag(content.ETag ?? string.Empty));
            xmlWriter.WriteElementString("Size", content.Size.ToString(CultureInfo.InvariantCulture));
            xmlWriter.WriteElementString("StorageClass", content.StorageClass);
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

    public static string WriteListAllMyBucketsResult(S3ListAllMyBucketsResult response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var builder = new StringBuilder();
        using var stringWriter = new StringWriter(builder, CultureInfo.InvariantCulture);
        using var xmlWriter = XmlWriter.Create(stringWriter, CreateSettings());

        xmlWriter.WriteStartDocument();
        xmlWriter.WriteStartElement("ListAllMyBucketsResult");

        xmlWriter.WriteStartElement("Owner");
        xmlWriter.WriteElementString("ID", response.Owner.Id);
        xmlWriter.WriteElementString("DisplayName", response.Owner.DisplayName);
        xmlWriter.WriteEndElement();

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
}
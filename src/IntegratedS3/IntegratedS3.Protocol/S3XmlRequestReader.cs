using System.Xml.Linq;

namespace IntegratedS3.Protocol;

public static class S3XmlRequestReader
{
    public static async Task<S3BucketVersioningConfiguration> ReadBucketVersioningConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The bucket versioning request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "VersioningConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The bucket versioning request body must contain a root 'VersioningConfiguration' element.");
            }

            return new S3BucketVersioningConfiguration
            {
                Status = root.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Status", StringComparison.Ordinal))?.Value
            };
        }
        catch (FormatException) {
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            throw new FormatException("The bucket versioning request body is not valid XML.", exception);
        }
    }

    public static async Task<S3CorsConfiguration> ReadCorsConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The bucket CORS request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "CORSConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The bucket CORS request body must contain a root 'CORSConfiguration' element.");
            }

            var rules = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "CORSRule", StringComparison.Ordinal))
                .Select(ParseCorsRule)
                .ToArray();

            if (rules.Length == 0) {
                throw new FormatException("The bucket CORS request body must contain at least one 'CORSRule' element.");
            }

            return new S3CorsConfiguration
            {
                Rules = rules
            };
        }
        catch (FormatException) {
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            throw new FormatException("The bucket CORS request body is not valid XML.", exception);
        }
    }

    public static async Task<S3CompleteMultipartUploadRequest> ReadCompleteMultipartUploadRequestAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The complete multipart upload request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "CompleteMultipartUpload", StringComparison.Ordinal)) {
                throw new FormatException("The complete multipart upload request body must contain a root 'CompleteMultipartUpload' element.");
            }

            var parts = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "Part", StringComparison.Ordinal))
                .Select(ParseCompleteMultipartUploadPart)
                .ToArray();

            if (parts.Length == 0) {
                throw new FormatException("The complete multipart upload request body must contain at least one 'Part' element.");
            }

            return new S3CompleteMultipartUploadRequest
            {
                Parts = parts
            };
        }
        catch (FormatException) {
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            throw new FormatException("The complete multipart upload request body is not valid XML.", exception);
        }
    }

    public static async Task<S3DeleteObjectsRequest> ReadDeleteObjectsRequestAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The delete request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "Delete", StringComparison.Ordinal)) {
                throw new FormatException("The delete request body must contain a root 'Delete' element.");
            }

            var objects = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "Object", StringComparison.Ordinal))
                .Select(static element => new S3DeleteObjectIdentifier
                {
                    Key = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Key", StringComparison.Ordinal))?.Value
                        ?? throw new FormatException("Each delete object entry must contain a 'Key' element."),
                    VersionId = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "VersionId", StringComparison.Ordinal))?.Value
                })
                .ToArray();

            if (objects.Length == 0) {
                throw new FormatException("The delete request body must contain at least one 'Object' element.");
            }

            var quietText = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Quiet", StringComparison.Ordinal))?.Value;
            var quiet = !string.IsNullOrWhiteSpace(quietText) && bool.TryParse(quietText, out var parsedQuiet) && parsedQuiet;

            return new S3DeleteObjectsRequest
            {
                Quiet = quiet,
                Objects = objects
            };
        }
        catch (FormatException) {
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            throw new FormatException("The delete request body is not valid XML.", exception);
        }
    }

    public static async Task<S3ObjectTagging> ReadObjectTaggingAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The tagging request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "Tagging", StringComparison.Ordinal)) {
                throw new FormatException("The tagging request body must contain a root 'Tagging' element.");
            }

            var tagSet = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "TagSet", StringComparison.Ordinal))
                ?? throw new FormatException("The tagging request body must contain a 'TagSet' element.");

            var tags = tagSet.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "Tag", StringComparison.Ordinal))
                .Select(static element => new S3ObjectTag
                {
                    Key = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Key", StringComparison.Ordinal))?.Value
                        ?? throw new FormatException("Each tag entry must contain a 'Key' element."),
                    Value = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Value", StringComparison.Ordinal))?.Value
                        ?? throw new FormatException("Each tag entry must contain a 'Value' element.")
                })
                .ToArray();

            return new S3ObjectTagging
            {
                TagSet = tags
            };
        }
        catch (FormatException) {
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            throw new FormatException("The tagging request body is not valid XML.", exception);
        }
    }

    private static S3CompleteMultipartUploadPart ParseCompleteMultipartUploadPart(XElement element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var checksumSha256 = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ChecksumSHA256", StringComparison.Ordinal))?.Value;
        if (!string.IsNullOrWhiteSpace(checksumSha256)) {
            checksums["sha256"] = checksumSha256;
        }

        var checksumCrc32 = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ChecksumCRC32", StringComparison.Ordinal))?.Value;
        if (!string.IsNullOrWhiteSpace(checksumCrc32)) {
            checksums["crc32"] = checksumCrc32;
        }

        var checksumCrc32c = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ChecksumCRC32C", StringComparison.Ordinal))?.Value;
        if (!string.IsNullOrWhiteSpace(checksumCrc32c)) {
            checksums["crc32c"] = checksumCrc32c;
        }

        var checksumSha1 = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ChecksumSHA1", StringComparison.Ordinal))?.Value;
        if (!string.IsNullOrWhiteSpace(checksumSha1)) {
            checksums["sha1"] = checksumSha1;
        }

        return new S3CompleteMultipartUploadPart
        {
            PartNumber = int.TryParse(
                element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "PartNumber", StringComparison.Ordinal))?.Value,
                out var parsedPartNumber)
                ? parsedPartNumber
                : throw new FormatException("Each multipart part entry must contain a valid 'PartNumber' element."),
            ETag = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ETag", StringComparison.Ordinal))?.Value
                ?? throw new FormatException("Each multipart part entry must contain an 'ETag' element."),
            Checksums = checksums.Count == 0 ? null : checksums
        };
    }

    private static S3CorsRule ParseCorsRule(XElement element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var allowedOrigins = ReadRequiredCorsValues(element, "AllowedOrigin", "Each CORS rule must contain at least one 'AllowedOrigin' element.");
        var allowedMethods = ReadRequiredCorsValues(element, "AllowedMethod", "Each CORS rule must contain at least one 'AllowedMethod' element.");
        var allowedHeaders = ReadOptionalCorsValues(element, "AllowedHeader");
        var exposeHeaders = ReadOptionalCorsValues(element, "ExposeHeader");
        var id = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ID", StringComparison.Ordinal))?.Value?.Trim();

        int? maxAgeSeconds = null;
        var maxAgeValue = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "MaxAgeSeconds", StringComparison.Ordinal))?.Value;
        if (!string.IsNullOrWhiteSpace(maxAgeValue)) {
            if (!int.TryParse(maxAgeValue, out var parsedMaxAgeSeconds) || parsedMaxAgeSeconds < 0) {
                throw new FormatException("Each CORS rule 'MaxAgeSeconds' value must be a non-negative integer.");
            }

            maxAgeSeconds = parsedMaxAgeSeconds;
        }

        return new S3CorsRule
        {
            Id = string.IsNullOrWhiteSpace(id) ? null : id,
            AllowedOrigins = allowedOrigins,
            AllowedMethods = allowedMethods,
            AllowedHeaders = allowedHeaders,
            ExposeHeaders = exposeHeaders,
            MaxAgeSeconds = maxAgeSeconds
        };
    }

    private static string[] ReadRequiredCorsValues(XElement element, string elementName, string errorMessage)
    {
        var values = ReadOptionalCorsValues(element, elementName);
        if (values.Length == 0) {
            throw new FormatException(errorMessage);
        }

        return values;
    }

    private static string[] ReadOptionalCorsValues(XElement element, string elementName)
    {
        return element.Elements()
            .Where(child => string.Equals(child.Name.LocalName, elementName, StringComparison.Ordinal))
            .Select(static child => child.Value.Trim())
            .Where(static value => !string.IsNullOrWhiteSpace(value))
            .ToArray();
    }
}

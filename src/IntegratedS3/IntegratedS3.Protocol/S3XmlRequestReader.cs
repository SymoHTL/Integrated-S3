using System.Xml.Linq;

namespace IntegratedS3.Protocol;

public static class S3XmlRequestReader
{
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
                .Select(static element => new S3CompleteMultipartUploadPart
                {
                    PartNumber = int.TryParse(
                        element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "PartNumber", StringComparison.Ordinal))?.Value,
                        out var parsedPartNumber)
                        ? parsedPartNumber
                        : throw new FormatException("Each multipart part entry must contain a valid 'PartNumber' element."),
                    ETag = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ETag", StringComparison.Ordinal))?.Value
                        ?? throw new FormatException("Each multipart part entry must contain an 'ETag' element.")
                })
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
}

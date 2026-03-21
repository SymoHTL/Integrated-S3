using System.Xml.Linq;
using IntegratedS3.Protocol.Internal;

namespace IntegratedS3.Protocol;

/// <summary>Reads S3 XML request bodies into typed DTO instances.</summary>
public static class S3XmlRequestReader
{
    /// <summary>Reads a bucket versioning configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3BucketVersioningConfiguration"/>.</returns>
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
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadBucketVersioningConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadBucketVersioningConfiguration", exception.Message);
            throw new FormatException("The bucket versioning request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a bucket encryption configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3BucketEncryptionConfiguration"/>.</returns>
    public static async Task<S3BucketEncryptionConfiguration> ReadBucketEncryptionConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The bucket encryption request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "ServerSideEncryptionConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The bucket encryption request body must contain a root 'ServerSideEncryptionConfiguration' element.");
            }

            var rules = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "Rule", StringComparison.Ordinal))
                .Select(ParseBucketEncryptionRule)
                .ToArray();

            if (rules.Length == 0) {
                throw new FormatException("The bucket encryption request body must contain at least one 'Rule' element.");
            }

            return new S3BucketEncryptionConfiguration
            {
                Rules = rules
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadBucketEncryptionConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadBucketEncryptionConfiguration", exception.Message);
            throw new FormatException("The bucket encryption request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a CORS configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3CorsConfiguration"/>.</returns>
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
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadCorsConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadCorsConfiguration", exception.Message);
            throw new FormatException("The bucket CORS request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a complete multipart upload request from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3CompleteMultipartUploadRequest"/>.</returns>
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
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadCompleteMultipartUploadRequest", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadCompleteMultipartUploadRequest", exception.Message);
            throw new FormatException("The complete multipart upload request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a delete objects request from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3DeleteObjectsRequest"/>.</returns>
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
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadDeleteObjectsRequest", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadDeleteObjectsRequest", exception.Message);
            throw new FormatException("The delete request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads an object tagging configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3ObjectTagging"/>.</returns>
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
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadObjectTagging", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadObjectTagging", exception.Message);
            throw new FormatException("The tagging request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads an access control policy from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3AccessControlPolicy"/>.</returns>
    public static async Task<S3AccessControlPolicy> ReadAccessControlPolicyAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The ACL request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "AccessControlPolicy", StringComparison.Ordinal)) {
                throw new FormatException("The ACL request body must contain a root 'AccessControlPolicy' element.");
            }

            var ownerElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Owner", StringComparison.Ordinal));
            var owner = ownerElement is null
                ? new S3BucketOwner()
                : new S3BucketOwner
                {
                    Id = ownerElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ID", StringComparison.Ordinal))?.Value ?? string.Empty,
                    DisplayName = ownerElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "DisplayName", StringComparison.Ordinal))?.Value ?? string.Empty
                };

            var accessControlList = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "AccessControlList", StringComparison.Ordinal))
                ?? throw new FormatException("The ACL request body must contain an 'AccessControlList' element.");

            var grants = accessControlList.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "Grant", StringComparison.Ordinal))
                .Select(ParseAccessControlGrant)
                .ToArray();

            if (grants.Length == 0) {
                throw new FormatException("The ACL request body must contain at least one 'Grant' element.");
            }

            return new S3AccessControlPolicy
            {
                Owner = owner,
                Grants = grants
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadAccessControlPolicy", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadAccessControlPolicy", exception.Message);
            throw new FormatException("The ACL request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a bucket tagging configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3BucketTagging"/>.</returns>
    public static async Task<S3BucketTagging> ReadBucketTaggingAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The bucket tagging request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "Tagging", StringComparison.Ordinal)) {
                throw new FormatException("The bucket tagging request body must contain a root 'Tagging' element.");
            }

            var tagSet = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "TagSet", StringComparison.Ordinal));
            var tags = tagSet?.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "Tag", StringComparison.Ordinal))
                .Select(static element => new S3Tag
                {
                    Key = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Key", StringComparison.Ordinal))?.Value ?? string.Empty,
                    Value = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Value", StringComparison.Ordinal))?.Value ?? string.Empty
                })
                .ToArray() ?? [];

            return new S3BucketTagging
            {
                TagSet = tags
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadBucketTagging", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadBucketTagging", exception.Message);
            throw new FormatException("The bucket tagging request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a bucket logging status from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3BucketLoggingStatus"/>.</returns>
    public static async Task<S3BucketLoggingStatus> ReadBucketLoggingStatusAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The bucket logging request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "BucketLoggingStatus", StringComparison.Ordinal)) {
                throw new FormatException("The bucket logging request body must contain a root 'BucketLoggingStatus' element.");
            }

            var loggingEnabledElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "LoggingEnabled", StringComparison.Ordinal));
            S3LoggingEnabled? loggingEnabled = null;
            if (loggingEnabledElement is not null) {
                loggingEnabled = new S3LoggingEnabled
                {
                    TargetBucket = loggingEnabledElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "TargetBucket", StringComparison.Ordinal))?.Value ?? string.Empty,
                    TargetPrefix = loggingEnabledElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "TargetPrefix", StringComparison.Ordinal))?.Value ?? string.Empty
                };
            }

            return new S3BucketLoggingStatus
            {
                LoggingEnabled = loggingEnabled
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadBucketLoggingStatus", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadBucketLoggingStatus", exception.Message);
            throw new FormatException("The bucket logging request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a website configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3WebsiteConfiguration"/>.</returns>
    public static async Task<S3WebsiteConfiguration> ReadWebsiteConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The website configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "WebsiteConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The website configuration request body must contain a root 'WebsiteConfiguration' element.");
            }

            var indexDocumentElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "IndexDocument", StringComparison.Ordinal));
            S3WebsiteIndexDocument? indexDocument = null;
            if (indexDocumentElement is not null) {
                indexDocument = new S3WebsiteIndexDocument
                {
                    Suffix = indexDocumentElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Suffix", StringComparison.Ordinal))?.Value ?? string.Empty
                };
            }

            var errorDocumentElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "ErrorDocument", StringComparison.Ordinal));
            S3WebsiteErrorDocument? errorDocument = null;
            if (errorDocumentElement is not null) {
                errorDocument = new S3WebsiteErrorDocument
                {
                    Key = errorDocumentElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Key", StringComparison.Ordinal))?.Value ?? string.Empty
                };
            }

            var redirectElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "RedirectAllRequestsTo", StringComparison.Ordinal));
            S3WebsiteRedirectAllRequestsTo? redirectAllRequestsTo = null;
            if (redirectElement is not null) {
                redirectAllRequestsTo = new S3WebsiteRedirectAllRequestsTo
                {
                    HostName = redirectElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "HostName", StringComparison.Ordinal))?.Value ?? string.Empty,
                    Protocol = redirectElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Protocol", StringComparison.Ordinal))?.Value
                };
            }

            var routingRulesElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "RoutingRules", StringComparison.Ordinal));
            var routingRules = routingRulesElement?.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "RoutingRule", StringComparison.Ordinal))
                .Select(static element =>
                {
                    var conditionElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Condition", StringComparison.Ordinal));
                    S3WebsiteRoutingRuleCondition? condition = null;
                    if (conditionElement is not null) {
                        var httpErrorCodeText = conditionElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "HttpErrorCodeReturnedEquals", StringComparison.Ordinal))?.Value;
                        condition = new S3WebsiteRoutingRuleCondition
                        {
                            KeyPrefixEquals = conditionElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "KeyPrefixEquals", StringComparison.Ordinal))?.Value,
                            HttpErrorCodeReturnedEquals = !string.IsNullOrWhiteSpace(httpErrorCodeText) && int.TryParse(httpErrorCodeText, out var parsedHttpErrorCode) ? parsedHttpErrorCode : null
                        };
                    }

                    var redirectEl = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Redirect", StringComparison.Ordinal))
                        ?? throw new FormatException("Each routing rule must contain a 'Redirect' element.");

                    var httpRedirectCodeText = redirectEl.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "HttpRedirectCode", StringComparison.Ordinal))?.Value;

                    return new S3WebsiteRoutingRule
                    {
                        Condition = condition,
                        Redirect = new S3WebsiteRoutingRuleRedirect
                        {
                            HostName = redirectEl.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "HostName", StringComparison.Ordinal))?.Value,
                            Protocol = redirectEl.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Protocol", StringComparison.Ordinal))?.Value,
                            ReplaceKeyPrefixWith = redirectEl.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ReplaceKeyPrefixWith", StringComparison.Ordinal))?.Value,
                            ReplaceKeyWith = redirectEl.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ReplaceKeyWith", StringComparison.Ordinal))?.Value,
                            HttpRedirectCode = !string.IsNullOrWhiteSpace(httpRedirectCodeText) && int.TryParse(httpRedirectCodeText, out var parsedRedirectCode) ? parsedRedirectCode : null
                        }
                    };
                })
                .ToArray() ?? [];

            return new S3WebsiteConfiguration
            {
                IndexDocument = indexDocument,
                ErrorDocument = errorDocument,
                RedirectAllRequestsTo = redirectAllRequestsTo,
                RoutingRules = routingRules
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadWebsiteConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadWebsiteConfiguration", exception.Message);
            throw new FormatException("The website configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a request payment configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3RequestPaymentConfiguration"/>.</returns>
    public static async Task<S3RequestPaymentConfiguration> ReadRequestPaymentConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The request payment configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "RequestPaymentConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The request payment configuration request body must contain a root 'RequestPaymentConfiguration' element.");
            }

            return new S3RequestPaymentConfiguration
            {
                Payer = root.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Payer", StringComparison.Ordinal))?.Value ?? "BucketOwner"
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadRequestPaymentConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadRequestPaymentConfiguration", exception.Message);
            throw new FormatException("The request payment configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads an accelerate configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3AccelerateConfiguration"/>.</returns>
    public static async Task<S3AccelerateConfiguration> ReadAccelerateConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The accelerate configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "AccelerateConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The accelerate configuration request body must contain a root 'AccelerateConfiguration' element.");
            }

            return new S3AccelerateConfiguration
            {
                Status = root.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Status", StringComparison.Ordinal))?.Value
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadAccelerateConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadAccelerateConfiguration", exception.Message);
            throw new FormatException("The accelerate configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a lifecycle configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3LifecycleConfiguration"/>.</returns>
    public static async Task<S3LifecycleConfiguration> ReadLifecycleConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The lifecycle configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "LifecycleConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The lifecycle configuration request body must contain a root 'LifecycleConfiguration' element.");
            }

            var rules = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "Rule", StringComparison.Ordinal))
                .Select(ParseLifecycleRule)
                .ToArray();

            return new S3LifecycleConfiguration
            {
                Rules = rules
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadLifecycleConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadLifecycleConfiguration", exception.Message);
            throw new FormatException("The lifecycle configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a replication configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3ReplicationConfiguration"/>.</returns>
    public static async Task<S3ReplicationConfiguration> ReadReplicationConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The replication configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "ReplicationConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The replication configuration request body must contain a root 'ReplicationConfiguration' element.");
            }

            var role = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Role", StringComparison.Ordinal))?.Value;

            var rules = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "Rule", StringComparison.Ordinal))
                .Select(ParseReplicationRule)
                .ToArray();

            return new S3ReplicationConfiguration
            {
                Role = role,
                Rules = rules
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadReplicationConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadReplicationConfiguration", exception.Message);
            throw new FormatException("The replication configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a notification configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3NotificationConfiguration"/>.</returns>
    public static async Task<S3NotificationConfiguration> ReadNotificationConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The notification configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "NotificationConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The notification configuration request body must contain a root 'NotificationConfiguration' element.");
            }

            var topicConfigurations = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "TopicConfiguration", StringComparison.Ordinal))
                .Select(static element => new S3TopicConfiguration
                {
                    Id = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Id", StringComparison.Ordinal))?.Value,
                    Topic = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Topic", StringComparison.Ordinal))?.Value ?? string.Empty,
                    Events = element.Elements().Where(static child => string.Equals(child.Name.LocalName, "Event", StringComparison.Ordinal)).Select(static child => child.Value).ToArray(),
                    Filter = ParseNotificationFilter(element)
                })
                .ToArray();

            var queueConfigurations = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "QueueConfiguration", StringComparison.Ordinal))
                .Select(static element => new S3QueueConfiguration
                {
                    Id = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Id", StringComparison.Ordinal))?.Value,
                    Queue = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Queue", StringComparison.Ordinal))?.Value ?? string.Empty,
                    Events = element.Elements().Where(static child => string.Equals(child.Name.LocalName, "Event", StringComparison.Ordinal)).Select(static child => child.Value).ToArray(),
                    Filter = ParseNotificationFilter(element)
                })
                .ToArray();

            var cloudFunctionConfigurations = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "CloudFunctionConfiguration", StringComparison.Ordinal))
                .Select(static element => new S3CloudFunctionConfiguration
                {
                    Id = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Id", StringComparison.Ordinal))?.Value,
                    CloudFunction = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "CloudFunction", StringComparison.Ordinal))?.Value ?? string.Empty,
                    Events = element.Elements().Where(static child => string.Equals(child.Name.LocalName, "Event", StringComparison.Ordinal)).Select(static child => child.Value).ToArray(),
                    Filter = ParseNotificationFilter(element)
                })
                .ToArray();

            return new S3NotificationConfiguration
            {
                TopicConfigurations = topicConfigurations,
                QueueConfigurations = queueConfigurations,
                CloudFunctionConfigurations = cloudFunctionConfigurations
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadNotificationConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadNotificationConfiguration", exception.Message);
            throw new FormatException("The notification configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads an object lock configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3ObjectLockConfiguration"/>.</returns>
    public static async Task<S3ObjectLockConfiguration> ReadObjectLockConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The object lock configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "ObjectLockConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The object lock configuration request body must contain a root 'ObjectLockConfiguration' element.");
            }

            var objectLockEnabled = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "ObjectLockEnabled", StringComparison.Ordinal))?.Value;

            var ruleElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Rule", StringComparison.Ordinal));
            S3ObjectLockDefaultRetention? defaultRetention = null;
            if (ruleElement is not null) {
                var retentionElement = ruleElement.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "DefaultRetention", StringComparison.Ordinal));
                if (retentionElement is not null) {
                    var daysText = retentionElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Days", StringComparison.Ordinal))?.Value;
                    var yearsText = retentionElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Years", StringComparison.Ordinal))?.Value;
                    defaultRetention = new S3ObjectLockDefaultRetention
                    {
                        Mode = retentionElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Mode", StringComparison.Ordinal))?.Value,
                        Days = !string.IsNullOrWhiteSpace(daysText) && int.TryParse(daysText, out var parsedDays) ? parsedDays : null,
                        Years = !string.IsNullOrWhiteSpace(yearsText) && int.TryParse(yearsText, out var parsedYears) ? parsedYears : null
                    };
                }
            }

            return new S3ObjectLockConfiguration
            {
                ObjectLockEnabled = objectLockEnabled,
                DefaultRetention = defaultRetention
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadObjectLockConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadObjectLockConfiguration", exception.Message);
            throw new FormatException("The object lock configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads an analytics configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3AnalyticsConfiguration"/>.</returns>
    public static async Task<S3AnalyticsConfiguration> ReadAnalyticsConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The analytics configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "AnalyticsConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The analytics configuration request body must contain a root 'AnalyticsConfiguration' element.");
            }

            var id = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Id", StringComparison.Ordinal))?.Value ?? string.Empty;

            string? filterPrefix = null;
            IReadOnlyList<S3AnalyticsFilterTag>? filterTags = null;
            var filterElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Filter", StringComparison.Ordinal));
            if (filterElement is not null) {
                var andElement = filterElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "And", StringComparison.Ordinal));
                var filterSource = andElement ?? filterElement;
                filterPrefix = filterSource.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Prefix", StringComparison.Ordinal))?.Value;
                filterTags = filterSource.Elements()
                    .Where(static child => string.Equals(child.Name.LocalName, "Tag", StringComparison.Ordinal))
                    .Select(static child => new S3AnalyticsFilterTag
                    {
                        Key = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Key", StringComparison.Ordinal))?.Value ?? string.Empty,
                        Value = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Value", StringComparison.Ordinal))?.Value ?? string.Empty
                    })
                    .ToArray();
                if (filterTags.Count == 0) {
                    filterTags = null;
                }
            }

            S3StorageClassAnalysis? storageClassAnalysis = null;
            var scaElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "StorageClassAnalysis", StringComparison.Ordinal));
            if (scaElement is not null) {
                S3StorageClassAnalysisDataExport? dataExport = null;
                var dataExportElement = scaElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "DataExport", StringComparison.Ordinal));
                if (dataExportElement is not null) {
                    S3AnalyticsS3BucketDestination? bucketDestination = null;
                    var destElement = dataExportElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Destination", StringComparison.Ordinal));
                    if (destElement is not null) {
                        var s3BucketDestElement = destElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "S3BucketDestination", StringComparison.Ordinal));
                        if (s3BucketDestElement is not null) {
                            bucketDestination = new S3AnalyticsS3BucketDestination
                            {
                                Format = s3BucketDestElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Format", StringComparison.Ordinal))?.Value ?? "CSV",
                                BucketAccountId = s3BucketDestElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "BucketAccountId", StringComparison.Ordinal))?.Value,
                                Bucket = s3BucketDestElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Bucket", StringComparison.Ordinal))?.Value ?? string.Empty,
                                Prefix = s3BucketDestElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Prefix", StringComparison.Ordinal))?.Value
                            };
                        }
                    }

                    dataExport = new S3StorageClassAnalysisDataExport
                    {
                        OutputSchemaVersion = dataExportElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "OutputSchemaVersion", StringComparison.Ordinal))?.Value ?? "V_1",
                        Destination = bucketDestination
                    };
                }

                storageClassAnalysis = new S3StorageClassAnalysis
                {
                    DataExport = dataExport
                };
            }

            return new S3AnalyticsConfiguration
            {
                Id = id,
                FilterPrefix = filterPrefix,
                FilterTags = filterTags,
                StorageClassAnalysis = storageClassAnalysis
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadAnalyticsConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadAnalyticsConfiguration", exception.Message);
            throw new FormatException("The analytics configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a metrics configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3MetricsConfiguration"/>.</returns>
    public static async Task<S3MetricsConfiguration> ReadMetricsConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The metrics configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "MetricsConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The metrics configuration request body must contain a root 'MetricsConfiguration' element.");
            }

            var id = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Id", StringComparison.Ordinal))?.Value ?? string.Empty;

            S3MetricsFilter? filter = null;
            var filterElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Filter", StringComparison.Ordinal));
            if (filterElement is not null) {
                var andElement = filterElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "And", StringComparison.Ordinal));
                var filterSource = andElement ?? filterElement;
                filter = new S3MetricsFilter
                {
                    Prefix = filterSource.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Prefix", StringComparison.Ordinal))?.Value,
                    AccessPointArn = filterSource.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "AccessPointArn", StringComparison.Ordinal))?.Value,
                    Tags = filterSource.Elements()
                        .Where(static child => string.Equals(child.Name.LocalName, "Tag", StringComparison.Ordinal))
                        .Select(static child => new S3Tag
                        {
                            Key = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Key", StringComparison.Ordinal))?.Value ?? string.Empty,
                            Value = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Value", StringComparison.Ordinal))?.Value ?? string.Empty
                        })
                        .ToArray()
                };
            }

            return new S3MetricsConfiguration
            {
                Id = id,
                Filter = filter
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadMetricsConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadMetricsConfiguration", exception.Message);
            throw new FormatException("The metrics configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads an inventory configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3InventoryConfiguration"/>.</returns>
    public static async Task<S3InventoryConfiguration> ReadInventoryConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The inventory configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "InventoryConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The inventory configuration request body must contain a root 'InventoryConfiguration' element.");
            }

            var id = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Id", StringComparison.Ordinal))?.Value ?? string.Empty;
            var isEnabledText = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "IsEnabled", StringComparison.Ordinal))?.Value;
            var isEnabled = !string.IsNullOrWhiteSpace(isEnabledText) && bool.TryParse(isEnabledText, out var parsedIsEnabled) && parsedIsEnabled;
            var includedObjectVersions = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "IncludedObjectVersions", StringComparison.Ordinal))?.Value ?? "All";

            S3InventoryFilter? filter = null;
            var filterElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Filter", StringComparison.Ordinal));
            if (filterElement is not null) {
                filter = new S3InventoryFilter
                {
                    Prefix = filterElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Prefix", StringComparison.Ordinal))?.Value
                };
            }

            S3InventoryDestination? destination = null;
            var destElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Destination", StringComparison.Ordinal));
            if (destElement is not null) {
                S3InventoryS3BucketDestination? s3BucketDestination = null;
                var s3BucketDestElement = destElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "S3BucketDestination", StringComparison.Ordinal));
                if (s3BucketDestElement is not null) {
                    s3BucketDestination = new S3InventoryS3BucketDestination
                    {
                        Format = s3BucketDestElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Format", StringComparison.Ordinal))?.Value ?? "CSV",
                        AccountId = s3BucketDestElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "AccountId", StringComparison.Ordinal))?.Value,
                        Bucket = s3BucketDestElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Bucket", StringComparison.Ordinal))?.Value ?? string.Empty,
                        Prefix = s3BucketDestElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Prefix", StringComparison.Ordinal))?.Value
                    };
                }
                destination = new S3InventoryDestination
                {
                    S3BucketDestination = s3BucketDestination
                };
            }

            S3InventorySchedule? schedule = null;
            var scheduleElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Schedule", StringComparison.Ordinal));
            if (scheduleElement is not null) {
                schedule = new S3InventorySchedule
                {
                    Frequency = scheduleElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Frequency", StringComparison.Ordinal))?.Value ?? "Daily"
                };
            }

            var optionalFieldsElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "OptionalFields", StringComparison.Ordinal));
            var optionalFields = optionalFieldsElement?.Elements()
                .Where(static child => string.Equals(child.Name.LocalName, "Field", StringComparison.Ordinal))
                .Select(static child => child.Value)
                .ToArray() ?? [];

            return new S3InventoryConfiguration
            {
                Id = id,
                IsEnabled = isEnabled,
                Destination = destination,
                Schedule = schedule,
                Filter = filter,
                IncludedObjectVersions = includedObjectVersions,
                OptionalFields = optionalFields
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadInventoryConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadInventoryConfiguration", exception.Message);
            throw new FormatException("The inventory configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads an intelligent tiering configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3IntelligentTieringConfiguration"/>.</returns>
    public static async Task<S3IntelligentTieringConfiguration> ReadIntelligentTieringConfigurationAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The intelligent tiering configuration request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "IntelligentTieringConfiguration", StringComparison.Ordinal)) {
                throw new FormatException("The intelligent tiering configuration request body must contain a root 'IntelligentTieringConfiguration' element.");
            }

            var id = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Id", StringComparison.Ordinal))?.Value ?? string.Empty;
            var status = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Status", StringComparison.Ordinal))?.Value ?? "Enabled";

            S3IntelligentTieringFilter? filter = null;
            var filterElement = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Filter", StringComparison.Ordinal));
            if (filterElement is not null) {
                var andElement = filterElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "And", StringComparison.Ordinal));
                var filterSource = andElement ?? filterElement;
                filter = new S3IntelligentTieringFilter
                {
                    Prefix = filterSource.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Prefix", StringComparison.Ordinal))?.Value,
                    Tags = filterSource.Elements()
                        .Where(static child => string.Equals(child.Name.LocalName, "Tag", StringComparison.Ordinal))
                        .Select(static child => new S3Tag
                        {
                            Key = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Key", StringComparison.Ordinal))?.Value ?? string.Empty,
                            Value = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Value", StringComparison.Ordinal))?.Value ?? string.Empty
                        })
                        .ToArray()
                };
            }

            var tierings = root.Elements()
                .Where(static element => string.Equals(element.Name.LocalName, "Tiering", StringComparison.Ordinal))
                .Select(static element =>
                {
                    var daysText = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Days", StringComparison.Ordinal))?.Value;
                    return new S3Tiering
                    {
                        AccessTier = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "AccessTier", StringComparison.Ordinal))?.Value ?? string.Empty,
                        Days = !string.IsNullOrWhiteSpace(daysText) && int.TryParse(daysText, out var parsedDays) ? parsedDays : 0
                    };
                })
                .ToArray();

            return new S3IntelligentTieringConfiguration
            {
                Id = id,
                Status = status,
                Filter = filter,
                Tierings = tierings
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadIntelligentTieringConfiguration", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadIntelligentTieringConfiguration", exception.Message);
            throw new FormatException("The intelligent tiering configuration request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads an object retention configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3ObjectRetention"/>.</returns>
    public static async Task<S3ObjectRetention> ReadObjectRetentionAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The object retention request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "Retention", StringComparison.Ordinal)) {
                throw new FormatException("The object retention request body must contain a root 'Retention' element.");
            }

            var mode = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Mode", StringComparison.Ordinal))?.Value;
            var retainUntilDateText = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "RetainUntilDate", StringComparison.Ordinal))?.Value;
            DateTimeOffset? retainUntilDateUtc = null;
            if (!string.IsNullOrWhiteSpace(retainUntilDateText) && DateTimeOffset.TryParse(retainUntilDateText, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind, out var parsedDate)) {
                retainUntilDateUtc = parsedDate;
            }

            return new S3ObjectRetention
            {
                Mode = mode,
                RetainUntilDateUtc = retainUntilDateUtc
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadObjectRetention", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadObjectRetention", exception.Message);
            throw new FormatException("The object retention request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads an object legal hold configuration from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3ObjectLegalHold"/>.</returns>
    public static async Task<S3ObjectLegalHold> ReadObjectLegalHoldAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The object legal hold request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "LegalHold", StringComparison.Ordinal)) {
                throw new FormatException("The object legal hold request body must contain a root 'LegalHold' element.");
            }

            return new S3ObjectLegalHold
            {
                Status = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Status", StringComparison.Ordinal))?.Value
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadObjectLegalHold", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadObjectLegalHold", exception.Message);
            throw new FormatException("The object legal hold request body is not valid XML.", exception);
        }
    }

    /// <summary>Reads a restore request from the XML request body.</summary>
    /// <param name="content">The request body stream containing the XML payload.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>The deserialized <see cref="S3RestoreRequest"/>.</returns>
    public static async Task<S3RestoreRequest> ReadRestoreRequestAsync(Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        using var reader = new StreamReader(content, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The restore request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "RestoreRequest", StringComparison.Ordinal)) {
                throw new FormatException("The restore request body must contain a root 'RestoreRequest' element.");
            }

            var daysText = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Days", StringComparison.Ordinal))?.Value;
            int? days = !string.IsNullOrWhiteSpace(daysText) && int.TryParse(daysText, out var parsedDays) ? parsedDays : null;

            string? glacierJobTier = null;
            var glacierJobParameters = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "GlacierJobParameters", StringComparison.Ordinal));
            if (glacierJobParameters is not null) {
                glacierJobTier = glacierJobParameters.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Tier", StringComparison.Ordinal))?.Value;
            }

            return new S3RestoreRequest
            {
                Days = days,
                GlacierJobTier = glacierJobTier
            };
        }
        catch (FormatException ex) {
            ProtocolTelemetry.RecordXmlParseError("ReadRestoreRequest", ex.Message);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            ProtocolTelemetry.RecordXmlParseError("ReadRestoreRequest", exception.Message);
            throw new FormatException("The restore request body is not valid XML.", exception);
        }
    }

    private static S3BucketEncryptionRule ParseBucketEncryptionRule(XElement element)
    {
        var defaultEncryptionElement = GetSingleRequiredChild(
            element,
            "ApplyServerSideEncryptionByDefault",
            "Each bucket encryption rule must contain an 'ApplyServerSideEncryptionByDefault' element.",
            "Each bucket encryption rule must not contain multiple 'ApplyServerSideEncryptionByDefault' elements.");

        var sseAlgorithm = GetSingleRequiredChildValue(
            defaultEncryptionElement,
            "SSEAlgorithm",
            "Each bucket encryption rule must contain a non-empty 'SSEAlgorithm' element.",
            "Each bucket encryption rule must not contain multiple 'SSEAlgorithm' elements.");

        var kmsMasterKeyId = GetSingleOptionalChildValue(
            defaultEncryptionElement,
            "KMSMasterKeyID",
            "Each bucket encryption rule must not contain multiple 'KMSMasterKeyID' elements.");

        var bucketKeyEnabledText = GetSingleOptionalChildValue(
            element,
            "BucketKeyEnabled",
            "Each bucket encryption rule must not contain multiple 'BucketKeyEnabled' elements.");

        bool? bucketKeyEnabled = null;
        if (bucketKeyEnabledText is not null) {
            if (!bool.TryParse(bucketKeyEnabledText, out var parsedBucketKeyEnabled)) {
                throw new FormatException("The 'BucketKeyEnabled' element must be 'true' or 'false'.");
            }

            bucketKeyEnabled = parsedBucketKeyEnabled;
        }

        return new S3BucketEncryptionRule
        {
            DefaultEncryption = new S3BucketEncryptionByDefault
            {
                SseAlgorithm = sseAlgorithm,
                KmsMasterKeyId = kmsMasterKeyId
            },
            BucketKeyEnabled = bucketKeyEnabled
        };
    }

    private static XElement GetSingleRequiredChild(
        XElement parent,
        string childName,
        string missingMessage,
        string duplicateMessage)
    {
        var matchingElements = parent.Elements()
            .Where(element => string.Equals(element.Name.LocalName, childName, StringComparison.Ordinal))
            .ToArray();

        return matchingElements.Length switch
        {
            1 => matchingElements[0],
            0 => throw new FormatException(missingMessage),
            _ => throw new FormatException(duplicateMessage)
        };
    }

    private static string GetSingleRequiredChildValue(
        XElement parent,
        string childName,
        string missingMessage,
        string duplicateMessage)
    {
        var value = GetSingleOptionalChildValue(parent, childName, duplicateMessage);
        return string.IsNullOrWhiteSpace(value)
            ? throw new FormatException(missingMessage)
            : value;
    }

    private static string? GetSingleOptionalChildValue(
        XElement parent,
        string childName,
        string duplicateMessage)
    {
        var matchingElements = parent.Elements()
            .Where(element => string.Equals(element.Name.LocalName, childName, StringComparison.Ordinal))
            .ToArray();

        return matchingElements.Length switch
        {
            0 => null,
            1 => matchingElements[0].Value,
            _ => throw new FormatException(duplicateMessage)
        };
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

    private static S3AccessControlGrant ParseAccessControlGrant(XElement element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var granteeElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Grantee", StringComparison.Ordinal))
            ?? throw new FormatException("Each ACL grant must contain a 'Grantee' element.");
        var permission = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Permission", StringComparison.Ordinal))?.Value?.Trim();
        if (string.IsNullOrWhiteSpace(permission)) {
            throw new FormatException("Each ACL grant must contain a non-empty 'Permission' element.");
        }

        var type = granteeElement.Attributes().FirstOrDefault(static attribute =>
            string.Equals(attribute.Name.LocalName, "type", StringComparison.Ordinal)
            && string.Equals(attribute.Name.NamespaceName, "http://www.w3.org/2001/XMLSchema-instance", StringComparison.Ordinal))?.Value;
        if (string.IsNullOrWhiteSpace(type)) {
            throw new FormatException("Each ACL grant grantee must declare an xsi:type attribute.");
        }

        return new S3AccessControlGrant
        {
            Grantee = new S3AccessControlGrantee
            {
                Type = type.Trim(),
                Id = granteeElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ID", StringComparison.Ordinal))?.Value,
                DisplayName = granteeElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "DisplayName", StringComparison.Ordinal))?.Value,
                Uri = granteeElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "URI", StringComparison.Ordinal))?.Value
            },
            Permission = permission
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

    private static S3LifecycleRule ParseLifecycleRule(XElement element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var id = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ID", StringComparison.Ordinal))?.Value;
        var status = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Status", StringComparison.Ordinal))?.Value ?? "Disabled";

        string? filterPrefix = null;
        IReadOnlyList<S3LifecycleFilterTag>? filterTags = null;
        var filterElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Filter", StringComparison.Ordinal));
        if (filterElement is not null) {
            var andElement = filterElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "And", StringComparison.Ordinal));
            var filterSource = andElement ?? filterElement;
            filterPrefix = filterSource.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Prefix", StringComparison.Ordinal))?.Value;
            filterTags = filterSource.Elements()
                .Where(static child => string.Equals(child.Name.LocalName, "Tag", StringComparison.Ordinal))
                .Select(static child => new S3LifecycleFilterTag
                {
                    Key = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Key", StringComparison.Ordinal))?.Value ?? string.Empty,
                    Value = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Value", StringComparison.Ordinal))?.Value ?? string.Empty
                })
                .ToArray();
            if (filterTags.Count == 0) {
                filterTags = null;
            }
        }

        int? expirationDays = null;
        string? expirationDate = null;
        bool? expiredObjectDeleteMarker = null;
        var expirationElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Expiration", StringComparison.Ordinal));
        if (expirationElement is not null) {
            var daysText = expirationElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Days", StringComparison.Ordinal))?.Value;
            expirationDays = !string.IsNullOrWhiteSpace(daysText) && int.TryParse(daysText, out var parsedExpDays) ? parsedExpDays : null;
            expirationDate = expirationElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Date", StringComparison.Ordinal))?.Value;
            var expObjDelMarkerText = expirationElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ExpiredObjectDeleteMarker", StringComparison.Ordinal))?.Value;
            expiredObjectDeleteMarker = !string.IsNullOrWhiteSpace(expObjDelMarkerText) && bool.TryParse(expObjDelMarkerText, out var parsedExpObjDelMarker) ? parsedExpObjDelMarker : null;
        }

        var transitions = element.Elements()
            .Where(static child => string.Equals(child.Name.LocalName, "Transition", StringComparison.Ordinal))
            .Select(static child =>
            {
                var daysText = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Days", StringComparison.Ordinal))?.Value;
                return new S3LifecycleTransition
                {
                    Days = !string.IsNullOrWhiteSpace(daysText) && int.TryParse(daysText, out var parsedTDays) ? parsedTDays : null,
                    Date = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Date", StringComparison.Ordinal))?.Value,
                    StorageClass = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "StorageClass", StringComparison.Ordinal))?.Value ?? string.Empty
                };
            })
            .ToArray();

        int? noncurrentVersionExpirationDays = null;
        var nveElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "NoncurrentVersionExpiration", StringComparison.Ordinal));
        if (nveElement is not null) {
            var daysText = nveElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "NoncurrentDays", StringComparison.Ordinal))?.Value;
            noncurrentVersionExpirationDays = !string.IsNullOrWhiteSpace(daysText) && int.TryParse(daysText, out var parsedNveDays) ? parsedNveDays : null;
        }

        var noncurrentVersionTransitions = element.Elements()
            .Where(static child => string.Equals(child.Name.LocalName, "NoncurrentVersionTransition", StringComparison.Ordinal))
            .Select(static child =>
            {
                var daysText = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "NoncurrentDays", StringComparison.Ordinal))?.Value;
                return new S3LifecycleNoncurrentVersionTransition
                {
                    NoncurrentDays = !string.IsNullOrWhiteSpace(daysText) && int.TryParse(daysText, out var parsedNvtDays) ? parsedNvtDays : null,
                    StorageClass = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "StorageClass", StringComparison.Ordinal))?.Value ?? string.Empty
                };
            })
            .ToArray();

        int? abortIncompleteMultipartUploadDaysAfterInitiation = null;
        var abortElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "AbortIncompleteMultipartUpload", StringComparison.Ordinal));
        if (abortElement is not null) {
            var daysText = abortElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "DaysAfterInitiation", StringComparison.Ordinal))?.Value;
            abortIncompleteMultipartUploadDaysAfterInitiation = !string.IsNullOrWhiteSpace(daysText) && int.TryParse(daysText, out var parsedAbortDays) ? parsedAbortDays : null;
        }

        return new S3LifecycleRule
        {
            Id = id,
            FilterPrefix = filterPrefix,
            FilterTags = filterTags,
            Status = status,
            ExpirationDays = expirationDays,
            ExpirationDate = expirationDate,
            ExpiredObjectDeleteMarker = expiredObjectDeleteMarker,
            NoncurrentVersionExpirationDays = noncurrentVersionExpirationDays,
            AbortIncompleteMultipartUploadDaysAfterInitiation = abortIncompleteMultipartUploadDaysAfterInitiation,
            Transitions = transitions,
            NoncurrentVersionTransitions = noncurrentVersionTransitions
        };
    }

    private static S3ReplicationRule ParseReplicationRule(XElement element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var id = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "ID", StringComparison.Ordinal))?.Value;
        var status = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Status", StringComparison.Ordinal))?.Value ?? "Disabled";

        string? filterPrefix = null;
        var filterElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Filter", StringComparison.Ordinal));
        if (filterElement is not null) {
            filterPrefix = filterElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Prefix", StringComparison.Ordinal))?.Value;
        }

        var priorityText = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Priority", StringComparison.Ordinal))?.Value;
        int? priority = !string.IsNullOrWhiteSpace(priorityText) && int.TryParse(priorityText, out var parsedPriority) ? parsedPriority : null;

        var destElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Destination", StringComparison.Ordinal));
        var destination = new S3ReplicationDestination();
        if (destElement is not null) {
            destination = new S3ReplicationDestination
            {
                Bucket = destElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Bucket", StringComparison.Ordinal))?.Value ?? string.Empty,
                StorageClass = destElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "StorageClass", StringComparison.Ordinal))?.Value,
                Account = destElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Account", StringComparison.Ordinal))?.Value
            };
        }

        bool? deleteMarkerReplication = null;
        var dmrElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "DeleteMarkerReplication", StringComparison.Ordinal));
        if (dmrElement is not null) {
            var dmrStatus = dmrElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Status", StringComparison.Ordinal))?.Value;
            deleteMarkerReplication = string.Equals(dmrStatus, "Enabled", StringComparison.OrdinalIgnoreCase);
        }

        return new S3ReplicationRule
        {
            Id = id,
            Status = status,
            FilterPrefix = filterPrefix,
            Destination = destination,
            Priority = priority,
            DeleteMarkerReplication = deleteMarkerReplication
        };
    }

    private static S3NotificationFilterRuleSet? ParseNotificationFilter(XElement element)
    {
        var filterElement = element.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "Filter", StringComparison.Ordinal));
        if (filterElement is null) {
            return null;
        }

        var s3KeyElement = filterElement.Elements().FirstOrDefault(static child => string.Equals(child.Name.LocalName, "S3Key", StringComparison.Ordinal));
        if (s3KeyElement is null) {
            return null;
        }

        var rules = s3KeyElement.Elements()
            .Where(static child => string.Equals(child.Name.LocalName, "FilterRule", StringComparison.Ordinal))
            .Select(static child => new S3NotificationFilterRule
            {
                Name = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Name", StringComparison.Ordinal))?.Value ?? string.Empty,
                Value = child.Elements().FirstOrDefault(static g => string.Equals(g.Name.LocalName, "Value", StringComparison.Ordinal))?.Value ?? string.Empty
            })
            .ToArray();

        return new S3NotificationFilterRuleSet
        {
            S3KeyRules = rules
        };
    }
}

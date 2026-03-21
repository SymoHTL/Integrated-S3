using System.Net;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Xml;
using System.Xml.Linq;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Services;
using IntegratedS3.Protocol;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Net.Http.Headers;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;

namespace IntegratedS3.AspNetCore.Endpoints;

public static class IntegratedS3EndpointRouteBuilderExtensions
{
    private const string SigV4AuthenticationClaimType = "integrateds3:auth-type";
    private const string SigV4AuthenticationClaimValue = "sigv4";
    private const string MetadataHeaderPrefix = "x-amz-meta-";
    private const string LegacyMetadataHeaderPrefix = "x-integrateds3-meta-";
    private const string ContinuationTokenHeaderName = "x-integrateds3-continuation-token";
    private const string CopySourceHeaderName = "x-amz-copy-source";
    private const string CannedAclHeaderName = "x-amz-acl";
    private const string GrantFullControlHeaderName = "x-amz-grant-full-control";
    private const string GrantReadHeaderName = "x-amz-grant-read";
    private const string GrantReadAcpHeaderName = "x-amz-grant-read-acp";
    private const string GrantWriteHeaderName = "x-amz-grant-write";
    private const string GrantWriteAcpHeaderName = "x-amz-grant-write-acp";
    private const string MetadataDirectiveHeaderName = "x-amz-metadata-directive";
    private const string CopySourceIfMatchHeaderName = "x-amz-copy-source-if-match";
    private const string CopySourceIfNoneMatchHeaderName = "x-amz-copy-source-if-none-match";
    private const string CopySourceIfModifiedSinceHeaderName = "x-amz-copy-source-if-modified-since";
    private const string CopySourceIfUnmodifiedSinceHeaderName = "x-amz-copy-source-if-unmodified-since";
    private const string CopySourceRangeHeaderName = "x-amz-copy-source-range";
    private const string TaggingHeaderName = "x-amz-tagging";
    private const string TaggingDirectiveHeaderName = "x-amz-tagging-directive";
    private const string CopySourceVersionIdHeaderName = "x-amz-copy-source-version-id";
    private const string ServerSideEncryptionHeaderPrefix = "x-amz-server-side-encryption";
    private const string CopySourceServerSideEncryptionHeaderPrefix = "x-amz-copy-source-server-side-encryption";
    private const string ServerSideEncryptionHeaderName = "x-amz-server-side-encryption";
    private const string ServerSideEncryptionAwsKmsKeyIdHeaderName = "x-amz-server-side-encryption-aws-kms-key-id";
    private const string ServerSideEncryptionContextHeaderName = "x-amz-server-side-encryption-context";
    private const string ServerSideEncryptionCustomerAlgorithmHeaderName = "x-amz-server-side-encryption-customer-algorithm";
    private const string ServerSideEncryptionCustomerKeyHeaderName = "x-amz-server-side-encryption-customer-key";
    private const string ServerSideEncryptionCustomerKeyMd5HeaderName = "x-amz-server-side-encryption-customer-key-MD5";
    private const string CopySourceServerSideEncryptionCustomerAlgorithmHeaderName = "x-amz-copy-source-server-side-encryption-customer-algorithm";
    private const string CopySourceServerSideEncryptionCustomerKeyHeaderName = "x-amz-copy-source-server-side-encryption-customer-key";
    private const string CopySourceServerSideEncryptionCustomerKeyMd5HeaderName = "x-amz-copy-source-server-side-encryption-customer-key-MD5";
    private const string SdkChecksumAlgorithmHeaderName = "x-amz-sdk-checksum-algorithm";
    private const string ChecksumAlgorithmHeaderName = "x-amz-checksum-algorithm";
    private const string ChecksumCrc32HeaderName = "x-amz-checksum-crc32";
    private const string ChecksumCrc32cHeaderName = "x-amz-checksum-crc32c";
    private const string ChecksumSha1HeaderName = "x-amz-checksum-sha1";
    private const string ChecksumSha256HeaderName = "x-amz-checksum-sha256";
    private const string ChecksumCrc64NvmeHeaderName = "x-amz-checksum-crc64nvme";
    private const string AwsContentSha256HeaderName = "x-amz-content-sha256";
    private const string AwsTrailerHeaderName = "x-amz-trailer";
    private const string AwsTrailerSignatureHeaderName = "x-amz-trailer-signature";
    private const string StreamingAws4HmacSha256PayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
    private const string StreamingAwsEcdsaSha256PayloadTrailer = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER";
    private const string StreamingUnsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
    private const string ChecksumTypeHeaderName = "x-amz-checksum-type";
    private const string DeleteMarkerHeaderName = "x-amz-delete-marker";
    private const string TaggingCountHeaderName = "x-amz-tagging-count";
    private const string VersionIdHeaderName = "x-amz-version-id";
    private const string ObjectLockModeHeaderName = "x-amz-object-lock-mode";
    private const string ObjectLockRetainUntilDateHeaderName = "x-amz-object-lock-retain-until-date";
    private const string ObjectLockLegalHoldHeaderName = "x-amz-object-lock-legal-hold";
    private const string StorageClassHeaderName = "x-amz-storage-class";
    private const string ServerSideEncryptionBucketKeyEnabledHeaderName = "x-amz-server-side-encryption-bucket-key-enabled";
    private const string BucketRegionHeaderName = "x-amz-bucket-region";
    private const string ErrorCodeHeaderName = "x-amz-error-code";
    private const string ErrorMessageHeaderName = "x-amz-error-message";
    private const string NoSuchVersionMessage = "The specified version does not exist.";
    private const string XmlContentType = "application/xml";
    private const string ListTypeQueryParameterName = "list-type";
    private const string PrefixQueryParameterName = "prefix";
    private const string DelimiterQueryParameterName = "delimiter";
    private const string MarkerQueryParameterName = "marker";
    private const string StartAfterQueryParameterName = "start-after";
    private const string MaxKeysQueryParameterName = "max-keys";
    private const string MaxUploadsQueryParameterName = "max-uploads";
    private const string ContinuationTokenQueryParameterName = "continuation-token";
    private const string AclQueryParameterName = "acl";
    private const string EncodingTypeQueryParameterName = "encoding-type";
    private const string FetchOwnerQueryParameterName = "fetch-owner";
    private const string LocationQueryParameterName = "location";
    private const string CorsQueryParameterName = "cors";
    private const string PolicyQueryParameterName = "policy";
    private const string TaggingQueryParameterName = "tagging";
    private const string RetentionQueryParameterName = "retention";
    private const string LegalHoldQueryParameterName = "legal-hold";
    private const string AttributesQueryParameterName = "attributes";
    private const string ObjectAttributesHeaderName = "x-amz-object-attributes";
    private const string VersioningQueryParameterName = "versioning";
    private const string EncryptionQueryParameterName = "encryption";
    private const string VersionsQueryParameterName = "versions";
    private const string KeyMarkerQueryParameterName = "key-marker";
    private const string VersionIdMarkerQueryParameterName = "version-id-marker";
    private const string UploadIdMarkerQueryParameterName = "upload-id-marker";
    private const string UploadsQueryParameterName = "uploads";
    private const string UploadIdQueryParameterName = "uploadId";
    private const string PartNumberQueryParameterName = "partNumber";
    private const string PartNumberMarkerQueryParameterName = "part-number-marker";
    private const string MaxPartsQueryParameterName = "max-parts";
    private const string VersionIdQueryParameterName = "versionId";
    private const string DeleteQueryParameterName = "delete";
    private const string LoggingQueryParameterName = "logging";
    private const string WebsiteQueryParameterName = "website";
    private const string RequestPaymentQueryParameterName = "requestPayment";
    private const string AccelerateQueryParameterName = "accelerate";
    private const string LifecycleQueryParameterName = "lifecycle";
    private const string ReplicationQueryParameterName = "replication";
    private const string NotificationQueryParameterName = "notification";
    private const string ObjectLockQueryParameterName = "object-lock";
    private const string AnalyticsQueryParameterName = "analytics";
    private const string MetricsQueryParameterName = "metrics";
    private const string InventoryQueryParameterName = "inventory";
    private const string IntelligentTieringQueryParameterName = "intelligent-tiering";
    private const string IdQueryParameterName = "id";
    private const string RestoreQueryParameterName = "restore";
    private const string SelectQueryParameterName = "select";
    private const string SelectTypeQueryParameterName = "select-type";
    private const string OriginHeaderName = "Origin";
    private const string AccessControlRequestMethodHeaderName = "Access-Control-Request-Method";
    private const string AccessControlRequestHeadersHeaderName = "Access-Control-Request-Headers";
    private const string AccessControlAllowOriginHeaderName = "Access-Control-Allow-Origin";
    private const string AccessControlAllowCredentialsHeaderName = "Access-Control-Allow-Credentials";
    private const string AccessControlAllowMethodsHeaderName = "Access-Control-Allow-Methods";
    private const string AccessControlAllowHeadersHeaderName = "Access-Control-Allow-Headers";
    private const string AccessControlExposeHeadersHeaderName = "Access-Control-Expose-Headers";
    private const string AccessControlMaxAgeHeaderName = "Access-Control-Max-Age";
    private const string AllUsersGroupUri = "http://acs.amazonaws.com/groups/global/AllUsers";
    private const string AuthenticatedUsersGroupUri = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
    private const string ChecksumModeHeaderName = "x-amz-checksum-mode";
    private const string CanonicalUserGranteeType = "CanonicalUser";
    private const string GroupGranteeType = "Group";
    private const string OwnerId = "integrated-s3";
    private static readonly HashSet<string> BucketAclQueryParameters = CreateQueryParameterSet(AclQueryParameterName);
    private const string UrlEncodingTypeValue = "url";
    private const string DefaultS3ListingIdentityId = "integrated-s3";
    private static readonly UTF8Encoding StrictTaggingHeaderEncoding = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
    private static readonly HashSet<string> EmptyQueryParameters = CreateQueryParameterSet();
    private static readonly HashSet<string> BucketListObjectsV1QueryParameters = CreateQueryParameterSet(PrefixQueryParameterName, DelimiterQueryParameterName, MarkerQueryParameterName, MaxKeysQueryParameterName, EncodingTypeQueryParameterName);
    private static readonly HashSet<string> BucketListObjectsV2QueryParameters = CreateQueryParameterSet(ListTypeQueryParameterName, PrefixQueryParameterName, DelimiterQueryParameterName, StartAfterQueryParameterName, MaxKeysQueryParameterName, ContinuationTokenQueryParameterName, EncodingTypeQueryParameterName, FetchOwnerQueryParameterName);
    private static readonly EndpointFeatureDescriptor ServiceEndpointFeature = new(IntegratedS3EndpointFeature.Service, "service", nameof(IntegratedS3EndpointOptions.ServiceRouteAuthorization), nameof(IntegratedS3EndpointOptions.ConfigureServiceRouteGroup));
    private static readonly EndpointFeatureDescriptor BucketEndpointFeature = new(IntegratedS3EndpointFeature.Bucket, "bucket", nameof(IntegratedS3EndpointOptions.BucketRouteAuthorization), nameof(IntegratedS3EndpointOptions.ConfigureBucketRouteGroup));
    private static readonly EndpointFeatureDescriptor ObjectEndpointFeature = new(IntegratedS3EndpointFeature.Object, "object", nameof(IntegratedS3EndpointOptions.ObjectRouteAuthorization), nameof(IntegratedS3EndpointOptions.ConfigureObjectRouteGroup));
    private static readonly EndpointFeatureDescriptor MultipartEndpointFeature = new(IntegratedS3EndpointFeature.Multipart, "multipart", nameof(IntegratedS3EndpointOptions.MultipartRouteAuthorization), nameof(IntegratedS3EndpointOptions.ConfigureMultipartRouteGroup));
    private static readonly EndpointFeatureDescriptor AdminEndpointFeature = new(IntegratedS3EndpointFeature.Admin, "admin", nameof(IntegratedS3EndpointOptions.AdminRouteAuthorization), nameof(IntegratedS3EndpointOptions.ConfigureAdminRouteGroup));
    private static readonly HashSet<string> BucketLocationQueryParameters = CreateQueryParameterSet(LocationQueryParameterName);
    private static readonly HashSet<string> BucketCorsQueryParameters = CreateQueryParameterSet(CorsQueryParameterName);
    private static readonly HashSet<string> BucketPolicyQueryParameters = CreateQueryParameterSet(PolicyQueryParameterName);
    private static readonly HashSet<string> BucketVersioningQueryParameters = CreateQueryParameterSet(VersioningQueryParameterName);
    private static readonly HashSet<string> BucketEncryptionQueryParameters = CreateQueryParameterSet(EncryptionQueryParameterName);
    private static readonly HashSet<string> BucketTaggingQueryParameters = CreateQueryParameterSet(TaggingQueryParameterName);
    private static readonly HashSet<string> BucketLoggingQueryParameters = CreateQueryParameterSet(LoggingQueryParameterName);
    private static readonly HashSet<string> BucketWebsiteQueryParameters = CreateQueryParameterSet(WebsiteQueryParameterName);
    private static readonly HashSet<string> BucketRequestPaymentQueryParameters = CreateQueryParameterSet(RequestPaymentQueryParameterName);
    private static readonly HashSet<string> BucketAccelerateQueryParameters = CreateQueryParameterSet(AccelerateQueryParameterName);
    private static readonly HashSet<string> BucketLifecycleQueryParameters = CreateQueryParameterSet(LifecycleQueryParameterName);
    private static readonly HashSet<string> BucketReplicationQueryParameters = CreateQueryParameterSet(ReplicationQueryParameterName);
    private static readonly HashSet<string> BucketNotificationQueryParameters = CreateQueryParameterSet(NotificationQueryParameterName);
    private static readonly HashSet<string> BucketObjectLockQueryParameters = CreateQueryParameterSet(ObjectLockQueryParameterName);
    private static readonly HashSet<string> BucketAnalyticsQueryParameters = CreateQueryParameterSet(AnalyticsQueryParameterName, IdQueryParameterName);
    private static readonly HashSet<string> BucketMetricsQueryParameters = CreateQueryParameterSet(MetricsQueryParameterName, IdQueryParameterName);
    private static readonly HashSet<string> BucketInventoryQueryParameters = CreateQueryParameterSet(InventoryQueryParameterName, IdQueryParameterName);
    private static readonly HashSet<string> BucketVersionListingQueryParameters = CreateQueryParameterSet(VersionsQueryParameterName, PrefixQueryParameterName, DelimiterQueryParameterName, MaxKeysQueryParameterName, KeyMarkerQueryParameterName, VersionIdMarkerQueryParameterName, EncodingTypeQueryParameterName);
    private static readonly HashSet<string> BucketMultipartUploadsQueryParameters = CreateQueryParameterSet(UploadsQueryParameterName, PrefixQueryParameterName, DelimiterQueryParameterName, MaxUploadsQueryParameterName, KeyMarkerQueryParameterName, UploadIdMarkerQueryParameterName, EncodingTypeQueryParameterName);
    private static readonly HashSet<string> BucketDeleteQueryParameters = CreateQueryParameterSet(DeleteQueryParameterName);
    private static readonly HashSet<string> KnownBucketQueryParameters = CreateQueryParameterSet(ListTypeQueryParameterName, PrefixQueryParameterName, DelimiterQueryParameterName, MarkerQueryParameterName, StartAfterQueryParameterName, MaxKeysQueryParameterName, MaxUploadsQueryParameterName, ContinuationTokenQueryParameterName, EncodingTypeQueryParameterName, FetchOwnerQueryParameterName, LocationQueryParameterName, AclQueryParameterName, CorsQueryParameterName, PolicyQueryParameterName, VersioningQueryParameterName, EncryptionQueryParameterName, TaggingQueryParameterName, LoggingQueryParameterName, WebsiteQueryParameterName, RequestPaymentQueryParameterName, AccelerateQueryParameterName, LifecycleQueryParameterName, ReplicationQueryParameterName, NotificationQueryParameterName, ObjectLockQueryParameterName, AnalyticsQueryParameterName, MetricsQueryParameterName, InventoryQueryParameterName, IdQueryParameterName, VersionsQueryParameterName, KeyMarkerQueryParameterName, VersionIdMarkerQueryParameterName, UploadIdMarkerQueryParameterName, UploadsQueryParameterName, DeleteQueryParameterName);
    private static readonly HashSet<string> ObjectVersionQueryParameters = CreateQueryParameterSet(VersionIdQueryParameterName);
    private static readonly HashSet<string> ObjectAclQueryParameters = CreateQueryParameterSet(AclQueryParameterName);
    private static readonly HashSet<string> ObjectTaggingQueryParameters = CreateQueryParameterSet(TaggingQueryParameterName, VersionIdQueryParameterName);
    private static readonly HashSet<string> ObjectRetentionQueryParameters = CreateQueryParameterSet(RetentionQueryParameterName, VersionIdQueryParameterName);
    private static readonly HashSet<string> ObjectLegalHoldQueryParameters = CreateQueryParameterSet(LegalHoldQueryParameterName, VersionIdQueryParameterName);
    private static readonly HashSet<string> ObjectAttributesQueryParameters = CreateQueryParameterSet(AttributesQueryParameterName, VersionIdQueryParameterName);
    private static readonly HashSet<string> ObjectRestoreQueryParameters = CreateQueryParameterSet(RestoreQueryParameterName, VersionIdQueryParameterName);
    private static readonly HashSet<string> ObjectSelectQueryParameters = CreateQueryParameterSet(SelectQueryParameterName, SelectTypeQueryParameterName);
    private static readonly HashSet<string> ObjectMultipartInitiateQueryParameters = CreateQueryParameterSet(UploadsQueryParameterName);
    private static readonly HashSet<string> ObjectMultipartPartQueryParameters = CreateQueryParameterSet(UploadIdQueryParameterName, PartNumberQueryParameterName);
    private static readonly HashSet<string> ObjectMultipartUploadQueryParameters = CreateQueryParameterSet(UploadIdQueryParameterName);
    private static readonly HashSet<string> ObjectMultipartListPartsQueryParameters = CreateQueryParameterSet(UploadIdQueryParameterName, PartNumberMarkerQueryParameterName, MaxPartsQueryParameterName, EncodingTypeQueryParameterName);
    private static readonly HashSet<string> KnownObjectQueryParameters = CreateQueryParameterSet(AclQueryParameterName, TaggingQueryParameterName, RetentionQueryParameterName, LegalHoldQueryParameterName, AttributesQueryParameterName, VersionIdQueryParameterName, RestoreQueryParameterName, SelectQueryParameterName, SelectTypeQueryParameterName, UploadsQueryParameterName, UploadIdQueryParameterName, PartNumberQueryParameterName, PartNumberMarkerQueryParameterName, MaxPartsQueryParameterName, EncodingTypeQueryParameterName);
    private static readonly HashSet<string> SupportedManagedServerSideEncryptionRequestHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        ServerSideEncryptionHeaderName,
        ServerSideEncryptionAwsKmsKeyIdHeaderName,
        ServerSideEncryptionContextHeaderName
    };

    private static readonly HashSet<string> SupportedCustomerServerSideEncryptionRequestHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        ServerSideEncryptionCustomerAlgorithmHeaderName,
        ServerSideEncryptionCustomerKeyHeaderName,
        ServerSideEncryptionCustomerKeyMd5HeaderName
    };

    private static readonly HashSet<string> SupportedCopySourceCustomerServerSideEncryptionRequestHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        CopySourceServerSideEncryptionCustomerAlgorithmHeaderName,
        CopySourceServerSideEncryptionCustomerKeyHeaderName,
        CopySourceServerSideEncryptionCustomerKeyMd5HeaderName
    };

    [RequiresUnreferencedCode("Minimal API endpoint registration may reflect over route handler delegates and parameters.")]
    [RequiresDynamicCode("Minimal API endpoint registration may require runtime-generated code for route handler delegates.")]
    public static RouteGroupBuilder MapIntegratedS3Endpoints(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        return endpoints.MapIntegratedS3Endpoints(ResolveConfiguredEndpointOptions(endpoints));
    }

    public static RouteGroupBuilder MapIntegratedS3Endpoints(
        this IEndpointRouteBuilder endpoints,
        IntegratedS3EndpointConfigurationOptions endpointConfiguration)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(endpointConfiguration);

        var resolvedEndpointOptions = new IntegratedS3EndpointOptions();
        endpointConfiguration.ApplyTo(resolvedEndpointOptions);
        return MapIntegratedS3EndpointsCore(endpoints, resolvedEndpointOptions);
    }

    [RequiresUnreferencedCode("Minimal API endpoint registration may reflect over route handler delegates and parameters.")]
    [RequiresDynamicCode("Minimal API endpoint registration may require runtime-generated code for route handler delegates.")]
    public static RouteGroupBuilder MapIntegratedS3Endpoints(this IEndpointRouteBuilder endpoints, Action<IntegratedS3EndpointOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(configure);

        var options = ResolveConfiguredEndpointOptions(endpoints);
        configure(options);
        return endpoints.MapIntegratedS3Endpoints(options);
    }

    [RequiresUnreferencedCode("Minimal API endpoint registration may reflect over route handler delegates and parameters.")]
    [RequiresDynamicCode("Minimal API endpoint registration may require runtime-generated code for route handler delegates.")]
    public static RouteGroupBuilder MapIntegratedS3Endpoints(this IEndpointRouteBuilder endpoints, IntegratedS3EndpointOptions endpointOptions)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(endpointOptions);

        return MapIntegratedS3EndpointsCore(endpoints, endpointOptions.Clone());
    }

    private static RouteGroupBuilder MapIntegratedS3EndpointsCore(
        IEndpointRouteBuilder endpoints,
        IntegratedS3EndpointOptions resolvedEndpointOptions)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(resolvedEndpointOptions);

        var options = endpoints.ServiceProvider.GetRequiredService<IOptions<IntegratedS3Options>>().Value;
        var group = endpoints.MapGroup(options.RoutePrefix);
        group.AddEndpointFilter<IntegratedS3RequestAuthenticationEndpointFilter>();
        var routeConfiguration = CreateRouteGroupConfiguration(
            nameof(IntegratedS3EndpointOptions.RouteAuthorization),
            resolvedEndpointOptions.RouteAuthorization,
            nameof(IntegratedS3EndpointOptions.ConfigureRouteGroup),
            resolvedEndpointOptions.ConfigureRouteGroup);
        routeConfiguration.Apply?.Invoke(group);
        var hasWholeRouteConfiguration = routeConfiguration.IsConfigured;
        var serviceRouteConfiguration = CreateFeatureRouteGroupConfiguration(resolvedEndpointOptions, ServiceEndpointFeature);
        var bucketRouteConfiguration = CreateFeatureRouteGroupConfiguration(resolvedEndpointOptions, BucketEndpointFeature);
        var objectRouteConfiguration = CreateFeatureRouteGroupConfiguration(resolvedEndpointOptions, ObjectEndpointFeature);
        var multipartRouteConfiguration = CreateFeatureRouteGroupConfiguration(resolvedEndpointOptions, MultipartEndpointFeature);
        var adminRouteConfiguration = CreateFeatureRouteGroupConfiguration(resolvedEndpointOptions, AdminEndpointFeature);
        var bucketGroup = CreateConfiguredRouteGroup(group, bucketRouteConfiguration);
        var objectGroup = CreateConfiguredRouteGroup(group, objectRouteConfiguration);
        var adminGroup = CreateConfiguredRouteGroup(group, adminRouteConfiguration);
        var rootGetGroup = CreateSharedRouteGroup(
            group,
            "GET /",
            $"{nameof(IntegratedS3EndpointOptions.RootRouteAuthorization)} or {nameof(IntegratedS3EndpointOptions.ConfigureRootRouteGroup)}",
            CreateRouteGroupConfiguration(
                nameof(IntegratedS3EndpointOptions.RootRouteAuthorization),
                resolvedEndpointOptions.RootRouteAuthorization,
                nameof(IntegratedS3EndpointOptions.ConfigureRootRouteGroup),
                resolvedEndpointOptions.ConfigureRootRouteGroup),
            hasWholeRouteConfiguration,
            $"{nameof(IntegratedS3EndpointOptions.RouteAuthorization)} or {nameof(IntegratedS3EndpointOptions.ConfigureRouteGroup)}",
            (resolvedEndpointOptions.EnableServiceEndpoints, "service", serviceRouteConfiguration),
            (resolvedEndpointOptions.EnableBucketEndpoints, "bucket", bucketRouteConfiguration));
        var compatibilityGroup = CreateSharedRouteGroup(
            group,
            "/{**s3Path}",
            $"{nameof(IntegratedS3EndpointOptions.CompatibilityRouteAuthorization)} or {nameof(IntegratedS3EndpointOptions.ConfigureCompatibilityRouteGroup)}",
            CreateRouteGroupConfiguration(
                nameof(IntegratedS3EndpointOptions.CompatibilityRouteAuthorization),
                resolvedEndpointOptions.CompatibilityRouteAuthorization,
                nameof(IntegratedS3EndpointOptions.ConfigureCompatibilityRouteGroup),
                resolvedEndpointOptions.ConfigureCompatibilityRouteGroup),
            hasWholeRouteConfiguration,
            $"{nameof(IntegratedS3EndpointOptions.RouteAuthorization)} or {nameof(IntegratedS3EndpointOptions.ConfigureRouteGroup)}",
            (resolvedEndpointOptions.EnableBucketEndpoints, "bucket", bucketRouteConfiguration),
            (resolvedEndpointOptions.EnableObjectEndpoints, "object", objectRouteConfiguration),
            (resolvedEndpointOptions.EnableMultipartEndpoints, "multipart", multipartRouteConfiguration));

        if (resolvedEndpointOptions.EnableServiceEndpoints || resolvedEndpointOptions.EnableBucketEndpoints) {
            rootGetGroup.MapGet("/", (HttpContext httpContext, IOptions<IntegratedS3Options> integratedS3Options, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, IStorageServiceDescriptorProvider descriptorProvider, CancellationToken cancellationToken) =>
                    HandleRootGetAsync(httpContext, integratedS3Options, resolvedEndpointOptions, requestContextAccessor, storageService, descriptorProvider, cancellationToken))
                .WithName("GetIntegratedS3ServiceDocument");
        }

        if (resolvedEndpointOptions.EnableBucketEndpoints) {
            bucketGroup.MapMethods("/", ["PUT", "HEAD", "DELETE", "POST"], (HttpContext httpContext, IOptions<IntegratedS3Options> integratedS3Options, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    HandleS3CompatibleRootAsync(httpContext, integratedS3Options, resolvedEndpointOptions, requestContextAccessor, storageService, cancellationToken))
                .WithName("HandleIntegratedS3CompatibleRoot");

            bucketGroup.MapMethods("/", ["OPTIONS"], (HttpContext httpContext, IOptions<IntegratedS3Options> integratedS3Options, CancellationToken cancellationToken) =>
                    HandleS3CompatibleRootOptionsAsync(httpContext, integratedS3Options, resolvedEndpointOptions, cancellationToken))
                .WithName("HandleIntegratedS3CompatibleRootOptions");
        }

        if (resolvedEndpointOptions.EnableAdminEndpoints) {
            adminGroup.MapGet("/capabilities", GetCapabilitiesAsync)
                .WithName("GetIntegratedS3Capabilities");

            adminGroup.MapGet("/admin/diagnostics", GetAdminDiagnosticsAsync)
                .WithName("GetIntegratedS3AdminDiagnostics");

            adminGroup.MapGet("/admin/repairs", ListOutstandingReplicaRepairsAsync)
                .WithName("ListIntegratedS3ReplicaRepairs");
        }

        if (resolvedEndpointOptions.EnableBucketEndpoints) {
            bucketGroup.MapGet("/buckets", ListBucketsAsync)
                .WithName("ListIntegratedS3Buckets");

            bucketGroup.MapPut("/buckets/{bucketName}", async (string bucketName, HttpContext httpContext, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    WrapBucketCorsResult(bucketName, await CreateBucketAsync(bucketName, httpContext, requestContextAccessor, storageService, cancellationToken)))
                .WithName("CreateIntegratedS3Bucket");

            bucketGroup.MapMethods("/buckets/{bucketName}", ["HEAD"], async (string bucketName, HttpContext httpContext, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    WrapBucketCorsResult(bucketName, await HeadBucketAsync(bucketName, httpContext, requestContextAccessor, storageService, cancellationToken)))
                .WithName("HeadIntegratedS3Bucket");

            bucketGroup.MapDelete("/buckets/{bucketName}", async (string bucketName, HttpContext httpContext, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    WrapBucketCorsResult(bucketName, await DeleteBucketAsync(bucketName, httpContext, requestContextAccessor, storageService, cancellationToken)))
                .WithName("DeleteIntegratedS3Bucket");

            bucketGroup.MapGet("/buckets/{bucketName}/objects", async (string bucketName, string? prefix, string? continuationToken, int? pageSize, HttpContext httpContext, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    WrapBucketCorsResult(bucketName, await ListObjectsAsync(bucketName, prefix, continuationToken, pageSize, httpContext, requestContextAccessor, storageService, cancellationToken)))
                .WithName("ListIntegratedS3Objects");

            bucketGroup.MapMethods("/buckets/{bucketName}", ["OPTIONS"], HandleBucketCorsPreflightAsync)
                .WithName("OptionsIntegratedS3Bucket");

            bucketGroup.MapMethods("/buckets/{bucketName}/objects", ["OPTIONS"], HandleBucketCorsPreflightAsync)
                .WithName("OptionsIntegratedS3BucketObjects");
        }

        if (resolvedEndpointOptions.EnableObjectEndpoints) {
            objectGroup.MapPost("/presign/object", CreateObjectPresignAsync)
                .WithName("CreateIntegratedS3ObjectPresign");

            objectGroup.MapPut("/buckets/{bucketName}/objects/{**key}", async (string bucketName, string key, HttpContext httpContext, HttpRequest request, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    WrapBucketCorsResult(bucketName, await PutObjectAsync(bucketName, key, httpContext, request, requestContextAccessor, storageService, cancellationToken)))
                .WithName("PutIntegratedS3Object");

            objectGroup.MapGet("/buckets/{bucketName}/objects/{**key}", async (string bucketName, string key, HttpContext httpContext, HttpRequest request, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    WrapBucketCorsResult(bucketName, await GetObjectAsync(bucketName, key, httpContext, request, requestContextAccessor, storageService, cancellationToken)))
                .WithName("GetIntegratedS3Object");

            objectGroup.MapMethods("/buckets/{bucketName}/objects/{**key}", ["HEAD"], async (string bucketName, string key, HttpContext httpContext, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    WrapBucketCorsResult(bucketName, await HeadObjectAsync(bucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken)))
                .WithName("HeadIntegratedS3Object");

            objectGroup.MapDelete("/buckets/{bucketName}/objects/{**key}", async (string bucketName, string key, HttpContext httpContext, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    WrapBucketCorsResult(bucketName, await DeleteObjectAsync(bucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken)))
                .WithName("DeleteIntegratedS3Object");

            objectGroup.MapMethods("/buckets/{bucketName}/objects/{**key}", ["OPTIONS"], HandleObjectCorsPreflightAsync)
                .WithName("OptionsIntegratedS3Object");
        }

        if (resolvedEndpointOptions.EnableBucketEndpoints || resolvedEndpointOptions.EnableObjectEndpoints || resolvedEndpointOptions.EnableMultipartEndpoints) {
            compatibilityGroup.MapMethods("/{**s3Path}", ["GET", "PUT", "HEAD", "DELETE", "POST"], (string s3Path, HttpContext httpContext, IOptions<IntegratedS3Options> integratedS3Options, IIntegratedS3RequestContextAccessor requestContextAccessor, IStorageService storageService, CancellationToken cancellationToken) =>
                    HandleS3CompatiblePathAsync(s3Path, httpContext, integratedS3Options, resolvedEndpointOptions, requestContextAccessor, storageService, cancellationToken))
                .WithName("HandleIntegratedS3CompatiblePath");

            compatibilityGroup.MapMethods("/{**s3Path}", ["OPTIONS"], (string s3Path, HttpContext httpContext, IOptions<IntegratedS3Options> integratedS3Options, CancellationToken cancellationToken) =>
                    HandleS3CompatiblePathOptionsAsync(s3Path, httpContext, integratedS3Options, resolvedEndpointOptions, cancellationToken))
                .WithName("HandleIntegratedS3CompatiblePathOptions");
        }

        return group;
    }

    private static IntegratedS3EndpointOptions ResolveConfiguredEndpointOptions(IEndpointRouteBuilder endpoints)
    {
        var configuredOptions = endpoints.ServiceProvider.GetService<IOptions<IntegratedS3EndpointOptions>>();

        return configuredOptions?.Value.Clone() ?? new IntegratedS3EndpointOptions();
    }

    private static RouteGroupConfiguration CreateRouteGroupConfiguration(
        string authorizationPropertyName,
        IntegratedS3EndpointAuthorizationOptions? authorizationOptions,
        string callbackPropertyName,
        Action<RouteGroupBuilder>? callbackConfiguration)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(authorizationPropertyName);
        ArgumentException.ThrowIfNullOrWhiteSpace(callbackPropertyName);

        var sourceNames = new List<string>(capacity: 2);
        if (authorizationOptions?.HasConventions == true) {
            ValidateAuthorizationOptions(authorizationPropertyName, authorizationOptions);
            sourceNames.Add(authorizationPropertyName);
        }

        if (callbackConfiguration is not null) {
            sourceNames.Add(callbackPropertyName);
        }

        if (sourceNames.Count == 0) {
            return RouteGroupConfiguration.None;
        }

        return new RouteGroupConfiguration(
            group => {
                if (authorizationOptions?.HasConventions == true) {
                    ApplyAuthorizationOptions(group, authorizationOptions);
                }

                callbackConfiguration?.Invoke(group);
            },
            sourceNames.ToArray());
    }

    private static RouteGroupConfiguration CreateFeatureRouteGroupConfiguration(
        IntegratedS3EndpointOptions endpointOptions,
        EndpointFeatureDescriptor feature)
    {
        ArgumentNullException.ThrowIfNull(endpointOptions);

        return CreateRouteGroupConfiguration(
            feature.AuthorizationPropertyName,
            GetFeatureAuthorizationOptions(endpointOptions, feature.Feature),
            FormatFeatureRouteGroupConfigurationReference(feature),
            endpointOptions.GetFeatureRouteGroupConfiguration(feature.Feature));
    }

    private static IntegratedS3EndpointAuthorizationOptions? GetFeatureAuthorizationOptions(
        IntegratedS3EndpointOptions endpointOptions,
        IntegratedS3EndpointFeature feature)
    {
        ArgumentNullException.ThrowIfNull(endpointOptions);

        return feature switch
        {
            IntegratedS3EndpointFeature.Service => endpointOptions.ServiceRouteAuthorization,
            IntegratedS3EndpointFeature.Bucket => endpointOptions.BucketRouteAuthorization,
            IntegratedS3EndpointFeature.Object => endpointOptions.ObjectRouteAuthorization,
            IntegratedS3EndpointFeature.Multipart => endpointOptions.MultipartRouteAuthorization,
            IntegratedS3EndpointFeature.Admin => endpointOptions.AdminRouteAuthorization,
            _ => throw new ArgumentOutOfRangeException(nameof(feature), feature, "Unknown Integrated S3 endpoint feature.")
        };
    }

    private static RouteGroupBuilder CreateConfiguredRouteGroup(RouteGroupBuilder parentGroup, params RouteGroupConfiguration[] configurations)
    {
        ArgumentNullException.ThrowIfNull(parentGroup);

        RouteGroupBuilder? configuredGroup = null;
        foreach (var configuration in configurations) {
            if (!configuration.IsConfigured) {
                continue;
            }

            configuredGroup ??= parentGroup.MapGroup(string.Empty);
            configuration.Apply?.Invoke(configuredGroup);
        }

        return configuredGroup ?? parentGroup;
    }

    private static RouteGroupBuilder CreateSharedRouteGroup(
        RouteGroupBuilder parentGroup,
        string routeDisplayName,
        string sharedConfigurationDescription,
        RouteGroupConfiguration sharedConfiguration,
        bool hasWholeRouteConfiguration,
        string wholeRouteConfigurationDescription,
        params (bool IsEnabled, string FeatureDisplayName, RouteGroupConfiguration Configuration)[] featureConfigurations)
    {
        ArgumentNullException.ThrowIfNull(parentGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(routeDisplayName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sharedConfigurationDescription);
        ArgumentException.ThrowIfNullOrWhiteSpace(wholeRouteConfigurationDescription);

        if (sharedConfiguration.IsConfigured) {
            return CreateConfiguredRouteGroup(parentGroup, sharedConfiguration);
        }

        var enabledFeatureCount = 0;
        var enabledFeatureNames = new List<string>(featureConfigurations.Length);
        var enabledFeatureConfigurations = new List<RouteGroupConfiguration>(featureConfigurations.Length);
        var configuredFeatureCount = 0;
        var configuredConfigurationNames = new List<string>(featureConfigurations.Length * 2);

        foreach (var featureConfiguration in featureConfigurations) {
            if (!featureConfiguration.IsEnabled) {
                continue;
            }

            enabledFeatureCount++;
            enabledFeatureNames.Add(featureConfiguration.FeatureDisplayName);
            enabledFeatureConfigurations.Add(featureConfiguration.Configuration);

            if (featureConfiguration.Configuration.IsConfigured) {
                configuredFeatureCount++;
                configuredConfigurationNames.AddRange(featureConfiguration.Configuration.SourceNames);
            }
        }

        if (enabledFeatureCount <= 1) {
            return CreateConfiguredRouteGroup(parentGroup, enabledFeatureConfigurations.ToArray());
        }

        if (configuredFeatureCount == 0 || hasWholeRouteConfiguration) {
            return parentGroup;
        }

        if (configuredFeatureCount == 1) {
            return CreateConfiguredRouteGroup(parentGroup, enabledFeatureConfigurations.ToArray());
        }

        throw new InvalidOperationException(
            $"The shared route '{routeDisplayName}' can serve multiple endpoint feature groups ({string.Join(", ", enabledFeatureNames)}). " +
            $"Multiple per-feature route-group configurations ({string.Join(", ", configuredConfigurationNames.Distinct(StringComparer.Ordinal))}) do not automatically apply to shared routes. " +
            $"Configure {sharedConfigurationDescription} or {wholeRouteConfigurationDescription} to protect the shared route explicitly.");
    }

    private static void ApplyAuthorizationOptions(RouteGroupBuilder group, IntegratedS3EndpointAuthorizationOptions authorizationOptions)
    {
        ArgumentNullException.ThrowIfNull(group);
        ArgumentNullException.ThrowIfNull(authorizationOptions);

        if (!authorizationOptions.HasConventions) {
            return;
        }

        if (authorizationOptions.AllowAnonymous) {
            group.AllowAnonymous();
            return;
        }

        if (authorizationOptions.RequireAuthorization) {
            group.RequireAuthorization();
        }

        if (authorizationOptions.PolicyNames.Length > 0) {
            group.RequireAuthorization(authorizationOptions.PolicyNames);
        }
    }

    private static void ValidateAuthorizationOptions(string propertyName, IntegratedS3EndpointAuthorizationOptions authorizationOptions)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(authorizationOptions);

        if (!authorizationOptions.HasConventions) {
            return;
        }

        if (authorizationOptions.AllowAnonymous
            && (authorizationOptions.RequireAuthorization || authorizationOptions.PolicyNames.Length > 0)) {
            throw new InvalidOperationException(
                $"{nameof(IntegratedS3EndpointOptions)}.{propertyName} cannot combine " +
                $"{nameof(IntegratedS3EndpointAuthorizationOptions.AllowAnonymous)} with " +
                $"{nameof(IntegratedS3EndpointAuthorizationOptions.RequireAuthorization)} or " +
                $"{nameof(IntegratedS3EndpointAuthorizationOptions.PolicyNames)}.");
        }
    }

    private readonly record struct RouteGroupConfiguration(Action<RouteGroupBuilder>? Apply, string[] SourceNames)
    {
        public static RouteGroupConfiguration None => new(null, []);

        public bool IsConfigured => Apply is not null;
    }

    private static string FormatFeatureRouteGroupConfigurationReference(EndpointFeatureDescriptor feature)
    {
        var genericReference = $"{nameof(IntegratedS3EndpointOptions.SetFeatureRouteGroupConfiguration)}({nameof(IntegratedS3EndpointFeature)}.{feature.Feature}, ...)";
        return $"{feature.CallbackPropertyName} or {genericReference}";
    }

    private readonly record struct EndpointFeatureDescriptor(
        IntegratedS3EndpointFeature Feature,
        string DisplayName,
        string AuthorizationPropertyName,
        string CallbackPropertyName);

    private static async Task<IResult> HandleRootGetAsync(
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IntegratedS3EndpointOptions endpointOptions,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        IStorageServiceDescriptorProvider descriptorProvider,
        CancellationToken cancellationToken)
    {
        if (TryResolveCompatibleRequest(httpContext.Request, options.Value, null, out var resolvedRequest, out var resolutionError)
            && resolvedRequest is not null
            && !string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            if (!endpointOptions.EnableBucketEndpoints) {
                return CreateFeatureDisabledResult();
            }

            if (!string.IsNullOrWhiteSpace(resolutionError)) {
                return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError, resource: null);
            }

            return WrapBucketCorsResult(
                resolvedRequest.BucketName,
                await ExecuteS3CompatibleBucketRequestAsync(resolvedRequest, httpContext, endpointOptions, requestContextAccessor, storageService, cancellationToken));
        }

        if (IsSigV4AuthenticatedRequest(httpContext)) {
            if (!endpointOptions.EnableBucketEndpoints) {
                return CreateFeatureDisabledResult();
            }

            return await ListBucketsS3CompatibleAsync(httpContext, requestContextAccessor, storageService, descriptorProvider, cancellationToken);
        }

        if (!endpointOptions.EnableServiceEndpoints) {
            return CreateFeatureDisabledResult();
        }

        return await GetServiceDocumentAsync(descriptorProvider, cancellationToken);
    }

    private static async Task<Ok<StorageServiceDocument>> GetServiceDocumentAsync(
        IStorageServiceDescriptorProvider descriptorProvider,
        CancellationToken cancellationToken)
    {
        var descriptor = await descriptorProvider.GetServiceDescriptorAsync(cancellationToken);
        return TypedResults.Ok(StorageServiceDocument.FromDescriptor(descriptor));
    }

    private static async Task<Ok<StorageCapabilities>> GetCapabilitiesAsync(
        IStorageCapabilityProvider capabilityProvider,
        CancellationToken cancellationToken)
    {
        var capabilities = await capabilityProvider.GetCapabilitiesAsync(cancellationToken);
        return TypedResults.Ok(capabilities);
    }

    private static async Task<Ok<StorageAdminDiagnostics>> GetAdminDiagnosticsAsync(
        IStorageAdminDiagnosticsProvider diagnosticsProvider,
        CancellationToken cancellationToken)
    {
        var diagnostics = await diagnosticsProvider.GetDiagnosticsAsync(cancellationToken);
        return TypedResults.Ok(diagnostics);
    }

    private static async Task<Ok<StorageReplicaRepairEntry[]>> ListOutstandingReplicaRepairsAsync(
        string? replicaBackend,
        IStorageReplicaRepairBacklog repairBacklog,
        CancellationToken cancellationToken)
    {
        var repairs = await repairBacklog.ListOutstandingAsync(replicaBackend, cancellationToken);
        return TypedResults.Ok(repairs.ToArray());
    }

    private static async Task<IResult> CreateObjectPresignAsync(
        StoragePresignRequest request,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStoragePresignService presignService,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        try {
            var result = await ExecuteWithRequestContextAsync(
                httpContext,
                requestContextAccessor,
                innerCancellationToken => presignService.PresignObjectAsync(httpContext.User, request, innerCancellationToken).AsTask(),
                cancellationToken);

            return result.IsSuccess
                ? TypedResults.Ok(result.Value)
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildResource(request.BucketName, request.Key));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildResource(request.BucketName, request.Key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                exception.Message,
                BuildResource(request.BucketName, request.Key),
                request.BucketName,
                request.Key);
        }
    }

    private static async Task<Ok<BucketInfo[]>> ListBucketsAsync(
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
            try {
                var buckets = await storageService.ListBucketsAsync(innerCancellationToken).ToArrayAsync(innerCancellationToken);
                return TypedResults.Ok(buckets);
            }
            catch (StorageAuthorizationException exception) {
                throw new EndpointStorageAuthorizationException(exception.Error);
            }
        }, cancellationToken);
    }

    private static async Task<IResult> ListBucketsS3CompatibleAsync(
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        IStorageServiceDescriptorProvider descriptorProvider,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                try {
                    var buckets = await storageService.ListBucketsAsync(innerCancellationToken).ToArrayAsync(innerCancellationToken);
                    var descriptor = await descriptorProvider.GetServiceDescriptorAsync(innerCancellationToken);

                    return new XmlContentResult(
                        S3XmlResponseWriter.WriteListAllMyBucketsResult(new S3ListAllMyBucketsResult
                        {
                            Owner = new S3BucketOwner
                            {
                                Id = "integrated-s3",
                                DisplayName = descriptor.ServiceName
                            },
                            Buckets = buckets.Select(static bucket => new S3BucketListEntry
                            {
                                Name = bucket.Name,
                                CreationDateUtc = bucket.CreatedAtUtc
                            }).ToArray()
                        }),
                        StatusCodes.Status200OK,
                        XmlContentType);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error);
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error);
        }
    }

    private static async Task<IResult> CreateBucketAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryParseOptionalWriteCannedAcl(httpContext.Request, BuildObjectResource(bucketName, null), bucketName, key: null, out var cannedAcl, out var aclErrorResult)) {
            return aclErrorResult!;
        }

        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.CreateBucketAsync(new CreateBucketRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken).AsTask(), cancellationToken);

            if (result.IsSuccess && cannedAcl is not null) {
                var aclResult = await compatibilityService.PutBucketAclAsync(new PutBucketAclCompatibilityRequest
                {
                    BucketName = bucketName,
                    CannedAcl = cannedAcl.Value
                }, cancellationToken);
                if (!aclResult.IsSuccess) {
                    return ToErrorResult(httpContext, aclResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
            }

            return result.IsSuccess
                ? TypedResults.Created($"buckets/{bucketName}", result.Value)
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidBucketName", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> HandleS3CompatibleRootAsync(
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IntegratedS3EndpointOptions endpointOptions,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!endpointOptions.EnableBucketEndpoints) {
            return CreateFeatureDisabledResult();
        }

        if (!TryResolveCompatibleRequest(httpContext.Request, options.Value, null, out var resolvedRequest, out var resolutionError)
            || resolvedRequest is null
            || string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError ?? "A bucket name could not be resolved from the request.", resource: null);
        }

        return WrapBucketCorsResult(
            resolvedRequest.BucketName,
            await ExecuteS3CompatibleBucketRequestAsync(resolvedRequest, httpContext, endpointOptions, requestContextAccessor, storageService, cancellationToken));
    }

    private static async Task<IResult> HandleS3CompatiblePathAsync(
        string s3Path,
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IntegratedS3EndpointOptions endpointOptions,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(s3Path)) {
            return CreateFeatureDisabledResult();
        }

        if (!TryResolveCompatibleRequest(httpContext.Request, options.Value, s3Path, out var resolvedRequest, out var resolutionError)
            || resolvedRequest is null
            || string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError ?? "A bucket name could not be resolved from the request.", resource: null);
        }

        if (resolvedRequest.Key is null) {
            if (!endpointOptions.EnableBucketEndpoints) {
                return CreateFeatureDisabledResult();
            }

            return WrapBucketCorsResult(
                resolvedRequest.BucketName,
                await ExecuteS3CompatibleBucketRequestAsync(resolvedRequest, httpContext, endpointOptions, requestContextAccessor, storageService, cancellationToken));
        }

        return WrapBucketCorsResult(
            resolvedRequest.BucketName,
            await ExecuteS3CompatibleObjectRequestAsync(resolvedRequest, httpContext, endpointOptions, requestContextAccessor, storageService, cancellationToken));
    }

    private static async Task<IResult> HandleS3CompatibleRootOptionsAsync(
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IntegratedS3EndpointOptions endpointOptions,
        CancellationToken cancellationToken)
    {
        if (!endpointOptions.EnableBucketEndpoints) {
            return CreateFeatureDisabledResult();
        }

        if (!TryResolveCompatibleRequest(httpContext.Request, options.Value, null, out var resolvedRequest, out var resolutionError)
            || resolvedRequest is null
            || string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError ?? "A bucket name could not be resolved from the request.", resource: null);
        }

        return await HandleBucketCorsPreflightAsync(resolvedRequest.BucketName, httpContext, cancellationToken);
    }

    private static async Task<IResult> HandleS3CompatiblePathOptionsAsync(
        string s3Path,
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IntegratedS3EndpointOptions endpointOptions,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(s3Path)) {
            return CreateFeatureDisabledResult();
        }

        if (!TryResolveCompatibleRequest(httpContext.Request, options.Value, s3Path, out var resolvedRequest, out var resolutionError)
            || resolvedRequest is null
            || string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError ?? "A bucket name could not be resolved from the request.", resource: null);
        }

        if (resolvedRequest.Key is null) {
            if (!endpointOptions.EnableBucketEndpoints) {
                return CreateFeatureDisabledResult();
            }

            return await HandleBucketCorsPreflightCoreAsync(resolvedRequest.BucketName, key: null, httpContext, cancellationToken);
        }

        if (!endpointOptions.EnableObjectEndpoints && !IsMultipartRequest(httpContext.Request)) {
            return CreateFeatureDisabledResult();
        }

        return await HandleBucketCorsPreflightCoreAsync(resolvedRequest.BucketName, resolvedRequest.Key, httpContext, cancellationToken);
    }

    private static Task<IResult> HandleObjectCorsPreflightAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        return HandleBucketCorsPreflightCoreAsync(bucketName, key, httpContext, cancellationToken);
    }

    private static Task<IResult> HandleBucketCorsPreflightAsync(
        string bucketName,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        return HandleBucketCorsPreflightCoreAsync(bucketName, key: null, httpContext, cancellationToken);
    }

    private static async Task<IResult> HandleBucketCorsPreflightCoreAsync(
        string bucketName,
        string? key,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        var origin = httpContext.Request.Headers[OriginHeaderName].ToString();
        if (string.IsNullOrWhiteSpace(origin)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", "The Origin header is required for CORS preflight requests.", BuildObjectResource(bucketName, key), bucketName, key);
        }

        var requestedMethod = httpContext.Request.Headers[AccessControlRequestMethodHeaderName].ToString();
        if (string.IsNullOrWhiteSpace(requestedMethod)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", "The Access-Control-Request-Method header is required for CORS preflight requests.", BuildObjectResource(bucketName, key), bucketName, key);
        }

        AppendVaryHeader(httpContext.Response, OriginHeaderName, AccessControlRequestMethodHeaderName, AccessControlRequestHeadersHeaderName);

        var requestedHeaders = ParseAccessControlRequestHeaders(httpContext.Request.Headers[AccessControlRequestHeadersHeaderName].ToString());
        var runtimeService = httpContext.RequestServices.GetRequiredService<BucketCorsRuntimeService>();
        var result = await runtimeService.GetPreflightResponseAsync(bucketName, origin, requestedMethod, requestedHeaders, cancellationToken);
        if (!result.IsSuccess) {
            return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }

        if (result.Value is null) {
            return ToErrorResult(httpContext, StatusCodes.Status403Forbidden, "AccessDenied", "This CORS request is not allowed.", BuildObjectResource(bucketName, key), bucketName, key);
        }

        ApplyBucketCorsPreflightHeaders(httpContext.Response, result.Value);
        return TypedResults.StatusCode(StatusCodes.Status200OK);
    }

    private static async Task<IResult> HeadBucketAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor,
                async innerCancellationToken => {
                    var headResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                    if (!headResult.IsSuccess) {
                        return ToErrorResult(httpContext, headResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                    }

                    // Best-effort: add x-amz-bucket-region from GetBucketLocation
                    try {
                        var locationResult = await storageService.GetBucketLocationAsync(bucketName, innerCancellationToken);
                        if (locationResult.IsSuccess && !string.IsNullOrWhiteSpace(locationResult.Value?.LocationConstraint)) {
                            httpContext.Response.Headers[BucketRegionHeaderName] = locationResult.Value!.LocationConstraint;
                        }
                    }
                    catch {
                        // Region header is informational; don't fail HeadBucket if location lookup fails
                    }

                    return (IResult)TypedResults.Ok();
                },
                cancellationToken);
            return result;
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidBucketName", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> DeleteBucketAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.DeleteBucketAsync(new DeleteBucketRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken).AsTask(), cancellationToken);

            return result.IsSuccess
                ? TypedResults.NoContent()
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidBucketName", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> ListObjectsAsync(
        string bucketName,
        string? prefix,
        string? continuationToken,
        int? pageSize,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                if (pageSize is <= 0) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "Page size must be greater than zero.", BuildObjectResource(bucketName, null), bucketName);
                }

                var requestedPageSize = pageSize;
                var fetchPageSize = requestedPageSize switch
                {
                    null => null,
                    int.MaxValue => int.MaxValue,
                    _ => requestedPageSize + 1
                };

                try {
                    var objects = await storageService.ListObjectsAsync(new ListObjectsRequest
                    {
                        BucketName = bucketName,
                        Prefix = prefix,
                        ContinuationToken = continuationToken,
                        PageSize = fetchPageSize
                    }, innerCancellationToken).ToArrayAsync(innerCancellationToken);

                    if (requestedPageSize is null || objects.Length <= requestedPageSize.Value) {
                        httpContext.Response.Headers.Remove(ContinuationTokenHeaderName);
                        return TypedResults.Ok(objects);
                    }

                    var page = objects.Take(requestedPageSize.Value).ToArray();
                    httpContext.Response.Headers[ContinuationTokenHeaderName] = page[^1].Key;

                    return TypedResults.Ok(page);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> PutObjectAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        HttpRequest request,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryParseOptionalWriteObjectAcl(request, BuildObjectResource(bucketName, key), bucketName, key, out var objectAcl, out var aclErrorResult)) {
            return aclErrorResult!;
        }

        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();
        try {
            var preparedBody = await PrepareRequestBodyAsync(request, cancellationToken);
            try {
                return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                    if (TryGetCopySource(request, out var copySource, out var copySourceError)) {
                        if (copySourceError is not null) {
                            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", copySourceError, BuildObjectResource(bucketName, key), bucketName, key);
                        }

                        if (!TryParseObjectServerSideEncryptionSettings(request, allowManagedRequestHeaders: true, BuildObjectResource(bucketName, key), bucketName, key, out var copyServerSideEncryption, out var copyServerSideEncryptionErrorResult)) {
                            return copyServerSideEncryptionErrorResult!;
                        }

                        var metadataDirective = ParseCopyObjectMetadataDirective(request.Headers[MetadataDirectiveHeaderName].ToString());
                        if (!TryParseTaggingDirective(request, BuildObjectResource(bucketName, key), bucketName, key, out var taggingDirective, out var taggingDirectiveErrorResult)) {
                            return taggingDirectiveErrorResult!;
                        }

                        if (!TryParseTaggingHeader(request, BuildObjectResource(bucketName, key), bucketName, key, out var copyTags, out var copyTagsErrorResult)) {
                            return copyTagsErrorResult!;
                        }

                        if (!TryParseRequestChecksums(request, preparedBody, requireChecksumValueForDeclaredAlgorithm: false, out var copyChecksumAlgorithm, out var requestedCopyChecksums, out var copyChecksumErrorResult)) {
                            return copyChecksumErrorResult!;
                        }

                        var (sourceCustomerEncryption, sourceCustomerEncryptionError) = TryParseCopySourceCustomerEncryptionSettings(request, BuildObjectResource(bucketName, key), bucketName, key);
                        if (sourceCustomerEncryptionError is not null) {
                            return sourceCustomerEncryptionError;
                        }

                        var (destinationCustomerEncryption, destinationCustomerEncryptionError) = TryParseCustomerEncryptionSettings(request, BuildObjectResource(bucketName, key), bucketName, key);
                        if (destinationCustomerEncryptionError is not null) {
                            return destinationCustomerEncryptionError;
                        }

                        var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
                        {
                            SourceBucketName = copySource!.BucketName,
                            SourceKey = copySource.Key,
                            SourceVersionId = copySource.VersionId,
                            DestinationBucketName = bucketName,
                            DestinationKey = key,
                            SourceIfMatchETag = request.Headers[CopySourceIfMatchHeaderName].ToString(),
                            SourceIfNoneMatchETag = request.Headers[CopySourceIfNoneMatchHeaderName].ToString(),
                            SourceIfModifiedSinceUtc = ParseOptionalHttpDateHeader(request.Headers[CopySourceIfModifiedSinceHeaderName].ToString()),
                            SourceIfUnmodifiedSinceUtc = ParseOptionalHttpDateHeader(request.Headers[CopySourceIfUnmodifiedSinceHeaderName].ToString()),
                            MetadataDirective = metadataDirective,
                            ContentType = metadataDirective == CopyObjectMetadataDirective.Replace ? request.ContentType : null,
                            CacheControl = metadataDirective == CopyObjectMetadataDirective.Replace ? GetOptionalHeaderValue(request.Headers[HeaderNames.CacheControl].ToString()) : null,
                            ContentDisposition = metadataDirective == CopyObjectMetadataDirective.Replace ? GetOptionalHeaderValue(request.Headers[HeaderNames.ContentDisposition].ToString()) : null,
                            ContentEncoding = metadataDirective == CopyObjectMetadataDirective.Replace ? GetOptionalHeaderValue(request.Headers[HeaderNames.ContentEncoding].ToString()) : null,
                            ContentLanguage = metadataDirective == CopyObjectMetadataDirective.Replace ? GetOptionalHeaderValue(request.Headers[HeaderNames.ContentLanguage].ToString()) : null,
                            ExpiresUtc = metadataDirective == CopyObjectMetadataDirective.Replace ? ParseOptionalHttpDateHeader(request.Headers[HeaderNames.Expires].ToString()) : null,
                            Metadata = metadataDirective == CopyObjectMetadataDirective.Replace ? ParseObjectMetadataHeaders(request.Headers) : null,
                            TaggingDirective = taggingDirective,
                            Tags = taggingDirective == ObjectTaggingDirective.Replace ? copyTags : null,
                            ChecksumAlgorithm = copyChecksumAlgorithm,
                            Checksums = requestedCopyChecksums,
                            DestinationServerSideEncryption = copyServerSideEncryption,
                            SourceCustomerEncryption = sourceCustomerEncryption,
                            DestinationCustomerEncryption = destinationCustomerEncryption,
                            StorageClass = GetOptionalHeaderValue(request.Headers[StorageClassHeaderName].ToString())
                        }, innerCancellationToken);

                        if (!copyResult.IsSuccess) {
                            return ToErrorResult(
                                httpContext,
                                copyResult.Error,
                                resourceOverride: BuildObjectResource(bucketName, key),
                                explicitVersionId: copySource.VersionId);
                        }

                        var aclApplyError = await ApplyRequestedObjectAclAsync(httpContext, compatibilityService, bucketName, key, objectAcl, innerCancellationToken);
                        return aclApplyError ?? ToCopyObjectResult(httpContext, copyResult.Value!, copySource.VersionId);
                    }

                    if (!TryParseRequestChecksums(request, preparedBody, requireChecksumValueForDeclaredAlgorithm: true, out var checksumAlgorithm, out var requestedChecksums, out var checksumErrorResult)) {
                        return checksumErrorResult!;
                    }

                    if (!TryParseTaggingHeader(request, BuildObjectResource(bucketName, key), bucketName, key, out var tags, out var taggingErrorResult)) {
                        return taggingErrorResult!;
                    }

                    var metadata = ParseObjectMetadataHeaders(request.Headers);

                    if (!TryParseObjectServerSideEncryptionSettings(request, allowManagedRequestHeaders: true, BuildObjectResource(bucketName, key), bucketName, key, out var serverSideEncryption, out var serverSideEncryptionErrorResult)) {
                        return serverSideEncryptionErrorResult!;
                    }

                    var (customerEncryption, customerEncryptionError) = TryParseCustomerEncryptionSettings(request, BuildObjectResource(bucketName, key), bucketName, key);
                    if (customerEncryptionError is not null) {
                        return customerEncryptionError;
                    }

                    if (serverSideEncryption is not null && customerEncryption is not null) {
                        return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument",
                            "Server-side encryption with a customer-provided key (SSE-C) and managed server-side encryption (SSE-S3/SSE-KMS) are mutually exclusive.",
                            BuildObjectResource(bucketName, key), bucketName, key);
                    }

                    var result = await storageService.PutObjectAsync(new PutObjectRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        Content = preparedBody.Content,
                        ContentLength = preparedBody.ContentLength,
                        ContentType = request.ContentType,
                        CacheControl = GetOptionalHeaderValue(request.Headers[HeaderNames.CacheControl].ToString()),
                        ContentDisposition = GetOptionalHeaderValue(request.Headers[HeaderNames.ContentDisposition].ToString()),
                        ContentEncoding = GetOptionalHeaderValue(request.Headers[HeaderNames.ContentEncoding].ToString()),
                        ContentLanguage = GetOptionalHeaderValue(request.Headers[HeaderNames.ContentLanguage].ToString()),
                        ExpiresUtc = ParseOptionalHttpDateHeader(request.Headers[HeaderNames.Expires].ToString()),
                        Metadata = metadata,
                        Tags = tags,
                        Checksums = requestedChecksums,
                        ServerSideEncryption = serverSideEncryption,
                        CustomerEncryption = customerEncryption,
                        StorageClass = GetOptionalHeaderValue(request.Headers[StorageClassHeaderName].ToString()),
                        IfMatchETag = GetOptionalHeaderValue(request.Headers.IfMatch.ToString()),
                        IfNoneMatchETag = GetOptionalHeaderValue(request.Headers.IfNoneMatch.ToString())
                    }, innerCancellationToken);

                    if (!result.IsSuccess) {
                        return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                    }

                    var aclError = await ApplyRequestedObjectAclAsync(httpContext, compatibilityService, bucketName, key, objectAcl, innerCancellationToken);
                    if (aclError is not null) {
                        return aclError;
                    }

                    if (result.Value is not null) {
                        ApplyObjectResultHeaders(httpContext.Response, result.Value);
                        ApplyChecksumAlgorithmHeader(httpContext.Response, checksumAlgorithm);
                    }

                    return TypedResults.Ok(result.Value);
                }, cancellationToken);
            }
            finally {
                await preparedBody.DisposeAsync();
            }
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> GetObjectAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        HttpRequest request,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var headers = request.GetTypedHeaders();

                if (!TryParseObjectServerSideEncryptionSettings(request, allowManagedRequestHeaders: false, BuildObjectResource(bucketName, key), bucketName, key, out _, out var serverSideEncryptionErrorResult)) {
                    return serverSideEncryptionErrorResult!;
                }

                var versionId = ParseVersionId(request);

                var (customerEncryption, customerEncryptionError) = TryParseCustomerEncryptionSettings(request, BuildObjectResource(bucketName, key), bucketName, key);
                if (customerEncryptionError is not null) {
                    return customerEncryptionError;
                }

                var rawRange = request.Headers.Range.ToString();
                var multipleRanges = ParseMultipleRangeHeaders(rawRange);

                if (multipleRanges.Length > 1) {
                    return new MultiRangeStreamObjectResult(
                        storageService, bucketName, key, versionId,
                        request.Headers.IfMatch.ToString(),
                        request.Headers.IfNoneMatch.ToString(),
                        headers.IfModifiedSince,
                        headers.IfUnmodifiedSince,
                        customerEncryption,
                        multipleRanges,
                        IsChecksumModeEnabled(request));
                }

                var result = await storageService.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = versionId,
                    Range = multipleRanges.Length == 1 ? multipleRanges[0] : null,
                    IfMatchETag = request.Headers.IfMatch.ToString(),
                    IfNoneMatchETag = request.Headers.IfNoneMatch.ToString(),
                    IfModifiedSinceUtc = headers.IfModifiedSince,
                    IfUnmodifiedSinceUtc = headers.IfUnmodifiedSince,
                    CustomerEncryption = customerEncryption
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key), explicitVersionId: versionId);
                }

                return new StreamObjectResult(result.Value!);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status416RangeNotSatisfiable, "InvalidRange", exception.Message, BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> HeadObjectAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var headers = httpContext.Request.GetTypedHeaders();

                if (!TryParseObjectServerSideEncryptionSettings(httpContext.Request, allowManagedRequestHeaders: false, BuildObjectResource(bucketName, key), bucketName, key, out _, out var serverSideEncryptionErrorResult)) {
                    return serverSideEncryptionErrorResult!;
                }

                var versionId = ParseVersionId(httpContext.Request);

                var (customerEncryption, customerEncryptionError) = TryParseCustomerEncryptionSettings(httpContext.Request, BuildObjectResource(bucketName, key), bucketName, key);
                if (customerEncryptionError is not null) {
                    return customerEncryptionError;
                }

                var result = await storageService.HeadObjectAsync(new HeadObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = versionId,
                    IfMatchETag = httpContext.Request.Headers.IfMatch.ToString(),
                    IfNoneMatchETag = httpContext.Request.Headers.IfNoneMatch.ToString(),
                    IfModifiedSinceUtc = headers.IfModifiedSince,
                    IfUnmodifiedSinceUtc = headers.IfUnmodifiedSince,
                    CustomerEncryption = customerEncryption
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key), explicitVersionId: versionId);
                }

                var objectInfo = result.Value!;
                ApplyObjectHeaders(httpContext.Response, objectInfo);
                ApplyObjectTaggingCountHeader(httpContext.Response, objectInfo);
                httpContext.Response.Headers.AcceptRanges = "bytes";

                if (!MatchesIfMatch(httpContext.Request.Headers.IfMatch.ToString(), objectInfo.ETag)) {
                    return TypedResults.StatusCode(StatusCodes.Status412PreconditionFailed);
                }

                if (string.IsNullOrWhiteSpace(httpContext.Request.Headers.IfMatch)
                    && headers.IfUnmodifiedSince is { } ifUnmodifiedSinceUtc
                    && WasModifiedAfter(objectInfo.LastModifiedUtc, ifUnmodifiedSinceUtc)) {
                    return TypedResults.StatusCode(StatusCodes.Status412PreconditionFailed);
                }

                if (MatchesAnyETag(httpContext.Request.Headers.IfNoneMatch.ToString(), objectInfo.ETag)) {
                    return TypedResults.StatusCode(StatusCodes.Status304NotModified);
                }

                if (string.IsNullOrWhiteSpace(httpContext.Request.Headers.IfNoneMatch)
                    && headers.IfModifiedSince is { } ifModifiedSinceUtc
                    && !WasModifiedAfter(objectInfo.LastModifiedUtc, ifModifiedSinceUtc)) {
                    return TypedResults.StatusCode(StatusCodes.Status304NotModified);
                }

                httpContext.Response.ContentLength = objectInfo.ContentLength;
                httpContext.Response.ContentType = objectInfo.ContentType ?? "application/octet-stream";

                return TypedResults.Ok();
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> DeleteObjectAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.DeleteObjectAsync(new DeleteObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request)
                }, innerCancellationToken).AsTask(), cancellationToken);

            if (result.IsSuccess && result.Value is not null) {
                ApplyDeleteObjectHeaders(httpContext.Response, result.Value);
            }

            return result.IsSuccess
                ? TypedResults.NoContent()
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> ExecuteS3CompatibleBucketRequestAsync(
        ResolvedS3Request resolvedRequest,
        HttpContext httpContext,
        IntegratedS3EndpointOptions endpointOptions,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (IsMultipartRequest(httpContext.Request)
            && !endpointOptions.EnableMultipartEndpoints) {
            return CreateFeatureDisabledResult();
        }

        if (!TryValidateBucketRequestSubresources(httpContext.Request, out var validationErrorCode, out var validationMessage, out var validationStatusCode)) {
            return ToErrorResult(httpContext, validationStatusCode, validationErrorCode!, validationMessage!, resolvedRequest.CanonicalResourcePath, resolvedRequest.BucketName);
        }

        return httpContext.Request.Method switch
        {
            "GET" when httpContext.Request.Query.ContainsKey(LocationQueryParameterName) => await GetBucketLocationAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(AclQueryParameterName) => await GetBucketAclAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(CorsQueryParameterName) => await GetBucketCorsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(PolicyQueryParameterName) => await GetBucketPolicyAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(VersioningQueryParameterName) => await GetBucketVersioningAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(EncryptionQueryParameterName) => await GetBucketDefaultEncryptionAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(TaggingQueryParameterName) => await GetBucketTaggingAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(LoggingQueryParameterName) => await GetBucketLoggingAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(WebsiteQueryParameterName) => await GetBucketWebsiteAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(RequestPaymentQueryParameterName) => await GetBucketRequestPaymentAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(AccelerateQueryParameterName) => await GetBucketAccelerateAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(LifecycleQueryParameterName) => await GetBucketLifecycleAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(ReplicationQueryParameterName) => await GetBucketReplicationAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(NotificationQueryParameterName) => await GetBucketNotificationAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(ObjectLockQueryParameterName) => await GetBucketObjectLockAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(AnalyticsQueryParameterName) && !httpContext.Request.Query.ContainsKey(IdQueryParameterName) => await ListBucketAnalyticsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(MetricsQueryParameterName) && !httpContext.Request.Query.ContainsKey(IdQueryParameterName) => await ListBucketMetricsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(InventoryQueryParameterName) && !httpContext.Request.Query.ContainsKey(IdQueryParameterName) => await ListBucketInventoryAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(IntelligentTieringQueryParameterName) && !httpContext.Request.Query.ContainsKey(IdQueryParameterName) => await ListBucketIntelligentTieringAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(AnalyticsQueryParameterName) => await GetBucketAnalyticsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(MetricsQueryParameterName) => await GetBucketMetricsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(InventoryQueryParameterName) => await GetBucketInventoryAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(IntelligentTieringQueryParameterName) => await GetBucketIntelligentTieringAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(UploadsQueryParameterName) => await ListMultipartUploadsAsync(
                resolvedRequest.BucketName,
                ParsePrefix(httpContext.Request),
                ParseDelimiter(httpContext.Request),
                ParseKeyMarker(httpContext.Request),
                ParseUploadIdMarker(httpContext.Request),
                httpContext,
                requestContextAccessor,
                storageService,
                cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(VersionsQueryParameterName) => await ListObjectVersionsAsync(
                resolvedRequest.BucketName,
                ParsePrefix(httpContext.Request),
                ParseDelimiter(httpContext.Request),
                ParseKeyMarker(httpContext.Request),
                ParseVersionIdMarker(httpContext.Request),
                ParseMaxKeys(httpContext.Request),
                ParseEncodingType(httpContext.Request),
                httpContext,
                requestContextAccessor,
                storageService,
                cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(AclQueryParameterName) => await PutBucketAclAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(CorsQueryParameterName) => await PutBucketCorsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(PolicyQueryParameterName) => await PutBucketPolicyAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(VersioningQueryParameterName) => await PutBucketVersioningAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(EncryptionQueryParameterName) => await PutBucketDefaultEncryptionAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(TaggingQueryParameterName) => await PutBucketTaggingAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(LoggingQueryParameterName) => await PutBucketLoggingAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(WebsiteQueryParameterName) => await PutBucketWebsiteAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(RequestPaymentQueryParameterName) => await PutBucketRequestPaymentAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(AccelerateQueryParameterName) => await PutBucketAccelerateAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(LifecycleQueryParameterName) => await PutBucketLifecycleAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(ReplicationQueryParameterName) => await PutBucketReplicationAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(NotificationQueryParameterName) => await PutBucketNotificationAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(ObjectLockQueryParameterName) => await PutBucketObjectLockAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(AnalyticsQueryParameterName) => await PutBucketAnalyticsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(MetricsQueryParameterName) => await PutBucketMetricsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(InventoryQueryParameterName) => await PutBucketInventoryAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(IntelligentTieringQueryParameterName) => await PutBucketIntelligentTieringAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(CorsQueryParameterName) => await DeleteBucketCorsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(PolicyQueryParameterName) => await DeleteBucketPolicyAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(EncryptionQueryParameterName) => await DeleteBucketDefaultEncryptionAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(TaggingQueryParameterName) => await DeleteBucketTaggingAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(WebsiteQueryParameterName) => await DeleteBucketWebsiteAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(LifecycleQueryParameterName) => await DeleteBucketLifecycleAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(ReplicationQueryParameterName) => await DeleteBucketReplicationAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(AnalyticsQueryParameterName) => await DeleteBucketAnalyticsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(MetricsQueryParameterName) => await DeleteBucketMetricsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(InventoryQueryParameterName) => await DeleteBucketInventoryAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(IntelligentTieringQueryParameterName) => await DeleteBucketIntelligentTieringAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            _ => httpContext.Request.Method switch
        {
            "GET" => IsListObjectsV2Request(httpContext.Request)
                ? await ListObjectsV2Async(
                    resolvedRequest.BucketName,
                    ParsePrefix(httpContext.Request),
                    ParseDelimiter(httpContext.Request),
                    ParseStartAfter(httpContext.Request),
                    ParseContinuationToken(httpContext.Request),
                    ParseMaxKeys(httpContext.Request),
                    ParseEncodingType(httpContext.Request),
                    ParseFetchOwner(httpContext.Request),
                    httpContext,
                    requestContextAccessor,
                    storageService,
                    cancellationToken)
                : await ListObjectsV1Async(
                    resolvedRequest.BucketName,
                    ParsePrefix(httpContext.Request),
                    ParseDelimiter(httpContext.Request),
                    ParseMarker(httpContext.Request),
                    ParseMaxKeys(httpContext.Request),
                    ParseEncodingType(httpContext.Request),
                    httpContext,
                    requestContextAccessor,
                    storageService,
                    cancellationToken),
            "PUT" => await CreateBucketS3CompatibleAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "HEAD" => await HeadBucketAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" => await DeleteBucketAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when httpContext.Request.Query.ContainsKey(DeleteQueryParameterName) => await DeleteObjectsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when IsPostObjectFormUpload(httpContext.Request) => await PostObjectAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" => ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", "Unsupported bucket subresource request.", resolvedRequest.CanonicalResourcePath, resolvedRequest.BucketName),
            _ => TypedResults.StatusCode(StatusCodes.Status405MethodNotAllowed)
        }};
    }

    private static async Task<IResult> GetBucketLocationAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketLocationAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteBucketLocation(new S3BucketLocationResponse
                    {
                        LocationConstraint = result.Value!.LocationConstraint
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> GetBucketVersioningAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketVersioningAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var status = result.Value!.Status switch
                {
                    BucketVersioningStatus.Enabled => "Enabled",
                    BucketVersioningStatus.Suspended => "Suspended",
                    _ => null
                };

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteBucketVersioningConfiguration(new S3BucketVersioningConfiguration
                    {
                        Status = status
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> GetBucketAclAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        CancellationToken cancellationToken)
    {
        var authorizationService = httpContext.RequestServices.GetRequiredService<IIntegratedS3AuthorizationService>();
        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();
        var descriptorProvider = httpContext.RequestServices.GetRequiredService<IStorageServiceDescriptorProvider>();

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                await AuthorizeCompatibilityOperationAsync(
                    httpContext,
                    authorizationService,
                    new StorageAuthorizationRequest
                    {
                        Operation = StorageOperationType.GetBucketAcl,
                        BucketName = bucketName
                    },
                    innerCancellationToken);

                var result = await compatibilityService.GetBucketAclAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var descriptor = await descriptorProvider.GetServiceDescriptorAsync(innerCancellationToken);
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteAccessControlPolicy(CreateAccessControlPolicy(result.Value, descriptor.ServiceName)),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketAclAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        CancellationToken cancellationToken)
    {
        var aclResult = await TryReadAclSubresourceRequestAsync(httpContext, bucketName, key: null, cancellationToken);
        if (aclResult.ErrorResult is not null) {
            return aclResult.ErrorResult;
        }

        var authorizationService = httpContext.RequestServices.GetRequiredService<IIntegratedS3AuthorizationService>();
        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                await AuthorizeCompatibilityOperationAsync(
                    httpContext,
                    authorizationService,
                    new StorageAuthorizationRequest
                    {
                        Operation = StorageOperationType.PutBucketAcl,
                        BucketName = bucketName
                    },
                    innerCancellationToken);

                var result = await compatibilityService.PutBucketAclAsync(new PutBucketAclCompatibilityRequest
                {
                    BucketName = bucketName,
                    CannedAcl = aclResult.CannedAcl!.Value
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> GetBucketPolicyAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        CancellationToken cancellationToken)
    {
        var authorizationService = httpContext.RequestServices.GetRequiredService<IIntegratedS3AuthorizationService>();
        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                await AuthorizeCompatibilityOperationAsync(
                    httpContext,
                    authorizationService,
                    new StorageAuthorizationRequest
                    {
                        Operation = StorageOperationType.GetBucketPolicy,
                        BucketName = bucketName
                    },
                    innerCancellationToken);

                var result = await compatibilityService.GetBucketPolicyAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                if (result.Value is null) {
                    return ToErrorResult(
                        httpContext,
                        StatusCodes.Status404NotFound,
                        "NoSuchBucketPolicy",
                        "The bucket policy does not exist.",
                        BuildObjectResource(bucketName, null),
                        bucketName);
                }

                return TypedResults.Text(result.Value.Document, "application/json");
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketPolicyAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        CancellationToken cancellationToken)
    {
        var policyResult = await TryReadBucketPolicyDocumentAsync(httpContext, bucketName, cancellationToken);
        if (policyResult.ErrorResult is not null) {
            return policyResult.ErrorResult;
        }

        var authorizationService = httpContext.RequestServices.GetRequiredService<IIntegratedS3AuthorizationService>();
        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                await AuthorizeCompatibilityOperationAsync(
                    httpContext,
                    authorizationService,
                    new StorageAuthorizationRequest
                    {
                        Operation = StorageOperationType.PutBucketPolicy,
                        BucketName = bucketName
                    },
                    innerCancellationToken);

                var result = await compatibilityService.PutBucketPolicyAsync(new PutBucketPolicyCompatibilityRequest
                {
                    BucketName = bucketName,
                    Policy = policyResult.Policy!
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketPolicyAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        CancellationToken cancellationToken)
    {
        var authorizationService = httpContext.RequestServices.GetRequiredService<IIntegratedS3AuthorizationService>();
        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                await AuthorizeCompatibilityOperationAsync(
                    httpContext,
                    authorizationService,
                    new StorageAuthorizationRequest
                    {
                        Operation = StorageOperationType.DeleteBucketPolicy,
                        BucketName = bucketName
                    },
                    innerCancellationToken);

                var result = await compatibilityService.DeleteBucketPolicyAsync(bucketName, innerCancellationToken);
                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> GetBucketCorsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketCorsAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteCorsConfiguration(new S3CorsConfiguration
                    {
                        Rules = result.Value!.Rules.Select(ToS3CorsRule).ToArray()
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketCorsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3CorsConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadCorsConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        if (requestBody.Rules.Count > 100) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", "Bucket CORS configurations can contain at most 100 rules.", BuildObjectResource(bucketName, null), bucketName);
        }

        BucketCorsRule[] rules;
        try {
            rules = requestBody.Rules.Select(ToBucketCorsRule).ToArray();
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketCorsAsync(new PutBucketCorsRequest
                {
                    BucketName = bucketName,
                    Rules = rules
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketCorsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketCorsAsync(new DeleteBucketCorsRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> GetBucketDefaultEncryptionAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketDefaultEncryptionAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteBucketEncryptionConfiguration(new S3BucketEncryptionConfiguration
                    {
                        Rules = [ToS3BucketEncryptionRule(result.Value!.Rule)]
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketDefaultEncryptionAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3BucketEncryptionConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadBucketEncryptionConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        if (!TryValidateBucketDefaultEncryptionRequest(requestBody, httpContext, bucketName, out var rule, out var errorResult)) {
            return errorResult!;
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketDefaultEncryptionAsync(new PutBucketDefaultEncryptionRequest
                {
                    BucketName = bucketName,
                    Rule = rule!
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketDefaultEncryptionAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketDefaultEncryptionAsync(new DeleteBucketDefaultEncryptionRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketVersioningAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3BucketVersioningConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadBucketVersioningConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        var requestedStatus = requestBody.Status switch
        {
            null or "" => BucketVersioningStatus.Disabled,
            "Enabled" => BucketVersioningStatus.Enabled,
            "Suspended" => BucketVersioningStatus.Suspended,
            _ => (BucketVersioningStatus?)null
        };

        if (requestedStatus is null) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "Bucket versioning status must be 'Enabled' or 'Suspended'.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketVersioningAsync(new PutBucketVersioningRequest
                {
                    BucketName = bucketName,
                    Status = requestedStatus.Value
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static bool TryValidateBucketDefaultEncryptionRequest(
        S3BucketEncryptionConfiguration requestBody,
        HttpContext httpContext,
        string bucketName,
        out BucketDefaultEncryptionRule? rule,
        out IResult? errorResult)
    {
        ArgumentNullException.ThrowIfNull(requestBody);

        if (requestBody.Rules.Count != 1) {
            rule = null;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                "Bucket default encryption requests must contain exactly one 'Rule' element.",
                BuildObjectResource(bucketName, null),
                bucketName);
            return false;
        }

        var requestRule = requestBody.Rules[0];

        var rawAlgorithm = requestRule.DefaultEncryption.SseAlgorithm?.Trim();
        if (!TryNormalizeServerSideEncryptionAlgorithm(rawAlgorithm ?? string.Empty, out var algorithm)) {
            rule = null;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"Server-side encryption algorithm '{rawAlgorithm}' is not implemented.",
                BuildObjectResource(bucketName, null),
                bucketName);
            return false;
        }

        var keyIdElementWasPresent = requestRule.DefaultEncryption.KmsMasterKeyId is not null;
        var keyId = string.IsNullOrWhiteSpace(requestRule.DefaultEncryption.KmsMasterKeyId)
            ? null
            : requestRule.DefaultEncryption.KmsMasterKeyId.Trim();

        if (keyIdElementWasPresent && keyId is null) {
            rule = null;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                "The 'KMSMasterKeyID' element must not be empty when present.",
                BuildObjectResource(bucketName, null),
                bucketName);
            return false;
        }

        if (algorithm == ObjectServerSideEncryptionAlgorithm.Aes256 && keyId is not null) {
            rule = null;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                "The 'KMSMasterKeyID' element is only supported when 'SSEAlgorithm' is 'aws:kms' or 'aws:kms:dsse'.",
                BuildObjectResource(bucketName, null),
                bucketName);
            return false;
        }

        rule = new BucketDefaultEncryptionRule
        {
            Algorithm = algorithm,
            KeyId = keyId,
            BucketKeyEnabled = requestRule.BucketKeyEnabled == true
        };
        errorResult = null;
        return true;
    }

    private static async Task<IResult> CreateBucketS3CompatibleAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryParseOptionalWriteCannedAcl(httpContext.Request, BuildObjectResource(bucketName, null), bucketName, key: null, out var cannedAcl, out var aclErrorResult)) {
            return aclErrorResult!;
        }

        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.CreateBucketAsync(new CreateBucketRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken).AsTask(), cancellationToken);

            if (result.IsSuccess && cannedAcl is not null) {
                var aclResult = await compatibilityService.PutBucketAclAsync(new PutBucketAclCompatibilityRequest
                {
                    BucketName = bucketName,
                    CannedAcl = cannedAcl.Value
                }, cancellationToken);
                if (!aclResult.IsSuccess) {
                    return ToErrorResult(httpContext, aclResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
            }

            return result.IsSuccess
                ? TypedResults.Ok()
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidBucketName", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> ExecuteS3CompatibleObjectRequestAsync(
        ResolvedS3Request resolvedRequest,
        HttpContext httpContext,
        IntegratedS3EndpointOptions endpointOptions,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var key = resolvedRequest.Key!;

        if (IsMultipartRequest(httpContext.Request)) {
            if (!endpointOptions.EnableMultipartEndpoints) {
                return CreateFeatureDisabledResult();
            }
        }
        else if (!endpointOptions.EnableObjectEndpoints) {
            return CreateFeatureDisabledResult();
        }

        if (!TryValidateObjectRequestSubresources(httpContext.Request, out var validationErrorCode, out var validationMessage, out var validationStatusCode)) {
            return ToErrorResult(httpContext, validationStatusCode, validationErrorCode!, validationMessage!, resolvedRequest.CanonicalResourcePath, resolvedRequest.BucketName, key);
        }

        return httpContext.Request.Method switch
        {
            "GET" when httpContext.Request.Query.ContainsKey(AclQueryParameterName) => await GetObjectAclAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(TaggingQueryParameterName) => await GetObjectTaggingAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(RetentionQueryParameterName) => await GetObjectRetentionAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(LegalHoldQueryParameterName) => await GetObjectLegalHoldAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(AttributesQueryParameterName) => await GetObjectAttributesAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when TryGetMultipartUploadId(httpContext.Request, out _, out _) => await ListMultipartPartsAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(AclQueryParameterName) => await PutObjectAclAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(TaggingQueryParameterName) => await PutObjectTaggingAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(RetentionQueryParameterName) => await PutObjectRetentionAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(LegalHoldQueryParameterName) => await PutObjectLegalHoldAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(TaggingQueryParameterName) => await DeleteObjectTaggingAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when httpContext.Request.Query.ContainsKey(RestoreQueryParameterName) => await RestoreObjectAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when httpContext.Request.Query.ContainsKey(SelectQueryParameterName) => await SelectObjectContentAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when httpContext.Request.Query.ContainsKey(UploadsQueryParameterName) => await InitiateMultipartUploadAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when TryGetMultipartUploadId(httpContext.Request, out _, out _) && httpContext.Request.Query.ContainsKey(PartNumberQueryParameterName) && TryGetCopySource(httpContext.Request, out _, out _) => await UploadPartCopyAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when TryGetMultipartUploadId(httpContext.Request, out _, out _) && httpContext.Request.Query.ContainsKey(PartNumberQueryParameterName) => await UploadMultipartPartAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when TryGetMultipartUploadId(httpContext.Request, out _, out _) => await CompleteMultipartUploadAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when TryGetMultipartUploadId(httpContext.Request, out _, out _) => await AbortMultipartUploadAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" => await GetObjectAsync(resolvedRequest.BucketName, key, httpContext, httpContext.Request, requestContextAccessor, storageService, cancellationToken),
            "PUT" => await PutObjectAsync(resolvedRequest.BucketName, key, httpContext, httpContext.Request, requestContextAccessor, storageService, cancellationToken),
            "HEAD" => await HeadObjectAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" => await DeleteObjectAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            _ => TypedResults.StatusCode(StatusCodes.Status405MethodNotAllowed)
        };
    }

    private static async Task<IResult> GetObjectAclAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        CancellationToken cancellationToken)
    {
        var authorizationService = httpContext.RequestServices.GetRequiredService<IIntegratedS3AuthorizationService>();
        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();
        var descriptorProvider = httpContext.RequestServices.GetRequiredService<IStorageServiceDescriptorProvider>();

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                await AuthorizeCompatibilityOperationAsync(
                    httpContext,
                    authorizationService,
                    new StorageAuthorizationRequest
                    {
                        Operation = StorageOperationType.GetObjectAcl,
                        BucketName = bucketName,
                        Key = key
                    },
                    innerCancellationToken);

                var result = await compatibilityService.GetObjectAclStateAsync(bucketName, key, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var acl = result.Value ?? throw new InvalidOperationException("Object ACL compatibility state was not returned.");
                var descriptor = await descriptorProvider.GetServiceDescriptorAsync(innerCancellationToken);
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteAccessControlPolicy(CreateObjectAccessControlPolicy(acl, descriptor.ServiceName)),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> PutObjectAclAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        CancellationToken cancellationToken)
    {
        var aclResult = await TryReadObjectAclSubresourceRequestAsync(httpContext, bucketName, key, cancellationToken);
        if (aclResult.ErrorResult is not null) {
            return aclResult.ErrorResult;
        }

        var authorizationService = httpContext.RequestServices.GetRequiredService<IIntegratedS3AuthorizationService>();
        var compatibilityService = httpContext.RequestServices.GetRequiredService<IStorageAuthorizationCompatibilityService>();

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                await AuthorizeCompatibilityOperationAsync(
                    httpContext,
                    authorizationService,
                    new StorageAuthorizationRequest
                    {
                        Operation = StorageOperationType.PutObjectAcl,
                        BucketName = bucketName,
                        Key = key
                    },
                    innerCancellationToken);

                var result = await compatibilityService.PutObjectAclAsync(new PutObjectAclCompatibilityRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    CannedAcl = aclResult.Acl!.CannedAcl,
                    Acl = aclResult.Acl
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> GetObjectTaggingAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request)
                }, innerCancellationToken);

                if (result.IsSuccess && result.Value is not null) {
                    ApplyObjectTaggingHeaders(httpContext.Response, result.Value);
                }

                return result.IsSuccess
                    ? new XmlContentResult(
                        S3XmlResponseWriter.WriteObjectTagging(new S3ObjectTagging
                        {
                            TagSet = result.Value!.Tags
                                .OrderBy(static pair => pair.Key, StringComparer.Ordinal)
                                .Select(static pair => new S3ObjectTag
                                {
                                    Key = pair.Key,
                                    Value = pair.Value
                                })
                                .ToArray()
                        }),
                        StatusCodes.Status200OK,
                        XmlContentType)
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> PutObjectTaggingAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3ObjectTagging requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadObjectTaggingAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
        }

        var requestedTags = requestBody.TagSet
            .Select(static tag => new KeyValuePair<string, string>(tag.Key, tag.Value))
            .ToArray();
        var tagValidationError = ObjectTagValidation.Validate(requestedTags);
        if (tagValidationError is not null) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidTag", tagValidationError, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request),
                    Tags = requestedTags.ToDictionary(static tag => tag.Key, static tag => tag.Value, StringComparer.Ordinal)
                }, innerCancellationToken);

                if (result.IsSuccess && result.Value is not null) {
                    ApplyObjectTaggingHeaders(httpContext.Response, result.Value);
                }

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> GetObjectRetentionAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetObjectRetentionAsync(new GetObjectRetentionRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request)
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var retention = result.Value ?? throw new InvalidOperationException("Object retention metadata was not returned.");
                ApplyVersionIdHeader(httpContext.Response, retention.VersionId);

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteObjectRetention(new S3ObjectRetention
                    {
                        Mode = ToS3RetentionMode(retention.Mode),
                        RetainUntilDateUtc = retention.RetainUntilDateUtc
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> GetObjectLegalHoldAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetObjectLegalHoldAsync(new GetObjectLegalHoldRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request)
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var legalHold = result.Value ?? throw new InvalidOperationException("Object legal-hold metadata was not returned.");
                ApplyVersionIdHeader(httpContext.Response, legalHold.VersionId);

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteObjectLegalHold(new S3ObjectLegalHold
                    {
                        Status = ToS3LegalHoldStatus(legalHold.Status)
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> GetObjectAttributesAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var objectAttributes = ParseObjectAttributes(httpContext.Request);
                if (objectAttributes.Count == 0) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument",
                        "The x-amz-object-attributes header is required.",
                        BuildObjectResource(bucketName, key), bucketName, key);
                }

                var versionId = ParseVersionId(httpContext.Request);

                var result = await storageService.GetObjectAttributesAsync(new GetObjectAttributesRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = versionId,
                    ObjectAttributes = objectAttributes
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var response = result.Value ?? throw new InvalidOperationException("Object attributes were not returned.");
                ApplyVersionIdHeader(httpContext.Response, response.VersionId);
                if (response.IsDeleteMarker) {
                    httpContext.Response.Headers[DeleteMarkerHeaderName] = "true";
                }
                if (response.LastModifiedUtc.HasValue) {
                    httpContext.Response.Headers["Last-Modified"] = response.LastModifiedUtc.Value.ToString("R");
                }

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteGetObjectAttributesResponse(response),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
    }

    private static List<string> ParseObjectAttributes(HttpRequest request)
    {
        var headerValue = request.Headers[ObjectAttributesHeaderName].ToString();
        if (string.IsNullOrWhiteSpace(headerValue))
            return [];

        return headerValue.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
    }

    private static async Task<IResult> DeleteObjectTaggingAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request)
                }, innerCancellationToken);

                if (result.IsSuccess && result.Value is not null) {
                    ApplyObjectTaggingHeaders(httpContext.Response, result.Value);
                }

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> InitiateMultipartUploadAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var unsupportedAclHeaderName = FindUnsupportedAclGrantHeader(httpContext.Request);
        if (!string.IsNullOrWhiteSpace(unsupportedAclHeaderName)) {
            return ToErrorResult(
                httpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"ACL request header '{unsupportedAclHeaderName}' is not implemented.",
                BuildObjectResource(bucketName, key),
                bucketName,
                key);
        }

        if (httpContext.Request.Headers.ContainsKey(CannedAclHeaderName)) {
            return ToErrorResult(
                httpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"ACL request header '{CannedAclHeaderName}' is not implemented for multipart upload initiation.",
                BuildObjectResource(bucketName, key),
                bucketName,
                key);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                if (!TryParseRequestChecksums(httpContext.Request, preparedBody: null, requireChecksumValueForDeclaredAlgorithm: false, out var checksumAlgorithm, out _, out var checksumErrorResult)) {
                    return checksumErrorResult!;
                }

                if (!TryParseTaggingHeader(httpContext.Request, BuildObjectResource(bucketName, key), bucketName, key, out var tags, out var taggingErrorResult)) {
                    return taggingErrorResult!;
                }

                var metadata = ParseObjectMetadataHeaders(httpContext.Request.Headers);

                if (!TryParseObjectServerSideEncryptionSettings(httpContext.Request, allowManagedRequestHeaders: true, BuildObjectResource(bucketName, key), bucketName, key, out var serverSideEncryption, out var serverSideEncryptionErrorResult)) {
                    return serverSideEncryptionErrorResult!;
                }

                var (customerEncryption, customerEncryptionError) = TryParseCustomerEncryptionSettings(httpContext.Request, BuildObjectResource(bucketName, key), bucketName, key);
                if (customerEncryptionError is not null) {
                    return customerEncryptionError;
                }

                if (serverSideEncryption is not null && customerEncryption is not null) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument",
                        "Server-side encryption with a customer-provided key (SSE-C) and managed server-side encryption (SSE-S3/SSE-KMS) are mutually exclusive.",
                        BuildObjectResource(bucketName, key), bucketName, key);
                }

                var result = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentType = httpContext.Request.ContentType,
                    CacheControl = GetOptionalHeaderValue(httpContext.Request.Headers[HeaderNames.CacheControl].ToString()),
                    ContentDisposition = GetOptionalHeaderValue(httpContext.Request.Headers[HeaderNames.ContentDisposition].ToString()),
                    ContentEncoding = GetOptionalHeaderValue(httpContext.Request.Headers[HeaderNames.ContentEncoding].ToString()),
                    ContentLanguage = GetOptionalHeaderValue(httpContext.Request.Headers[HeaderNames.ContentLanguage].ToString()),
                    ExpiresUtc = ParseOptionalHttpDateHeader(httpContext.Request.Headers[HeaderNames.Expires].ToString()),
                    Metadata = metadata,
                    Tags = tags,
                    ChecksumAlgorithm = checksumAlgorithm,
                    ServerSideEncryption = serverSideEncryption,
                    CustomerEncryption = customerEncryption,
                    StorageClass = GetOptionalHeaderValue(httpContext.Request.Headers[StorageClassHeaderName].ToString())
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                ApplyChecksumAlgorithmHeader(httpContext.Response, result.Value!.ChecksumAlgorithm);
                ApplyServerSideEncryptionHeaders(httpContext.Response, result.Value.ServerSideEncryption);
                EmitCustomerEncryptionResponseHeaders(httpContext.Response, result.Value.CustomerEncryption);
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteInitiateMultipartUploadResult(new S3InitiateMultipartUploadResult
                    {
                        Bucket = bucketName,
                        Key = key,
                        UploadId = result.Value.UploadId,
                        ChecksumAlgorithm = ToS3ChecksumAlgorithmValue(result.Value.ChecksumAlgorithm)
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> UploadMultipartPartAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryGetMultipartUploadId(httpContext.Request, out var uploadId, out var uploadIdError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", uploadIdError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        if (!TryGetPartNumber(httpContext.Request, out var partNumber, out var partNumberError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", partNumberError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            var preparedBody = await PrepareRequestBodyAsync(httpContext.Request, cancellationToken);
            try {
                if (!TryParseRequestChecksums(httpContext.Request, preparedBody, requireChecksumValueForDeclaredAlgorithm: true, out var checksumAlgorithm, out var requestedChecksums, out var checksumErrorResult)) {
                    return checksumErrorResult!;
                }

                return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                    var (customerEncryption, customerEncryptionError) = TryParseCustomerEncryptionSettings(httpContext.Request, BuildObjectResource(bucketName, key), bucketName, key);
                    if (customerEncryptionError is not null) {
                        return customerEncryptionError;
                    }

                    var result = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        UploadId = uploadId!,
                        PartNumber = partNumber!.Value,
                        Content = preparedBody.Content,
                        ContentLength = preparedBody.ContentLength,
                        ChecksumAlgorithm = checksumAlgorithm,
                        Checksums = requestedChecksums,
                        CustomerEncryption = customerEncryption
                    }, innerCancellationToken);

                    if (!result.IsSuccess) {
                        return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                    }

                    httpContext.Response.Headers.ETag = QuoteETag(result.Value!.ETag);
                    ApplyChecksumHeaders(httpContext.Response, result.Value.Checksums);
                    if (customerEncryption is not null) {
                        httpContext.Response.Headers[ServerSideEncryptionCustomerAlgorithmHeaderName] = customerEncryption.Algorithm;
                        httpContext.Response.Headers[ServerSideEncryptionCustomerKeyMd5HeaderName] = customerEncryption.KeyMd5;
                    }
                    return TypedResults.Ok();
                }, cancellationToken);
            }
            finally {
                await preparedBody.DisposeAsync();
            }
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> UploadPartCopyAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryGetMultipartUploadId(httpContext.Request, out var uploadId, out var uploadIdError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", uploadIdError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        if (!TryGetPartNumber(httpContext.Request, out var partNumber, out var partNumberError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", partNumberError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        if (!TryGetCopySource(httpContext.Request, out var copySource, out var copySourceError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", $"The '{CopySourceHeaderName}' header is required for multipart part copy requests.", BuildObjectResource(bucketName, key), bucketName, key);
        }

        if (copySourceError is not null) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", copySourceError, BuildObjectResource(bucketName, key), bucketName, key);
        }

        if (!TryGetCopySourceRange(httpContext.Request, out var sourceRange, out var sourceRangeError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", sourceRangeError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        if (!TryParseRequestChecksums(httpContext.Request, preparedBody: null, requireChecksumValueForDeclaredAlgorithm: false, out var checksumAlgorithm, out var requestedChecksums, out var checksumErrorResult)) {
            return checksumErrorResult!;
        }

        if (!TryParseObjectServerSideEncryptionSettings(httpContext.Request, allowManagedRequestHeaders: false, BuildObjectResource(bucketName, key), bucketName, key, out _, out var serverSideEncryptionErrorResult)) {
            return serverSideEncryptionErrorResult!;
        }

        var (sourceCustomerEncryption, sourceCustomerEncryptionError) = TryParseCopySourceCustomerEncryptionSettings(httpContext.Request, BuildObjectResource(bucketName, key), bucketName, key);
        if (sourceCustomerEncryptionError is not null) {
            return sourceCustomerEncryptionError;
        }

        var (destinationCustomerEncryption, destinationCustomerEncryptionError) = TryParseCustomerEncryptionSettings(httpContext.Request, BuildObjectResource(bucketName, key), bucketName, key);
        if (destinationCustomerEncryptionError is not null) {
            return destinationCustomerEncryptionError;
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.UploadPartCopyAsync(new UploadPartCopyRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = uploadId!,
                    PartNumber = partNumber!.Value,
                    SourceBucketName = copySource!.BucketName,
                    SourceKey = copySource.Key,
                    SourceVersionId = copySource.VersionId,
                    SourceIfMatchETag = httpContext.Request.Headers[CopySourceIfMatchHeaderName].ToString(),
                    SourceIfNoneMatchETag = httpContext.Request.Headers[CopySourceIfNoneMatchHeaderName].ToString(),
                    SourceIfModifiedSinceUtc = ParseOptionalHttpDateHeader(httpContext.Request.Headers[CopySourceIfModifiedSinceHeaderName].ToString()),
                    SourceIfUnmodifiedSinceUtc = ParseOptionalHttpDateHeader(httpContext.Request.Headers[CopySourceIfUnmodifiedSinceHeaderName].ToString()),
                    SourceRange = sourceRange,
                    ChecksumAlgorithm = checksumAlgorithm,
                    Checksums = requestedChecksums,
                    SourceCustomerEncryption = sourceCustomerEncryption,
                    DestinationCustomerEncryption = destinationCustomerEncryption
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(
                        httpContext,
                        result.Error,
                        resourceOverride: BuildObjectResource(bucketName, key),
                        explicitVersionId: copySource.VersionId);
                }

                if (destinationCustomerEncryption is not null) {
                    httpContext.Response.Headers[ServerSideEncryptionCustomerAlgorithmHeaderName] = destinationCustomerEncryption.Algorithm;
                    httpContext.Response.Headers[ServerSideEncryptionCustomerKeyMd5HeaderName] = destinationCustomerEncryption.KeyMd5;
                }

                return ToCopyPartResult(httpContext, result.Value!, copySource.VersionId);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryGetMultipartUploadId(httpContext.Request, out var uploadId, out var uploadIdError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", uploadIdError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        S3CompleteMultipartUploadRequest requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadCompleteMultipartUploadRequestAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var upload = await FindMultipartUploadAsync(storageService, bucketName, key, uploadId!, innerCancellationToken);
                if (upload is null) {
                    return ToErrorResult(
                        httpContext,
                        StatusCodes.Status404NotFound,
                        "NoSuchUpload",
                        "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
                        BuildObjectResource(bucketName, key),
                        bucketName,
                        key);
                }

                var result = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = uploadId!,
                    Parts = requestBody.Parts.Select(static part => new MultipartUploadPart
                    {
                        PartNumber = part.PartNumber,
                        ETag = part.ETag,
                        ContentLength = 0,
                        LastModifiedUtc = default,
                        Checksums = part.Checksums
                    }).ToArray()
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var completedObject = result.Value!;
                ApplyObjectIdentityHeaders(httpContext.Response, completedObject);
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteCompleteMultipartUploadResult(new S3CompleteMultipartUploadResult
                    {
                        Location = $"{httpContext.Request.Scheme}://{httpContext.Request.Host}{httpContext.Request.PathBase}{httpContext.Request.Path}",
                        Bucket = bucketName,
                        Key = key,
                        ETag = completedObject.ETag ?? string.Empty,
                        ChecksumCrc32 = GetChecksumValue(completedObject.Checksums, "crc32"),
                        ChecksumCrc32c = GetChecksumValue(completedObject.Checksums, "crc32c"),
                        ChecksumSha1 = GetChecksumValue(completedObject.Checksums, "sha1"),
                        ChecksumSha256 = GetChecksumValue(completedObject.Checksums, "sha256"),
                        ChecksumType = GetChecksumType(completedObject.Checksums)
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> AbortMultipartUploadAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryGetMultipartUploadId(httpContext.Request, out var uploadId, out var uploadIdError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", uploadIdError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = uploadId!
                }, innerCancellationToken).AsTask(), cancellationToken);

            return result.IsSuccess
                ? TypedResults.NoContent()
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> ListMultipartPartsAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryGetMultipartUploadId(httpContext.Request, out var uploadId, out var uploadIdError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", uploadIdError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            var parsedEncodingType = ParseMultipartListEncodingType(httpContext.Request);
            var parsedPartNumberMarker = ParsePartNumberMarker(httpContext.Request);
            var parsedMaxParts = ParseMaxParts(httpContext.Request);

            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                if (parsedPartNumberMarker < 0) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "part-number-marker must be greater than or equal to 0.", BuildObjectResource(bucketName, key), bucketName, key);
                }

                if (parsedMaxParts is <= 0 or > 1000) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "max-parts must be between 1 and 1000.", BuildObjectResource(bucketName, key), bucketName, key);
                }

                MultipartUploadInfo? upload;
                try {
                    upload = await FindMultipartUploadAsync(storageService, bucketName, key, uploadId!, innerCancellationToken);
                    if (upload is null) {
                        return ToErrorResult(
                            httpContext,
                            StatusCodes.Status404NotFound,
                            "NoSuchUpload",
                            "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
                            BuildObjectResource(bucketName, key),
                            bucketName,
                            key);
                    }

                    var fetchPageSize = parsedMaxParts == int.MaxValue
                        ? parsedMaxParts
                        : parsedMaxParts + 1;
                    var parts = await storageService.ListMultipartPartsAsync(new ListMultipartPartsRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        UploadId = uploadId!,
                        PartNumberMarker = parsedPartNumberMarker == 0 ? null : parsedPartNumberMarker,
                        PageSize = fetchPageSize
                    }, innerCancellationToken).ToArrayAsync(innerCancellationToken);

                    var response = BuildListMultipartPartsResult(
                        bucketName,
                        key,
                        upload,
                        parsedEncodingType,
                        parsedPartNumberMarker,
                        parsedMaxParts,
                        parts,
                        ResolveS3ListingIdentity(httpContext.User));

                    return new XmlContentResult(S3XmlResponseWriter.WriteListPartsResult(response), StatusCodes.Status200OK, XmlContentType);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }
                catch (NotSupportedException exception) {
                    return ToErrorResult(httpContext, StatusCodes.Status501NotImplemented, "NotImplemented", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
        }
    }

    private static async Task<IResult> ListMultipartUploadsAsync(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? uploadIdMarker,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var parsedEncodingType = ParseMultipartListEncodingType(httpContext.Request);
            var parsedMaxUploads = ParseMaxUploads(httpContext.Request);

            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                if (parsedMaxUploads is <= 0 or > 1000) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "max-uploads must be between 1 and 1000.", BuildObjectResource(bucketName, null), bucketName);
                }

                var normalizedKeyMarker = string.IsNullOrWhiteSpace(keyMarker)
                    ? null
                    : keyMarker;
                var normalizedUploadIdMarker = normalizedKeyMarker is null || string.IsNullOrWhiteSpace(uploadIdMarker)
                    ? null
                    : uploadIdMarker;
                var requestedPageSize = parsedMaxUploads ?? 1000;

                try {
                    var uploads = await storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
                    {
                        BucketName = bucketName,
                        Prefix = prefix,
                        Delimiter = delimiter,
                        KeyMarker = normalizedKeyMarker,
                        UploadIdMarker = normalizedUploadIdMarker
                    }, innerCancellationToken).ToArrayAsync(innerCancellationToken);

                    var response = BuildListMultipartUploadsResult(
                        bucketName,
                        prefix,
                        delimiter,
                        normalizedKeyMarker,
                        normalizedUploadIdMarker,
                        parsedEncodingType,
                        requestedPageSize,
                        uploads,
                        ResolveS3ListingIdentity(httpContext.User));

                    return new XmlContentResult(S3XmlResponseWriter.WriteListMultipartUploadsResult(response), StatusCodes.Status200OK, XmlContentType);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
                catch (NotSupportedException exception) {
                    return ToErrorResult(httpContext, StatusCodes.Status501NotImplemented, "NotImplemented", exception.Message, BuildObjectResource(bucketName, null), bucketName);
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> ListObjectsV1Async(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? marker,
        int? maxKeys,
        string? encodingType,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                if (maxKeys is <= 0) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "max-keys must be greater than zero.", BuildObjectResource(bucketName, null), bucketName);
                }

                var requestedPageSize = maxKeys ?? 1000;

                try {
                    var objects = await storageService.ListObjectsAsync(new ListObjectsRequest
                    {
                        BucketName = bucketName,
                        Prefix = prefix
                    }, innerCancellationToken).ToArrayAsync(innerCancellationToken);

                    var response = BuildListBucketResult(
                        bucketName,
                        prefix,
                        delimiter,
                        marker,
                        startAfter: null,
                        continuationToken: null,
                        requestedPageSize,
                        objects,
                        isV2: false,
                        includeOwner: true,
                        encodingType,
                        ResolveS3ListingIdentity(httpContext.User));

                    return new XmlContentResult(S3XmlResponseWriter.WriteListBucketResult(response), StatusCodes.Status200OK, XmlContentType);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> ListObjectsV2Async(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? startAfter,
        string? continuationToken,
        int? maxKeys,
        string? encodingType,
        bool fetchOwner,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                if (maxKeys is <= 0) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "max-keys must be greater than zero.", BuildObjectResource(bucketName, null), bucketName);
                }

                var requestedPageSize = maxKeys ?? 1000;

                try {
                    var objects = await storageService.ListObjectsAsync(new ListObjectsRequest
                    {
                        BucketName = bucketName,
                        Prefix = prefix
                    }, innerCancellationToken).ToArrayAsync(innerCancellationToken);

                    var response = BuildListBucketResult(
                        bucketName,
                        prefix,
                        delimiter,
                        marker: null,
                        startAfter,
                        continuationToken,
                        requestedPageSize,
                        objects,
                        isV2: true,
                        includeOwner: fetchOwner,
                        encodingType,
                        ResolveS3ListingIdentity(httpContext.User));

                    return new XmlContentResult(S3XmlResponseWriter.WriteListBucketResult(response), StatusCodes.Status200OK, XmlContentType);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> ListObjectVersionsAsync(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? versionIdMarker,
        int? maxKeys,
        string? encodingType,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                if (maxKeys is <= 0) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "max-keys must be greater than zero.", BuildObjectResource(bucketName, null), bucketName);
                }

                var requestedPageSize = maxKeys ?? 1000;

                try {
                    var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
                    {
                        BucketName = bucketName,
                        Prefix = prefix,
                        Delimiter = delimiter,
                        KeyMarker = keyMarker,
                        VersionIdMarker = versionIdMarker
                    }, innerCancellationToken).ToArrayAsync(innerCancellationToken);

                    var response = BuildListObjectVersionsResult(
                        bucketName,
                        prefix,
                        delimiter,
                        keyMarker,
                        versionIdMarker,
                        encodingType,
                        requestedPageSize,
                        versions);

                    return new XmlContentResult(S3XmlResponseWriter.WriteListObjectVersionsResult(response), StatusCodes.Status200OK, XmlContentType);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static bool IsPostObjectFormUpload(HttpRequest request)
    {
        var contentType = request.ContentType;
        return !string.IsNullOrWhiteSpace(contentType)
            && contentType.StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<IResult> PostObjectAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                if (!httpContext.Request.HasFormContentType) {
                    return ToErrorResult(
                        httpContext,
                        StatusCodes.Status400BadRequest,
                        "InvalidRequest",
                        "POST object requests must use multipart/form-data encoding.",
                        BuildObjectResource(bucketName, null),
                        bucketName);
                }

                var form = await httpContext.Request.ReadFormAsync(innerCancellationToken);
                var objectKey = form["key"].ToString();
                if (string.IsNullOrWhiteSpace(objectKey)) {
                    return ToErrorResult(
                        httpContext,
                        StatusCodes.Status400BadRequest,
                        "InvalidArgument",
                        "The 'key' field is required in POST object requests.",
                        BuildObjectResource(bucketName, null),
                        bucketName);
                }

                // Validate policy if provided.
                var policyField = form["policy"].ToString();
                var signatureField = form["signature"].ToString();
                if (!string.IsNullOrWhiteSpace(policyField)) {
                    var policyValidation = ValidatePostObjectPolicy(policyField, bucketName, objectKey, form);
                    if (policyValidation is not null) {
                        return policyValidation;
                    }
                }

                // Extract file from the 'file' form field.
                var file = form.Files.GetFile("file");
                if (file is null || file.Length == 0) {
                    return ToErrorResult(
                        httpContext,
                        StatusCodes.Status400BadRequest,
                        "InvalidArgument",
                        "The 'file' field is required in POST object requests.",
                        BuildObjectResource(bucketName, objectKey),
                        bucketName,
                        objectKey);
                }

                // Resolve the key: replace ${filename} placeholder if present.
                if (objectKey.Contains("${filename}")) {
                    objectKey = objectKey.Replace("${filename}", file.FileName ?? "upload");
                }

                var contentType = form["Content-Type"].ToString();
                if (string.IsNullOrWhiteSpace(contentType)) {
                    contentType = file.ContentType ?? "application/octet-stream";
                }

                var acl = form[CannedAclHeaderName].ToString();
                var successActionStatus = 204;
                var successActionStatusRaw = form["success_action_status"].ToString();
                if (!string.IsNullOrWhiteSpace(successActionStatusRaw)
                    && int.TryParse(successActionStatusRaw, out var parsedStatus)
                    && parsedStatus is 200 or 201 or 204) {
                    successActionStatus = parsedStatus;
                }

                var successActionRedirect = form["success_action_redirect"].ToString();

                // Extract user metadata from x-amz-meta-* form fields.
                Dictionary<string, string>? metadata = null;
                foreach (var field in form) {
                    if (field.Key.StartsWith(MetadataHeaderPrefix, StringComparison.OrdinalIgnoreCase)) {
                        metadata ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        metadata[field.Key[MetadataHeaderPrefix.Length..]] = field.Value.ToString();
                    }
                }

                await using var fileStream = file.OpenReadStream();
                var putResult = await storageService.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = objectKey,
                    ContentType = contentType,
                    Content = fileStream,
                    ContentLength = file.Length,
                    Metadata = metadata
                }, innerCancellationToken);

                if (!putResult.IsSuccess) {
                    return ToErrorResult(httpContext, putResult.Error, resourceOverride: BuildObjectResource(bucketName, objectKey));
                }

                var objectInfo = putResult.Value!;
                ApplyObjectIdentityHeaders(httpContext.Response, objectInfo);

                // Handle ACL if specified.
                if (!string.IsNullOrWhiteSpace(acl) && TryParseSupportedCannedAcl(acl, out var cannedAcl)) {
                    var compatibilityService = httpContext.RequestServices.GetService<IStorageAuthorizationCompatibilityService>();
                    if (compatibilityService is not null) {
                        await compatibilityService.PutObjectAclAsync(new PutObjectAclCompatibilityRequest
                        {
                            BucketName = bucketName,
                            Key = objectKey,
                            CannedAcl = cannedAcl
                        }, innerCancellationToken);
                    }
                }

                // Handle success_action_redirect.
                if (!string.IsNullOrWhiteSpace(successActionRedirect)) {
                    var separator = successActionRedirect.Contains('?') ? "&" : "?";
                    var redirectUrl = $"{successActionRedirect}{separator}bucket={Uri.EscapeDataString(bucketName)}&key={Uri.EscapeDataString(objectKey)}&etag={Uri.EscapeDataString(objectInfo.ETag ?? "")}";
                    return TypedResults.Redirect(redirectUrl);
                }

                return successActionStatus switch
                {
                    200 => TypedResults.Ok(),
                    201 => new XmlContentResult(
                        S3XmlResponseWriter.WritePostObjectResult(bucketName, objectKey, objectInfo.ETag),
                        StatusCodes.Status201Created,
                        XmlContentType),
                    _ => TypedResults.StatusCode(StatusCodes.Status204NoContent)
                };
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (InvalidOperationException exception) when (exception.Message.Contains("form", StringComparison.OrdinalIgnoreCase)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", "Failed to parse form data.", BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static IResult? ValidatePostObjectPolicy(string base64Policy, string bucketName, string objectKey, IFormCollection form)
    {
        try {
            var policyBytes = Convert.FromBase64String(base64Policy);
            using var policyDoc = JsonDocument.Parse(policyBytes);
            var root = policyDoc.RootElement;

            // Validate expiration.
            if (root.TryGetProperty("expiration", out var expirationElement)) {
                if (DateTimeOffset.TryParse(expirationElement.GetString(), out var expiration)
                    && expiration < DateTimeOffset.UtcNow) {
                    return TypedResults.StatusCode(StatusCodes.Status403Forbidden);
                }
            }

            // Validate conditions.
            if (root.TryGetProperty("conditions", out var conditions) && conditions.ValueKind == JsonValueKind.Array) {
                foreach (var condition in conditions.EnumerateArray()) {
                    if (condition.ValueKind == JsonValueKind.Object) {
                        foreach (var property in condition.EnumerateObject()) {
                            var fieldName = property.Name;
                            var expectedValue = property.Value.GetString() ?? "";
                            if (string.Equals(fieldName, "bucket", StringComparison.OrdinalIgnoreCase)) {
                                if (!string.Equals(expectedValue, bucketName, StringComparison.Ordinal)) {
                                    return TypedResults.StatusCode(StatusCodes.Status403Forbidden);
                                }
                            }
                            else if (string.Equals(fieldName, "key", StringComparison.OrdinalIgnoreCase)) {
                                if (!string.Equals(expectedValue, objectKey, StringComparison.Ordinal)) {
                                    return TypedResults.StatusCode(StatusCodes.Status403Forbidden);
                                }
                            }
                            else {
                                var formValue = form[fieldName].ToString();
                                if (!string.Equals(expectedValue, formValue, StringComparison.Ordinal)) {
                                    return TypedResults.StatusCode(StatusCodes.Status403Forbidden);
                                }
                            }
                        }
                    }
                    else if (condition.ValueKind == JsonValueKind.Array) {
                        var elements = condition.EnumerateArray().ToArray();
                        if (elements.Length == 3) {
                            var op = elements[0].GetString() ?? "";
                            var fieldName = (elements[1].GetString() ?? "").TrimStart('$');
                            var bound = elements[2].GetString() ?? "";
                            var formValue = form[fieldName].ToString();

                            if (string.Equals(op, "eq", StringComparison.OrdinalIgnoreCase)) {
                                if (!string.Equals(bound, formValue, StringComparison.Ordinal)) {
                                    return TypedResults.StatusCode(StatusCodes.Status403Forbidden);
                                }
                            }
                            else if (string.Equals(op, "starts-with", StringComparison.OrdinalIgnoreCase)) {
                                if (!formValue.StartsWith(bound, StringComparison.Ordinal)) {
                                    return TypedResults.StatusCode(StatusCodes.Status403Forbidden);
                                }
                            }
                        }
                    }
                }
            }

            return null;
        }
        catch (FormatException) {
            return TypedResults.StatusCode(StatusCodes.Status403Forbidden);
        }
        catch (JsonException) {
            return TypedResults.StatusCode(StatusCodes.Status403Forbidden);
        }
    }

    private static async Task<IResult> DeleteObjectsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3DeleteObjectsRequest deleteRequest;
        try {
            deleteRequest = await S3XmlRequestReader.ReadDeleteObjectsRequestAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var deleted = new List<S3DeletedObjectResult>(deleteRequest.Objects.Count);
                var errors = new List<S3DeleteObjectError>();

                foreach (var objectIdentifier in deleteRequest.Objects) {
                    var result = await storageService.DeleteObjectAsync(new DeleteObjectRequest
                    {
                        BucketName = bucketName,
                        Key = objectIdentifier.Key,
                        VersionId = objectIdentifier.VersionId
                    }, innerCancellationToken);

                    if (result.IsSuccess || (result.Error?.Code == StorageErrorCode.ObjectNotFound && string.IsNullOrWhiteSpace(objectIdentifier.VersionId))) {
                        if (!deleteRequest.Quiet) {
                            var deletedVersionId = result.Value?.VersionId ?? objectIdentifier.VersionId;
                            var isDeleteMarker = result.Value?.IsDeleteMarker == true;
                            deleted.Add(new S3DeletedObjectResult
                            {
                                Key = objectIdentifier.Key,
                                VersionId = isDeleteMarker && string.IsNullOrWhiteSpace(objectIdentifier.VersionId)
                                    ? null
                                    : deletedVersionId,
                                DeleteMarker = isDeleteMarker,
                                DeleteMarkerVersionId = isDeleteMarker
                                    ? deletedVersionId
                                    : null
                            });
                        }

                        continue;
                    }

                    var deleteError = result.Error!;
                    errors.Add(new S3DeleteObjectError
                    {
                        Key = objectIdentifier.Key,
                        VersionId = objectIdentifier.VersionId,
                        Code = ToCompatibleErrorCode(deleteError, objectIdentifier.VersionId),
                        Message = ToCompatibleErrorMessage(deleteError, objectIdentifier.VersionId)
                    });
                }

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteDeleteObjectsResult(new S3DeleteObjectsResult
                    {
                        Deleted = deleted,
                        Errors = errors
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static IResult CreateFeatureDisabledResult()
    {
        return TypedResults.NotFound();
    }

    private static bool IsMultipartRequest(HttpRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return request.Query.ContainsKey(UploadsQueryParameterName)
            || request.Query.ContainsKey(UploadIdQueryParameterName);
    }

    private static async Task<T> ExecuteWithRequestContextAsync<T>(
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        Func<CancellationToken, Task<T>> action,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(requestContextAccessor);
        ArgumentNullException.ThrowIfNull(action);

        var previousContext = requestContextAccessor.Current;
        requestContextAccessor.Current = new IntegratedS3RequestContext
        {
            Principal = httpContext.User,
            CorrelationId = IntegratedS3AspNetCoreTelemetry.GetOrCreateCorrelationId(httpContext),
            RequestId = httpContext.TraceIdentifier
        };

        try {
            return await action(cancellationToken);
        }
        catch (StorageAuthorizationException exception) {
            throw new EndpointStorageAuthorizationException(exception.Error);
        }
        finally {
            requestContextAccessor.Current = previousContext;
        }
    }

    private static IResult WrapBucketCorsResult(string bucketName, IResult innerResult)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentNullException.ThrowIfNull(innerResult);
        return new BucketCorsResult(bucketName, innerResult);
    }

    private static IResult ToErrorResult(HttpContext httpContext, StorageError? error, string? resourceOverride = null, string? explicitVersionId = null)
    {
        if (error is null)
        {
            return ToErrorResult(httpContext, StatusCodes.Status500InternalServerError, "InternalError", "Storage operation failed.", resourceOverride);
        }

        ApplyStorageErrorHeaders(httpContext.Response, error);

        return ToErrorResult(
            httpContext,
            error.SuggestedHttpStatusCode ?? ToStatusCode(error.Code),
            ToCompatibleErrorCode(error, explicitVersionId),
            ToCompatibleErrorMessage(error, explicitVersionId),
            resourceOverride ?? BuildResource(error.BucketName, error.ObjectKey),
            error.BucketName,
            error.ObjectKey);
    }

    private static IResult ToErrorResult(
        HttpContext httpContext,
        int statusCode,
        string code,
        string message,
        string? resource,
        string? bucketName = null,
        string? key = null)
    {
        httpContext.Response.Headers["x-amz-request-id"] = httpContext.TraceIdentifier;
        httpContext.Response.Headers["x-amz-id-2"] = httpContext.TraceIdentifier;

        if (HttpMethods.IsHead(httpContext.Request.Method)) {
            httpContext.Response.Headers[ErrorCodeHeaderName] = code;
            httpContext.Response.Headers[ErrorMessageHeaderName] = message;
        }

        return new XmlContentResult(
            S3XmlResponseWriter.WriteError(new S3ErrorResponse
            {
                Code = code,
                Message = message,
                Resource = resource,
                RequestId = httpContext.TraceIdentifier,
                BucketName = bucketName,
                Key = key
            }),
            statusCode,
            XmlContentType);
    }

    private static string ToCompatibleErrorCode(StorageError error, string? explicitVersionId)
    {
        return IsExplicitMissingVersionError(error, explicitVersionId)
            ? "NoSuchVersion"
            : ToS3ErrorCode(error.Code);
    }

    private static string ToCompatibleErrorMessage(StorageError error, string? explicitVersionId)
    {
        return IsExplicitMissingVersionError(error, explicitVersionId)
            ? NoSuchVersionMessage
            : error.Message;
    }

    private static bool IsExplicitMissingVersionError(StorageError? error, string? explicitVersionId)
    {
        return error?.Code == StorageErrorCode.ObjectNotFound
            && !string.IsNullOrWhiteSpace(explicitVersionId);
    }

    private static IResult ToCopyObjectResult(HttpContext httpContext, ObjectInfo @object, string? sourceVersionId)
    {
        ApplyObjectResultHeaders(httpContext.Response, @object);
        ApplyChecksumAlgorithmHeader(httpContext.Response, GetResponseChecksumAlgorithm(@object.Checksums));
        if (!string.IsNullOrWhiteSpace(sourceVersionId)) {
            httpContext.Response.Headers[CopySourceVersionIdHeaderName] = sourceVersionId;
        }

        return new XmlContentResult(
            S3XmlResponseWriter.WriteCopyObjectResult(new S3CopyObjectResult
            {
                ETag = @object.ETag ?? string.Empty,
                LastModifiedUtc = @object.LastModifiedUtc,
                ChecksumCrc32 = GetChecksumValue(@object.Checksums, "crc32"),
                ChecksumCrc32c = GetChecksumValue(@object.Checksums, "crc32c"),
                ChecksumSha1 = GetChecksumValue(@object.Checksums, "sha1"),
                ChecksumSha256 = GetChecksumValue(@object.Checksums, "sha256"),
                ChecksumType = GetChecksumType(@object.Checksums)
            }),
            StatusCodes.Status200OK,
            XmlContentType);
    }

    private static IResult ToCopyPartResult(HttpContext httpContext, MultipartUploadPart part, string? sourceVersionId)
    {
        httpContext.Response.Headers.ETag = QuoteETag(part.ETag);
        ApplyChecksumHeaders(httpContext.Response, part.Checksums);
        if (!string.IsNullOrWhiteSpace(sourceVersionId)) {
            httpContext.Response.Headers[CopySourceVersionIdHeaderName] = sourceVersionId;
        }

        return new XmlContentResult(
            S3XmlResponseWriter.WriteCopyPartResult(new S3CopyObjectResult
            {
                ETag = part.ETag,
                LastModifiedUtc = part.LastModifiedUtc,
                ChecksumCrc32 = GetChecksumValue(part.Checksums, "crc32"),
                ChecksumCrc32c = GetChecksumValue(part.Checksums, "crc32c"),
                ChecksumSha1 = GetChecksumValue(part.Checksums, "sha1"),
                ChecksumSha256 = GetChecksumValue(part.Checksums, "sha256")
            }),
            StatusCodes.Status200OK,
            XmlContentType);
    }

    private static int ToStatusCode(StorageErrorCode code)
    {
        return code switch
        {
            StorageErrorCode.ObjectNotFound => StatusCodes.Status404NotFound,
            StorageErrorCode.BucketNotFound => StatusCodes.Status404NotFound,
            StorageErrorCode.CorsConfigurationNotFound => StatusCodes.Status404NotFound,
            StorageErrorCode.BucketEncryptionConfigurationNotFound => StatusCodes.Status404NotFound,
            StorageErrorCode.AccessDenied => StatusCodes.Status403Forbidden,
            StorageErrorCode.InvalidTag => StatusCodes.Status400BadRequest,
            StorageErrorCode.InvalidChecksum => StatusCodes.Status400BadRequest,
            StorageErrorCode.InvalidRange => StatusCodes.Status416RangeNotSatisfiable,
            StorageErrorCode.PreconditionFailed => StatusCodes.Status412PreconditionFailed,
            StorageErrorCode.MethodNotAllowed => StatusCodes.Status405MethodNotAllowed,
            StorageErrorCode.VersionConflict => StatusCodes.Status409Conflict,
            StorageErrorCode.BucketAlreadyExists => StatusCodes.Status409Conflict,
            StorageErrorCode.BucketNotEmpty => StatusCodes.Status409Conflict,
            StorageErrorCode.MultipartConflict => StatusCodes.Status409Conflict,
            StorageErrorCode.Throttled => StatusCodes.Status429TooManyRequests,
            StorageErrorCode.ProviderUnavailable => StatusCodes.Status503ServiceUnavailable,
            StorageErrorCode.UnsupportedCapability => StatusCodes.Status501NotImplemented,
            StorageErrorCode.QuotaExceeded => StatusCodes.Status413PayloadTooLarge,
            _ => StatusCodes.Status500InternalServerError
        };
    }

    private static string ToS3ErrorCode(StorageErrorCode code)
    {
        return code switch
        {
            StorageErrorCode.ObjectNotFound => "NoSuchKey",
            StorageErrorCode.BucketNotFound => "NoSuchBucket",
            StorageErrorCode.CorsConfigurationNotFound => "NoSuchCORSConfiguration",
            StorageErrorCode.BucketEncryptionConfigurationNotFound => "ServerSideEncryptionConfigurationNotFoundError",
            StorageErrorCode.AccessDenied => "AccessDenied",
            StorageErrorCode.InvalidTag => "InvalidTag",
            StorageErrorCode.InvalidChecksum => "BadDigest",
            StorageErrorCode.InvalidRange => "InvalidRange",
            StorageErrorCode.PreconditionFailed => "PreconditionFailed",
            StorageErrorCode.MethodNotAllowed => "MethodNotAllowed",
            StorageErrorCode.VersionConflict => "OperationAborted",
            StorageErrorCode.BucketAlreadyExists => "BucketAlreadyExists",
            StorageErrorCode.BucketNotEmpty => "BucketNotEmpty",
            StorageErrorCode.MultipartConflict => "InvalidRequest",
            StorageErrorCode.Throttled => "SlowDown",
            StorageErrorCode.ProviderUnavailable => "ServiceUnavailable",
            StorageErrorCode.UnsupportedCapability => "NotImplemented",
            StorageErrorCode.QuotaExceeded => "EntityTooLarge",
            _ => "InternalError"
        };
    }

    private static string? BuildResource(string? bucketName, string? key)
    {
        if (string.IsNullOrWhiteSpace(bucketName)) {
            return null;
        }

        return BuildObjectResource(bucketName, key);
    }

    private static string BuildObjectResource(string bucketName, string? key)
    {
        return string.IsNullOrWhiteSpace(key)
            ? $"/{bucketName}"
            : $"/{bucketName}/{key}";
    }

    private static bool IsSigV4AuthenticatedRequest(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        return httpContext.User.HasClaim(SigV4AuthenticationClaimType, SigV4AuthenticationClaimValue);
    }

    private static bool TryResolveCompatibleRequest(
        HttpRequest request,
        IntegratedS3Options options,
        string? s3Path,
        out ResolvedS3Request? resolvedRequest,
        out string? error)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(options);

        var normalizedPath = string.IsNullOrWhiteSpace(s3Path)
            ? null
            : s3Path.Trim('/');

        var virtualHostedBucketName = TryResolveVirtualHostedBucketName(request.Host, options);
        if (!string.IsNullOrWhiteSpace(virtualHostedBucketName)) {
            resolvedRequest = CreateResolvedRequest(virtualHostedBucketName, string.IsNullOrWhiteSpace(normalizedPath) ? null : normalizedPath, S3AddressingStyle.VirtualHosted);
            error = null;
            return true;
        }

        if (string.IsNullOrWhiteSpace(normalizedPath)) {
            resolvedRequest = null;
            error = null;
            return false;
        }

        var separatorIndex = normalizedPath.IndexOf('/');
        var bucketName = separatorIndex < 0
            ? normalizedPath
            : normalizedPath[..separatorIndex];

        if (string.IsNullOrWhiteSpace(bucketName)) {
            resolvedRequest = null;
            error = "The request path must contain a bucket name.";
            return false;
        }

        var key = separatorIndex < 0 || separatorIndex == normalizedPath.Length - 1
            ? null
            : normalizedPath[(separatorIndex + 1)..];

        resolvedRequest = CreateResolvedRequest(bucketName, key, S3AddressingStyle.Path);
        error = null;
        return true;
    }

    private static string? TryResolveVirtualHostedBucketName(HostString host, IntegratedS3Options options)
    {
        if (!options.EnableVirtualHostedStyleAddressing || options.VirtualHostedStyleHostSuffixes.Count == 0) {
            return null;
        }

        var hostValue = host.Host;
        if (string.IsNullOrWhiteSpace(hostValue)) {
            return null;
        }

        foreach (var suffix in options.VirtualHostedStyleHostSuffixes) {
            if (string.Equals(hostValue, suffix, StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            if (!hostValue.EndsWith($".{suffix}", StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            var bucketName = hostValue[..^(suffix.Length + 1)];
            if (!string.IsNullOrWhiteSpace(bucketName)) {
                return bucketName;
            }
        }

        return null;
    }

    private static ResolvedS3Request CreateResolvedRequest(string bucketName, string? key, S3AddressingStyle addressingStyle)
    {
        var canonicalResourcePath = string.IsNullOrWhiteSpace(key)
            ? $"/{bucketName}"
            : $"/{bucketName}/{key}";

        var canonicalPath = string.IsNullOrWhiteSpace(key)
            ? "/"
            : $"/{string.Join('/', key.Split('/', StringSplitOptions.RemoveEmptyEntries).Select(Uri.EscapeDataString))}";

        return new ResolvedS3Request(bucketName, key, addressingStyle, canonicalResourcePath, canonicalPath);
    }

    private static string? ParsePrefix(HttpRequest request)
    {
        return request.Query.TryGetValue(PrefixQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static string? ParseDelimiter(HttpRequest request)
    {
        if (!request.Query.TryGetValue(DelimiterQueryParameterName, out var values)) {
            return null;
        }

        var delimiter = values.ToString();
        return string.IsNullOrEmpty(delimiter)
            ? null
            : delimiter;
    }

    private static string? ParseStartAfter(HttpRequest request)
    {
        if (!request.Query.TryGetValue(StartAfterQueryParameterName, out var values)) {
            return null;
        }

        var startAfter = values.ToString();
        return string.IsNullOrWhiteSpace(startAfter)
            ? null
            : startAfter;
    }

    private static string? ParseMarker(HttpRequest request)
    {
        if (!request.Query.TryGetValue(MarkerQueryParameterName, out var values)) {
            return null;
        }

        var marker = values.ToString();
        return string.IsNullOrWhiteSpace(marker)
            ? null
            : marker;
    }

    private static string? ParseContinuationToken(HttpRequest request)
    {
        return request.Query.TryGetValue(ContinuationTokenQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static string? ParseEncodingType(HttpRequest request)
    {
        if (!request.Query.TryGetValue(EncodingTypeQueryParameterName, out var values)) {
            return null;
        }

        var encodingType = values.ToString();
        return string.Equals(encodingType, UrlEncodingTypeValue, StringComparison.Ordinal)
            ? encodingType
            : throw new ArgumentException("The encoding-type query parameter must be 'url'.", EncodingTypeQueryParameterName);
    }

    private static bool ParseFetchOwner(HttpRequest request)
    {
        if (!request.Query.TryGetValue(FetchOwnerQueryParameterName, out var values)) {
            return false;
        }

        return bool.TryParse(values.ToString(), out var fetchOwner)
            ? fetchOwner
            : throw new ArgumentException("The fetch-owner query parameter must be 'true' or 'false'.", FetchOwnerQueryParameterName);
    }

    private static string? ParseKeyMarker(HttpRequest request)
    {
        return request.Query.TryGetValue(KeyMarkerQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static string? ParseVersionIdMarker(HttpRequest request)
    {
        return request.Query.TryGetValue(VersionIdMarkerQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static string? ParseUploadIdMarker(HttpRequest request)
    {
        return request.Query.TryGetValue(UploadIdMarkerQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static string? ParseMultipartListEncodingType(HttpRequest request)
    {
        if (!request.Query.TryGetValue(EncodingTypeQueryParameterName, out var values)) {
            return null;
        }

        var encodingType = values.ToString();
        if (string.IsNullOrWhiteSpace(encodingType)) {
            return null;
        }

        return string.Equals(encodingType, "url", StringComparison.Ordinal)
            ? "url"
            : throw new ArgumentException("The encoding-type query parameter must be 'url' when specified for multipart listing operations.", EncodingTypeQueryParameterName);
    }

    private static async Task<MultipartUploadInfo?> FindMultipartUploadAsync(
        IStorageService storageService,
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken)
    {
        await foreach (var upload in storageService.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
                       {
                           BucketName = bucketName,
                           Prefix = key
                       }, cancellationToken).WithCancellation(cancellationToken)) {
            if (string.Equals(upload.Key, key, StringComparison.Ordinal)
                && string.Equals(upload.UploadId, uploadId, StringComparison.Ordinal)) {
                return upload;
            }
        }

        return null;
    }

    private static S3ListBucketResult BuildListBucketResult(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? marker,
        string? startAfter,
        string? continuationToken,
        int maxKeys,
        IReadOnlyList<ObjectInfo> objects,
        bool isV2,
        bool includeOwner,
        string? encodingType,
        S3BucketOwner owner)
    {
        var normalizedPrefix = prefix ?? string.Empty;
        var normalizedDelimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter;
        var markerValue = isV2 && string.IsNullOrWhiteSpace(continuationToken)
            ? startAfter
            : isV2 ? continuationToken : marker;

        var entries = new List<ListBucketResultEntry>();

        for (var index = 0; index < objects.Count; index++) {
            var currentObject = objects[index];
            if (!currentObject.Key.StartsWith(normalizedPrefix, StringComparison.Ordinal)) {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(markerValue)
                && StringComparer.Ordinal.Compare(currentObject.Key, markerValue) <= 0) {
                continue;
            }

            if (!string.IsNullOrEmpty(normalizedDelimiter)) {
                var suffix = currentObject.Key[normalizedPrefix.Length..];
                var delimiterIndex = suffix.IndexOf(normalizedDelimiter, StringComparison.Ordinal);
                if (delimiterIndex >= 0) {
                    var commonPrefix = normalizedPrefix + suffix[..(delimiterIndex + normalizedDelimiter.Length)];
                    var lastObjectKey = currentObject.Key;

                    while (index + 1 < objects.Count) {
                        var nextObject = objects[index + 1];
                        if (!nextObject.Key.StartsWith(commonPrefix, StringComparison.Ordinal)) {
                            break;
                        }

                        lastObjectKey = nextObject.Key;
                        index++;
                    }

                    entries.Add(ListBucketResultEntry.ForCommonPrefix(commonPrefix, lastObjectKey));
                    continue;
                }
            }

            entries.Add(ListBucketResultEntry.ForObject(currentObject));
        }

        var isTruncated = entries.Count > maxKeys;
        var page = isTruncated
            ? entries.Take(maxKeys).ToArray()
            : entries.ToArray();

        return new S3ListBucketResult
        {
            Name = bucketName,
            IsV2 = isV2,
            Prefix = prefix,
            Delimiter = normalizedDelimiter,
            Marker = isV2 ? null : marker,
            StartAfter = isV2 ? startAfter : null,
            ContinuationToken = isV2 ? continuationToken : null,
            NextMarker = !isV2 && isTruncated && normalizedDelimiter is not null ? page[^1].ContinuationToken : null,
            NextContinuationToken = isV2 && isTruncated ? page[^1].ContinuationToken : null,
            EncodingType = encodingType,
            KeyCount = isV2 ? page.Length : 0,
            MaxKeys = maxKeys,
            IsTruncated = isTruncated,
            Contents = page
                .Where(static entry => entry.Object is not null)
                .Select(entry => new S3ListBucketObject
                {
                    Key = entry.Object!.Key,
                    ETag = entry.Object.ETag,
                    Size = entry.Object.ContentLength,
                    LastModifiedUtc = entry.Object.LastModifiedUtc,
                    Owner = includeOwner ? owner : null
                })
                .ToArray(),
            CommonPrefixes = page
                .Where(static entry => entry.CommonPrefix is not null)
                .Select(static entry => new S3ListBucketCommonPrefix
                {
                    Prefix = entry.CommonPrefix!
                })
                .ToArray()
        };
    }

    private static S3ListObjectVersionsResult BuildListObjectVersionsResult(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? versionIdMarker,
        string? encodingType,
        int maxKeys,
        IReadOnlyList<ObjectInfo> versions)
    {
        var normalizedPrefix = prefix ?? string.Empty;
        var normalizedDelimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter;
        var entries = new List<ListObjectVersionsResultEntry>();

        for (var index = 0; index < versions.Count; index++) {
            var currentVersion = versions[index];
            if (!currentVersion.Key.StartsWith(normalizedPrefix, StringComparison.Ordinal)) {
                continue;
            }

            if (!IsVersionAfterMarker(currentVersion, keyMarker, versionIdMarker)) {
                continue;
            }

            if (!string.IsNullOrEmpty(normalizedDelimiter)) {
                var suffix = currentVersion.Key[normalizedPrefix.Length..];
                var delimiterIndex = suffix.IndexOf(normalizedDelimiter, StringComparison.Ordinal);
                if (delimiterIndex >= 0) {
                    var commonPrefix = normalizedPrefix + suffix[..(delimiterIndex + normalizedDelimiter.Length)];
                    var lastVersion = currentVersion;

                    while (index + 1 < versions.Count) {
                        var nextVersion = versions[index + 1];
                        if (!nextVersion.Key.StartsWith(commonPrefix, StringComparison.Ordinal)) {
                            break;
                        }

                        lastVersion = nextVersion;
                        index++;
                    }

                    entries.Add(ListObjectVersionsResultEntry.ForCommonPrefix(commonPrefix, lastVersion.Key, lastVersion.VersionId));
                    continue;
                }
            }

            entries.Add(ListObjectVersionsResultEntry.ForVersion(currentVersion));
        }

        var isTruncated = entries.Count > maxKeys;
        var page = isTruncated
            ? entries.Take(maxKeys).ToArray()
            : entries.ToArray();

        return new S3ListObjectVersionsResult
        {
            Name = bucketName,
            Prefix = prefix,
            Delimiter = normalizedDelimiter,
            KeyMarker = keyMarker,
            VersionIdMarker = versionIdMarker,
            NextKeyMarker = isTruncated ? page[^1].NextKeyMarker : null,
            NextVersionIdMarker = isTruncated ? page[^1].NextVersionIdMarker : null,
            EncodingType = encodingType,
            MaxKeys = maxKeys,
            IsTruncated = isTruncated,
            Versions = page
                .Where(static entry => entry.Version is not null)
                .Select(static entry => new S3ObjectVersionEntry
                {
                    Key = entry.Version!.Key,
                    VersionId = entry.Version.VersionId ?? string.Empty,
                    IsLatest = entry.Version.IsLatest,
                    IsDeleteMarker = entry.Version.IsDeleteMarker,
                    ETag = entry.Version.ETag,
                    Size = entry.Version.ContentLength,
                    LastModifiedUtc = entry.Version.LastModifiedUtc
                })
                .ToArray(),
            CommonPrefixes = page
                .Where(static entry => entry.CommonPrefix is not null)
                .Select(static entry => new S3ListBucketCommonPrefix
                {
                    Prefix = entry.CommonPrefix!
                })
                .ToArray()
        };
    }

    private static S3ListMultipartUploadsResult BuildListMultipartUploadsResult(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? uploadIdMarker,
        string? encodingType,
        int maxUploads,
        IReadOnlyList<MultipartUploadInfo> uploads,
        S3BucketOwner owner)
    {
        var normalizedPrefix = prefix ?? string.Empty;
        var normalizedDelimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter;
        var entries = new List<ListMultipartUploadsResultEntry>();

        for (var index = 0; index < uploads.Count; index++) {
            var currentUpload = uploads[index];
            if (!currentUpload.Key.StartsWith(normalizedPrefix, StringComparison.Ordinal)) {
                continue;
            }

            if (!IsMultipartUploadAfterMarker(currentUpload, keyMarker, uploadIdMarker)) {
                continue;
            }

            if (!string.IsNullOrEmpty(normalizedDelimiter)) {
                var suffix = currentUpload.Key[normalizedPrefix.Length..];
                var delimiterIndex = suffix.IndexOf(normalizedDelimiter, StringComparison.Ordinal);
                if (delimiterIndex >= 0) {
                    var commonPrefix = normalizedPrefix + suffix[..(delimiterIndex + normalizedDelimiter.Length)];
                    var lastUpload = currentUpload;

                    while (index + 1 < uploads.Count) {
                        var nextUpload = uploads[index + 1];
                        if (!nextUpload.Key.StartsWith(commonPrefix, StringComparison.Ordinal)) {
                            break;
                        }

                        lastUpload = nextUpload;
                        index++;
                    }

                    entries.Add(ListMultipartUploadsResultEntry.ForCommonPrefix(commonPrefix, lastUpload.Key, lastUpload.UploadId));
                    continue;
                }
            }

            entries.Add(ListMultipartUploadsResultEntry.ForUpload(currentUpload));
        }

        var isTruncated = entries.Count > maxUploads;
        var page = isTruncated
            ? entries.Take(maxUploads).ToArray()
            : entries.ToArray();

        return new S3ListMultipartUploadsResult
        {
            Bucket = bucketName,
            Prefix = prefix,
            Delimiter = normalizedDelimiter,
            EncodingType = encodingType,
            KeyMarker = keyMarker,
            UploadIdMarker = uploadIdMarker,
            NextKeyMarker = isTruncated ? page[^1].NextKeyMarker : null,
            NextUploadIdMarker = isTruncated ? page[^1].NextUploadIdMarker : null,
            MaxUploads = maxUploads,
            IsTruncated = isTruncated,
            Uploads = page
                .Where(static entry => entry.Upload is not null)
                .Select(entry => new S3MultipartUploadEntry
                {
                    Key = entry.Upload!.Key,
                    UploadId = entry.Upload.UploadId,
                    Initiator = owner,
                    Owner = owner,
                    InitiatedAtUtc = entry.Upload.InitiatedAtUtc,
                    ChecksumAlgorithm = ToS3ChecksumAlgorithmValue(entry.Upload.ChecksumAlgorithm)
                })
                .ToArray(),
            CommonPrefixes = page
                .Where(static entry => entry.CommonPrefix is not null)
                .Select(static entry => new S3ListBucketCommonPrefix
                {
                    Prefix = entry.CommonPrefix!
                })
                .ToArray()
        };
    }

    private static S3ListPartsResult BuildListMultipartPartsResult(
        string bucketName,
        string key,
        MultipartUploadInfo upload,
        string? encodingType,
        int partNumberMarker,
        int maxParts,
        IReadOnlyList<MultipartUploadPart> parts,
        S3BucketOwner owner)
    {
        var isTruncated = parts.Count > maxParts;
        var page = isTruncated
            ? parts.Take(maxParts).ToArray()
            : parts.ToArray();

        return new S3ListPartsResult
        {
            Bucket = bucketName,
            Key = key,
            UploadId = upload.UploadId,
            PartNumberMarker = partNumberMarker,
            NextPartNumberMarker = isTruncated ? page[^1].PartNumber : null,
            MaxParts = maxParts,
            IsTruncated = isTruncated,
            EncodingType = encodingType,
            Initiator = owner,
            Owner = owner,
            ChecksumAlgorithm = ToS3ChecksumAlgorithmValue(upload.ChecksumAlgorithm),
            ChecksumType = string.IsNullOrWhiteSpace(upload.ChecksumAlgorithm) ? null : "COMPOSITE",
            Parts = page.Select(static part => new S3ListPartEntry
            {
                PartNumber = part.PartNumber,
                ETag = part.ETag,
                LastModifiedUtc = part.LastModifiedUtc,
                Size = part.ContentLength,
                ChecksumCrc32 = GetChecksumValue(part.Checksums, "crc32"),
                ChecksumCrc32c = GetChecksumValue(part.Checksums, "crc32c"),
                ChecksumCrc64Nvme = GetChecksumValue(part.Checksums, "crc64nvme"),
                ChecksumSha1 = GetChecksumValue(part.Checksums, "sha1"),
                ChecksumSha256 = GetChecksumValue(part.Checksums, "sha256")
            }).ToArray()
        };
    }

    private static int? ParseMaxKeys(HttpRequest request)
    {
        if (!request.Query.TryGetValue(MaxKeysQueryParameterName, out var values)
            || string.IsNullOrWhiteSpace(values.ToString())) {
            return 1000;
        }

        return int.TryParse(values.ToString(), out var parsedValue)
            ? parsedValue
            : throw new ArgumentException("The max-keys query parameter must be an integer.", MaxKeysQueryParameterName);
    }

    private static int? ParseMaxUploads(HttpRequest request)
    {
        if (!request.Query.TryGetValue(MaxUploadsQueryParameterName, out var values)
            || string.IsNullOrWhiteSpace(values.ToString())) {
            return 1000;
        }

        return int.TryParse(values.ToString(), out var parsedValue)
            ? parsedValue
            : throw new ArgumentException("The max-uploads query parameter must be an integer.", MaxUploadsQueryParameterName);
    }

    private static int ParseMaxParts(HttpRequest request)
    {
        if (!request.Query.TryGetValue(MaxPartsQueryParameterName, out var values)
            || string.IsNullOrWhiteSpace(values.ToString())) {
            return 1000;
        }

        return int.TryParse(values.ToString(), out var parsedValue)
            ? parsedValue
            : throw new ArgumentException("The max-parts query parameter must be an integer.", MaxPartsQueryParameterName);
    }

    private static int ParsePartNumberMarker(HttpRequest request)
    {
        if (!request.Query.TryGetValue(PartNumberMarkerQueryParameterName, out var values)
            || string.IsNullOrWhiteSpace(values.ToString())) {
            return 0;
        }

        return int.TryParse(values.ToString(), out var parsedValue)
            ? parsedValue
            : throw new ArgumentException("The part-number-marker query parameter must be an integer.", PartNumberMarkerQueryParameterName);
    }

    private static bool TryValidateBucketRequestSubresources(HttpRequest request, out string? errorCode, out string? errorMessage, out int statusCode)
    {
        var queryKeys = GetValidatedQueryKeys(request);
        var isListObjectsV2Request = IsListObjectsV2Request(request) && queryKeys.IsSubsetOf(BucketListObjectsV2QueryParameters);
        var isListObjectsV1Request = !IsListObjectsV2Request(request) && queryKeys.IsSubsetOf(BucketListObjectsV1QueryParameters);
        var isBucketAclRequest = queryKeys.SetEquals(BucketAclQueryParameters);
        var isBucketLocationRequest = queryKeys.SetEquals(BucketLocationQueryParameters);
        var isBucketCorsRequest = queryKeys.SetEquals(BucketCorsQueryParameters);
        var isBucketPolicyRequest = queryKeys.SetEquals(BucketPolicyQueryParameters);
        var isBucketVersioningRequest = queryKeys.SetEquals(BucketVersioningQueryParameters);
        var isBucketEncryptionRequest = queryKeys.SetEquals(BucketEncryptionQueryParameters);
        var isBucketTaggingRequest = queryKeys.SetEquals(BucketTaggingQueryParameters);
        var isBucketLoggingRequest = queryKeys.SetEquals(BucketLoggingQueryParameters);
        var isBucketWebsiteRequest = queryKeys.SetEquals(BucketWebsiteQueryParameters);
        var isBucketRequestPaymentRequest = queryKeys.SetEquals(BucketRequestPaymentQueryParameters);
        var isBucketAccelerateRequest = queryKeys.SetEquals(BucketAccelerateQueryParameters);
        var isBucketLifecycleRequest = queryKeys.SetEquals(BucketLifecycleQueryParameters);
        var isBucketReplicationRequest = queryKeys.SetEquals(BucketReplicationQueryParameters);
        var isBucketNotificationRequest = queryKeys.SetEquals(BucketNotificationQueryParameters);
        var isBucketObjectLockRequest = queryKeys.SetEquals(BucketObjectLockQueryParameters);
        var isBucketAnalyticsRequest = queryKeys.Contains(AnalyticsQueryParameterName) && queryKeys.IsSubsetOf(BucketAnalyticsQueryParameters);
        var isBucketMetricsRequest = queryKeys.Contains(MetricsQueryParameterName) && queryKeys.IsSubsetOf(BucketMetricsQueryParameters);
        var isBucketInventoryRequest = queryKeys.Contains(InventoryQueryParameterName) && queryKeys.IsSubsetOf(BucketInventoryQueryParameters);
        var isListObjectVersionsRequest = queryKeys.Contains(VersionsQueryParameterName) && queryKeys.IsSubsetOf(BucketVersionListingQueryParameters);
        var isListMultipartUploadsRequest = queryKeys.Contains(UploadsQueryParameterName) && queryKeys.IsSubsetOf(BucketMultipartUploadsQueryParameters);
        var isDeleteObjectsRequest = queryKeys.SetEquals(BucketDeleteQueryParameters);

        switch (request.Method) {
            case "GET":
                if (isListObjectsV1Request || isListObjectsV2Request) {
                    if (request.Query.TryGetValue(ListTypeQueryParameterName, out var listTypeValue)
                        && !string.IsNullOrWhiteSpace(listTypeValue.ToString())
                        && !string.Equals(listTypeValue.ToString(), "2", StringComparison.Ordinal)) {
                        return SetValidationError(
                            "InvalidArgument",
                            "Only list-type=2 is supported for S3-compatible bucket listing.",
                            StatusCodes.Status400BadRequest,
                            out errorCode,
                            out errorMessage,
                            out statusCode);
                    }

                    break;
                }

                if (isBucketLocationRequest
                    || isBucketAclRequest
                    || isBucketCorsRequest
                    || isBucketPolicyRequest
                    || isBucketVersioningRequest
                    || isBucketEncryptionRequest
                    || isBucketTaggingRequest
                    || isBucketLoggingRequest
                    || isBucketWebsiteRequest
                    || isBucketRequestPaymentRequest
                    || isBucketAccelerateRequest
                    || isBucketLifecycleRequest
                    || isBucketReplicationRequest
                    || isBucketNotificationRequest
                    || isBucketObjectLockRequest
                    || isBucketAnalyticsRequest
                    || isBucketMetricsRequest
                    || isBucketInventoryRequest
                    || isListObjectVersionsRequest
                    || isListMultipartUploadsRequest) {
                    break;
                }

                return CreateUnsupportedSubresourceValidationError(queryKeys, KnownBucketQueryParameters, "bucket", out errorCode, out errorMessage, out statusCode);

            case "POST":
                if (queryKeys.Count == 0) {
                    return SetValidationError(
                        "InvalidRequest",
                        "Only POST ?delete is supported for bucket-compatible subresource operations.",
                        StatusCodes.Status400BadRequest,
                        out errorCode,
                        out errorMessage,
                        out statusCode);
                }

                if (isDeleteObjectsRequest) {
                    break;
                }

                return CreateUnsupportedSubresourceValidationError(queryKeys, KnownBucketQueryParameters, "bucket", out errorCode, out errorMessage, out statusCode);

            case "PUT":
                if (queryKeys.SetEquals(EmptyQueryParameters)
                    || isBucketAclRequest
                    || isBucketVersioningRequest
                    || isBucketCorsRequest
                    || isBucketPolicyRequest
                    || isBucketEncryptionRequest
                    || isBucketTaggingRequest
                    || isBucketLoggingRequest
                    || isBucketWebsiteRequest
                    || isBucketRequestPaymentRequest
                    || isBucketAccelerateRequest
                    || isBucketLifecycleRequest
                    || isBucketReplicationRequest
                    || isBucketNotificationRequest
                    || isBucketObjectLockRequest
                    || isBucketAnalyticsRequest
                    || isBucketMetricsRequest
                    || isBucketInventoryRequest) {
                    break;
                }

                return CreateUnsupportedSubresourceValidationError(queryKeys, KnownBucketQueryParameters, "bucket", out errorCode, out errorMessage, out statusCode);

            case "DELETE":
                if (queryKeys.SetEquals(EmptyQueryParameters)
                    || isBucketCorsRequest
                    || isBucketPolicyRequest
                    || isBucketEncryptionRequest
                    || isBucketTaggingRequest
                    || isBucketWebsiteRequest
                    || isBucketLifecycleRequest
                    || isBucketReplicationRequest
                    || isBucketAnalyticsRequest
                    || isBucketMetricsRequest
                    || isBucketInventoryRequest) {
                    break;
                }

                return CreateUnsupportedSubresourceValidationError(queryKeys, KnownBucketQueryParameters, "bucket", out errorCode, out errorMessage, out statusCode);

            case "HEAD":
                if (queryKeys.SetEquals(EmptyQueryParameters)) {
                    break;
                }

                return CreateUnsupportedSubresourceValidationError(queryKeys, KnownBucketQueryParameters, "bucket", out errorCode, out errorMessage, out statusCode);
        }

        return SetValidationSuccess(out errorCode, out errorMessage, out statusCode);
    }

    private static bool TryValidateObjectRequestSubresources(HttpRequest request, out string? errorCode, out string? errorMessage, out int statusCode)
    {
        var queryKeys = GetValidatedQueryKeys(request);
        var isCurrentObjectRequest = queryKeys.SetEquals(EmptyQueryParameters);
        var isVersionedObjectRequest = queryKeys.SetEquals(ObjectVersionQueryParameters);
        var isAclRequest = queryKeys.SetEquals(ObjectAclQueryParameters);
        var isTaggingRequest = queryKeys.Contains(TaggingQueryParameterName) && queryKeys.IsSubsetOf(ObjectTaggingQueryParameters);
        var isRetentionRequest = queryKeys.Contains(RetentionQueryParameterName) && queryKeys.IsSubsetOf(ObjectRetentionQueryParameters);
        var isLegalHoldRequest = queryKeys.Contains(LegalHoldQueryParameterName) && queryKeys.IsSubsetOf(ObjectLegalHoldQueryParameters);
        var isAttributesRequest = queryKeys.Contains(AttributesQueryParameterName) && queryKeys.IsSubsetOf(ObjectAttributesQueryParameters);
        var isRestoreRequest = queryKeys.Contains(RestoreQueryParameterName) && queryKeys.IsSubsetOf(ObjectRestoreQueryParameters);
        var isSelectRequest = queryKeys.Contains(SelectQueryParameterName) && queryKeys.IsSubsetOf(ObjectSelectQueryParameters);
        var isInitiateMultipartRequest = queryKeys.SetEquals(ObjectMultipartInitiateQueryParameters);
        var isUploadMultipartPartRequest = queryKeys.SetEquals(ObjectMultipartPartQueryParameters);
        var isUploadScopedMultipartRequest = queryKeys.SetEquals(ObjectMultipartUploadQueryParameters);
        var isListMultipartPartsRequest = queryKeys.Contains(UploadIdQueryParameterName) && queryKeys.IsSubsetOf(ObjectMultipartListPartsQueryParameters);

        if (isSelectRequest
            && request.Query.TryGetValue(SelectTypeQueryParameterName, out var selectTypeValue)
            && !string.IsNullOrWhiteSpace(selectTypeValue.ToString())
            && !string.Equals(selectTypeValue.ToString(), "2", StringComparison.Ordinal)) {
            return SetValidationError(
                "InvalidArgument",
                "Only select-type=2 is supported for S3 Select requests.",
                StatusCodes.Status400BadRequest,
                out errorCode,
                out errorMessage,
                out statusCode);
        }

        switch (request.Method) {
            case "GET" when isCurrentObjectRequest || isVersionedObjectRequest || isAclRequest || isTaggingRequest || isRetentionRequest || isLegalHoldRequest || isAttributesRequest || isListMultipartPartsRequest:
            case "PUT" when isCurrentObjectRequest || isAclRequest || isTaggingRequest || isRetentionRequest || isLegalHoldRequest || isUploadMultipartPartRequest:
            case "HEAD" when isCurrentObjectRequest || isVersionedObjectRequest:
            case "DELETE" when isCurrentObjectRequest || isVersionedObjectRequest || isTaggingRequest || isUploadScopedMultipartRequest:
            case "POST" when isCurrentObjectRequest || isInitiateMultipartRequest || isUploadScopedMultipartRequest || isRestoreRequest || isSelectRequest:
                return SetValidationSuccess(out errorCode, out errorMessage, out statusCode);
            case "GET":
            case "PUT":
            case "HEAD":
            case "DELETE":
            case "POST":
                return CreateUnsupportedSubresourceValidationError(queryKeys, KnownObjectQueryParameters, "object", out errorCode, out errorMessage, out statusCode);
        }

        return SetValidationSuccess(out errorCode, out errorMessage, out statusCode);
    }

    private static async Task AuthorizeCompatibilityOperationAsync(
        HttpContext httpContext,
        IIntegratedS3AuthorizationService authorizationService,
        StorageAuthorizationRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(authorizationService);
        ArgumentNullException.ThrowIfNull(request);

        var result = await authorizationService.AuthorizeAsync(httpContext.User, request, cancellationToken);
        if (result.IsSuccess) {
            return;
        }

        throw new EndpointStorageAuthorizationException(result.Error ?? new StorageError
        {
            Code = StorageErrorCode.AccessDenied,
            Message = $"The current principal is not authorized to perform '{request.Operation}'.",
            BucketName = request.BucketName,
            ObjectKey = request.Key,
            SuggestedHttpStatusCode = StatusCodes.Status403Forbidden
        });
    }

    private static async Task<(StorageCannedAcl? CannedAcl, IResult? ErrorResult)> TryReadAclSubresourceRequestAsync(
        HttpContext httpContext,
        string bucketName,
        string? key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        var request = httpContext.Request;
        var resource = BuildObjectResource(bucketName, key);
        var unsupportedHeaderName = FindUnsupportedAclGrantHeader(request);
        if (!string.IsNullOrWhiteSpace(unsupportedHeaderName)) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"ACL request header '{unsupportedHeaderName}' is not implemented.",
                resource,
                bucketName,
                key));
        }

        var hasCannedAclHeader = request.Headers.ContainsKey(CannedAclHeaderName);
        var hasBody = RequestHasBody(request);
        if (hasCannedAclHeader && hasBody) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The ACL request must not include both '{CannedAclHeaderName}' and an AccessControlPolicy body.",
                resource,
                bucketName,
                key));
        }

        if (hasCannedAclHeader) {
            return TryParseCannedAclHeader(request, resource, bucketName, key, out var cannedAcl, out var errorResult)
                ? (cannedAcl, null)
                : (null, errorResult);
        }

        if (!hasBody) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The ACL request must include a supported '{CannedAclHeaderName}' header or an AccessControlPolicy body.",
                resource,
                bucketName,
                key));
        }

        S3AccessControlPolicy accessControlPolicy;
        try {
            accessControlPolicy = await S3XmlRequestReader.ReadAccessControlPolicyAsync(request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "MalformedACLError",
                exception.Message,
                resource,
                bucketName,
                key));
        }

        return TryResolveCannedAcl(accessControlPolicy, httpContext, resource, bucketName, key, out var resolvedAcl, out var aclErrorResult)
            ? (resolvedAcl, null)
            : (null, aclErrorResult);
    }

    private static async Task<(ObjectAclCompatibilityState? Acl, IResult? ErrorResult)> TryReadObjectAclSubresourceRequestAsync(
        HttpContext httpContext,
        string bucketName,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        var request = httpContext.Request;
        var resource = BuildObjectResource(bucketName, key);
        var hasCannedAclHeader = request.Headers.ContainsKey(CannedAclHeaderName);
        var hasGrantHeaders = HasAnyObjectAclGrantHeader(request);
        var hasBody = RequestHasBody(request);

        if (hasCannedAclHeader && hasGrantHeaders) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The ACL request must not include both '{CannedAclHeaderName}' and grant headers.",
                resource,
                bucketName,
                key));
        }

        if ((hasCannedAclHeader || hasGrantHeaders) && hasBody) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                "The ACL request must not include both ACL headers and an AccessControlPolicy body.",
                resource,
                bucketName,
                key));
        }

        if (hasGrantHeaders) {
            return TryParseObjectAclGrantHeaders(request, resource, bucketName, key, out var parsedAcl, out var grantErrorResult)
                ? (parsedAcl, null)
                : (null, grantErrorResult);
        }

        if (hasCannedAclHeader) {
            return TryParseCannedAclHeader(request, resource, bucketName, key, out var cannedAcl, out var errorResult)
                ? (CreateObjectAclCompatibilityState(cannedAcl!.Value), null)
                : (null, errorResult);
        }

        if (!hasBody) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The ACL request must include a supported '{CannedAclHeaderName}' header, supported object ACL grant headers, or an AccessControlPolicy body.",
                resource,
                bucketName,
                key));
        }

        S3AccessControlPolicy accessControlPolicy;
        try {
            accessControlPolicy = await S3XmlRequestReader.ReadAccessControlPolicyAsync(request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "MalformedACLError",
                exception.Message,
                resource,
                bucketName,
                key));
        }

        return TryResolveObjectAcl(accessControlPolicy, httpContext, resource, bucketName, key, out var resolvedAcl, out var aclErrorResult)
            ? (resolvedAcl, null)
            : (null, aclErrorResult);
    }

    private static async Task<(BucketPolicyCompatibilityDocument? Policy, IResult? ErrorResult)> TryReadBucketPolicyDocumentAsync(
        HttpContext httpContext,
        string bucketName,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        using var reader = new StreamReader(httpContext.Request.Body, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
        var json = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(json)) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "MalformedPolicy",
                "The bucket policy request body is required.",
                BuildObjectResource(bucketName, null),
                bucketName));
        }

        try {
            using var document = JsonDocument.Parse(json);
            return TryParseBucketPolicyDocument(httpContext, bucketName, document, out var policy, out var errorResult)
                ? (policy, null)
                : (null, errorResult);
        }
        catch (JsonException) {
            return (null, ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "MalformedPolicy",
                "The bucket policy request body is not valid JSON.",
                BuildObjectResource(bucketName, null),
                bucketName));
        }
    }

    private static bool TryParseBucketPolicyDocument(
        HttpContext httpContext,
        string bucketName,
        JsonDocument document,
        out BucketPolicyCompatibilityDocument? policy,
        out IResult? errorResult)
    {
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object) {
            policy = null;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "MalformedPolicy",
                "The bucket policy request body must contain a JSON object.",
                BuildObjectResource(bucketName, null),
                bucketName);
            return false;
        }

        if (root.TryGetProperty("Condition", out _)
            || root.TryGetProperty("NotAction", out _)
            || root.TryGetProperty("NotPrincipal", out _)
            || root.TryGetProperty("NotResource", out _)) {
            policy = null;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                "Bucket policy conditions and negative match clauses are not implemented.",
                BuildObjectResource(bucketName, null),
                bucketName);
            return false;
        }

        if (!root.TryGetProperty("Statement", out var statementElement)) {
            policy = null;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "MalformedPolicy",
                "The bucket policy must contain a 'Statement' property.",
                BuildObjectResource(bucketName, null),
                bucketName);
            return false;
        }

        var statements = statementElement.ValueKind switch
        {
            JsonValueKind.Object => [statementElement],
            JsonValueKind.Array => statementElement.EnumerateArray().ToArray(),
            _ => null
        };

        if (statements is null || statements.Length == 0) {
            policy = null;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status400BadRequest,
                "MalformedPolicy",
                "The bucket policy 'Statement' property must contain at least one statement object.",
                BuildObjectResource(bucketName, null),
                bucketName);
            return false;
        }

        var bucketArn = $"arn:aws:s3:::{bucketName}";
        var objectArn = $"{bucketArn}/*";
        var allowsPublicList = false;
        var allowsPublicRead = false;

        foreach (var statement in statements) {
            if (statement.ValueKind != JsonValueKind.Object) {
                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status400BadRequest,
                    "MalformedPolicy",
                    "Each bucket policy statement must be a JSON object.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }

            if (statement.TryGetProperty("Condition", out _)
                || statement.TryGetProperty("NotAction", out _)
                || statement.TryGetProperty("NotPrincipal", out _)
                || statement.TryGetProperty("NotResource", out _)) {
                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status501NotImplemented,
                    "NotImplemented",
                    "Bucket policy conditions and negative match clauses are not implemented.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }

            if (!TryReadRequiredStringProperty(statement, "Effect", out var effect)) {
                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status400BadRequest,
                    "MalformedPolicy",
                    "Each bucket policy statement must contain a non-empty 'Effect' property.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }

            if (!string.Equals(effect, "Allow", StringComparison.Ordinal) &&
                !string.Equals(effect, "Deny", StringComparison.Ordinal)) {
                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status400BadRequest,
                    "MalformedPolicy",
                    $"Invalid bucket policy effect '{effect}'. Must be 'Allow' or 'Deny'.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }

            if (!statement.TryGetProperty("Principal", out var principalElement)) {
                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status400BadRequest,
                    "MalformedPolicy",
                    "Each bucket policy statement must contain a 'Principal' property.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }

            if (!IsAnonymousPrincipal(principalElement)) {
                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status501NotImplemented,
                    "NotImplemented",
                    "Only anonymous bucket policy principals are implemented.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }

            if (!TryReadStringListProperty(statement, "Action", out var actions)) {
                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status400BadRequest,
                    "MalformedPolicy",
                    "Each bucket policy statement must contain a non-empty 'Action' property.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }

            if (!TryReadStringListProperty(statement, "Resource", out var resources)) {
                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status400BadRequest,
                    "MalformedPolicy",
                    "Each bucket policy statement must contain a non-empty 'Resource' property.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }

            var hasBucketResource = false;
            var hasObjectResource = false;
            foreach (var resource in resources) {
                if (string.Equals(resource, bucketArn, StringComparison.Ordinal)) {
                    hasBucketResource = true;
                    continue;
                }

                if (string.Equals(resource, objectArn, StringComparison.Ordinal)) {
                    hasObjectResource = true;
                    continue;
                }

                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status501NotImplemented,
                    "NotImplemented",
                    $"Bucket policy resource '{resource}' is not implemented.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }

            foreach (var action in actions) {
                if (string.Equals(action, "s3:ListBucket", StringComparison.Ordinal)) {
                    if (!hasBucketResource) {
                        policy = null;
                        errorResult = ToErrorResult(
                            httpContext,
                            StatusCodes.Status501NotImplemented,
                            "NotImplemented",
                            "Bucket policy action 's3:ListBucket' requires the bucket ARN resource.",
                            BuildObjectResource(bucketName, null),
                            bucketName);
                        return false;
                    }

                    allowsPublicList = true;
                    continue;
                }

                if (string.Equals(action, "s3:GetObject", StringComparison.Ordinal)
                    || string.Equals(action, "s3:GetObjectVersion", StringComparison.Ordinal)) {
                    if (!hasObjectResource) {
                        policy = null;
                        errorResult = ToErrorResult(
                            httpContext,
                            StatusCodes.Status501NotImplemented,
                            "NotImplemented",
                            $"Bucket policy action '{action}' requires the bucket object ARN resource.",
                            BuildObjectResource(bucketName, null),
                            bucketName);
                        return false;
                    }

                    allowsPublicRead = true;
                    continue;
                }

                policy = null;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status501NotImplemented,
                    "NotImplemented",
                    $"Bucket policy action '{action}' is not implemented.",
                    BuildObjectResource(bucketName, null),
                    bucketName);
                return false;
            }
        }

        policy = new BucketPolicyCompatibilityDocument
        {
            Document = NormalizeJson(document),
            AllowsPublicList = allowsPublicList,
            AllowsPublicRead = allowsPublicRead
        };
        errorResult = null;
        return true;
    }

    private static async Task<IResult?> ApplyRequestedObjectAclAsync(
        HttpContext httpContext,
        IStorageAuthorizationCompatibilityService compatibilityService,
        string bucketName,
        string key,
        ObjectAclCompatibilityState? acl,
        CancellationToken cancellationToken)
    {
        if (acl is null) {
            return null;
        }

        var aclResult = await compatibilityService.PutObjectAclAsync(new PutObjectAclCompatibilityRequest
        {
            BucketName = bucketName,
            Key = key,
            CannedAcl = acl.CannedAcl,
            Acl = acl
        }, cancellationToken);

        return aclResult.IsSuccess
            ? null
            : ToErrorResult(httpContext, aclResult.Error, resourceOverride: BuildObjectResource(bucketName, key));
    }

    private static bool TryParseOptionalWriteObjectAcl(
        HttpRequest request,
        string resource,
        string bucketName,
        string key,
        out ObjectAclCompatibilityState? acl,
        out IResult? errorResult)
    {
        ArgumentNullException.ThrowIfNull(request);

        var hasCannedAclHeader = request.Headers.ContainsKey(CannedAclHeaderName);
        var hasGrantHeaders = HasAnyObjectAclGrantHeader(request);

        if (hasCannedAclHeader && hasGrantHeaders) {
            acl = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The ACL request must not include both '{CannedAclHeaderName}' and grant headers.",
                resource,
                bucketName,
                key);
            return false;
        }

        if (hasCannedAclHeader) {
            if (!TryParseCannedAclHeader(request, resource, bucketName, key, out var cannedAcl, out errorResult)) {
                acl = null;
                return false;
            }

            acl = CreateObjectAclCompatibilityState(cannedAcl!.Value);
            errorResult = null;
            return true;
        }

        if (!hasGrantHeaders) {
            acl = null;
            errorResult = null;
            return true;
        }

        return TryParseObjectAclGrantHeaders(request, resource, bucketName, key, out acl, out errorResult);
    }

    private static bool TryParseOptionalWriteCannedAcl(
        HttpRequest request,
        string resource,
        string? bucketName,
        string? key,
        out StorageCannedAcl? cannedAcl,
        out IResult? errorResult)
    {
        ArgumentNullException.ThrowIfNull(request);

        var unsupportedHeaderName = FindUnsupportedAclGrantHeader(request);
        if (!string.IsNullOrWhiteSpace(unsupportedHeaderName)) {
            cannedAcl = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"ACL request header '{unsupportedHeaderName}' is not implemented.",
                resource,
                bucketName,
                key);
            return false;
        }

        if (!request.Headers.ContainsKey(CannedAclHeaderName)) {
            cannedAcl = null;
            errorResult = null;
            return true;
        }

        return TryParseCannedAclHeader(request, resource, bucketName, key, out cannedAcl, out errorResult);
    }

    private static bool TryParseCannedAclHeader(
        HttpRequest request,
        string resource,
        string? bucketName,
        string? key,
        out StorageCannedAcl? cannedAcl,
        out IResult? errorResult)
    {
        var rawValue = request.Headers[CannedAclHeaderName].ToString();
        if (string.IsNullOrWhiteSpace(rawValue)) {
            cannedAcl = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                $"The '{CannedAclHeaderName}' header must not be empty.",
                resource,
                bucketName,
                key);
            return false;
        }

        rawValue = rawValue.Trim();
        if (!TryParseSupportedCannedAcl(rawValue, out var parsedAcl)) {
            cannedAcl = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"Canned ACL '{rawValue}' is not implemented.",
                resource,
                bucketName,
                key);
            return false;
        }

        cannedAcl = parsedAcl;
        errorResult = null;
        return true;
    }

    private static bool TryParseObjectAclGrantHeaders(
        HttpRequest request,
        string resource,
        string bucketName,
        string? key,
        out ObjectAclCompatibilityState? acl,
        out IResult? errorResult)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.Headers.ContainsKey(GrantWriteHeaderName)) {
            acl = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"ACL request header '{GrantWriteHeaderName}' is not implemented.",
                resource,
                bucketName,
                key);
            return false;
        }

        List<StorageAclGrant> grants = [];
        if (!TryAppendObjectAclGrantHeaderValues(request, GrantReadHeaderName, StorageAclPermission.Read, grants, resource, bucketName, key, out errorResult)
            || !TryAppendObjectAclGrantHeaderValues(request, GrantReadAcpHeaderName, StorageAclPermission.ReadAcp, grants, resource, bucketName, key, out errorResult)
            || !TryAppendObjectAclGrantHeaderValues(request, GrantWriteAcpHeaderName, StorageAclPermission.WriteAcp, grants, resource, bucketName, key, out errorResult)
            || !TryAppendObjectAclGrantHeaderValues(request, GrantFullControlHeaderName, StorageAclPermission.FullControl, grants, resource, bucketName, key, out errorResult)) {
            acl = null;
            return false;
        }

        acl = new ObjectAclCompatibilityState
        {
            CannedAcl = ResolveObjectCannedAclFromGrants(grants),
            AdditionalGrants = DeduplicateAclGrants(grants)
        };
        errorResult = null;
        return true;
    }

    private static bool TryAppendObjectAclGrantHeaderValues(
        HttpRequest request,
        string headerName,
        StorageAclPermission permission,
        List<StorageAclGrant> grants,
        string resource,
        string bucketName,
        string? key,
        out IResult? errorResult)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(grants);

        if (!request.Headers.ContainsKey(headerName)) {
            errorResult = null;
            return true;
        }

        var rawValue = request.Headers[headerName].ToString();
        if (string.IsNullOrWhiteSpace(rawValue)) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                $"The '{headerName}' header must not be empty.",
                resource,
                bucketName,
                key);
            return false;
        }

        if (!TryParseAclGrantHeaderValue(rawValue, out var grantees)) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                $"The '{headerName}' header value is not valid.",
                resource,
                bucketName,
                key);
            return false;
        }

        foreach (var grantee in grantees) {
            if (string.Equals(grantee.Type, "id", StringComparison.OrdinalIgnoreCase)) {
                grants.Add(new StorageAclGrant
                {
                    Grantee = new StorageAclGrantee
                    {
                        Type = StorageAclGranteeType.CanonicalUser,
                        Id = grantee.Value
                    },
                    Permission = permission
                });

                continue;
            }

            if (string.Equals(grantee.Type, "uri", StringComparison.OrdinalIgnoreCase)) {
                if (!string.Equals(grantee.Value, AllUsersGroupUri, StringComparison.Ordinal)) {
                    errorResult = ToErrorResult(
                        request.HttpContext,
                        StatusCodes.Status501NotImplemented,
                        "NotImplemented",
                        $"ACL group '{grantee.Value}' is not implemented.",
                        resource,
                        bucketName,
                        key);
                    return false;
                }

                if (permission != StorageAclPermission.Read) {
                    errorResult = ToErrorResult(
                        request.HttpContext,
                        StatusCodes.Status501NotImplemented,
                        "NotImplemented",
                        $"ACL permission '{ToS3AclPermission(permission)}' is not implemented for the AllUsers group.",
                        resource,
                        bucketName,
                        key);
                    return false;
                }

                grants.Add(new StorageAclGrant
                {
                    Grantee = new StorageAclGrantee
                    {
                        Type = StorageAclGranteeType.Group,
                        Uri = AllUsersGroupUri
                    },
                    Permission = StorageAclPermission.Read
                });

                continue;
            }

            var granteeType = grantee.Type.Trim();
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"ACL grantee type '{granteeType}' is not implemented.",
                resource,
                bucketName,
                key);
            return false;
        }

        errorResult = null;
        return true;
    }

    private static bool TryParseAclGrantHeaderValue(string rawValue, out List<AclHeaderGrantee> grantees)
    {
        grantees = [];
        if (string.IsNullOrWhiteSpace(rawValue)) {
            return false;
        }

        var index = 0;
        while (index < rawValue.Length) {
            SkipWhitespace(rawValue, ref index);
            if (index >= rawValue.Length) {
                return false;
            }

            var typeStart = index;
            while (index < rawValue.Length && rawValue[index] != '=' && rawValue[index] != ',') {
                index++;
            }

            if (index >= rawValue.Length || rawValue[index] != '=') {
                return false;
            }

            var type = rawValue[typeStart..index].Trim();
            if (string.IsNullOrWhiteSpace(type)) {
                return false;
            }

            index++;
            SkipWhitespace(rawValue, ref index);
            if (index >= rawValue.Length || rawValue[index] != '"') {
                return false;
            }

            index++;
            var valueStart = index;
            while (index < rawValue.Length && rawValue[index] != '"') {
                index++;
            }

            if (index >= rawValue.Length) {
                return false;
            }

            var value = rawValue[valueStart..index].Trim();
            if (string.IsNullOrWhiteSpace(value)) {
                return false;
            }

            grantees.Add(new AclHeaderGrantee(type, value));

            index++;
            SkipWhitespace(rawValue, ref index);
            if (index >= rawValue.Length) {
                break;
            }

            if (rawValue[index] != ',') {
                return false;
            }

            index++;
        }

        return grantees.Count > 0;
    }

    private static bool TryResolveObjectAcl(
        S3AccessControlPolicy policy,
        HttpContext httpContext,
        string resource,
        string bucketName,
        string key,
        out ObjectAclCompatibilityState acl,
        out IResult? errorResult)
    {
        ArgumentNullException.ThrowIfNull(policy);

        List<StorageAclGrant> grants = [];
        foreach (var grant in policy.Grants) {
            if (string.Equals(grant.Grantee.Type, CanonicalUserGranteeType, StringComparison.OrdinalIgnoreCase)) {
                if (string.IsNullOrWhiteSpace(grant.Grantee.Id)) {
                    acl = default!;
                    errorResult = ToErrorResult(
                        httpContext,
                        StatusCodes.Status400BadRequest,
                        "MalformedACLError",
                        "CanonicalUser ACL grants must include a non-empty 'ID' element.",
                        resource,
                        bucketName,
                        key);
                    return false;
                }

                if (!TryParseSupportedCanonicalUserAclPermission(grant.Permission, out var permission)) {
                    acl = default!;
                    errorResult = ToErrorResult(
                        httpContext,
                        StatusCodes.Status501NotImplemented,
                        "NotImplemented",
                        $"ACL permission '{grant.Permission}' is not implemented for canonical grantees.",
                        resource,
                        bucketName,
                        key);
                    return false;
                }

                grants.Add(new StorageAclGrant
                {
                    Grantee = new StorageAclGrantee
                    {
                        Type = StorageAclGranteeType.CanonicalUser,
                        Id = grant.Grantee.Id.Trim()
                    },
                    Permission = permission
                });

                continue;
            }

            if (string.Equals(grant.Grantee.Type, GroupGranteeType, StringComparison.OrdinalIgnoreCase)) {
                if (string.IsNullOrWhiteSpace(grant.Grantee.Uri)) {
                    acl = default!;
                    errorResult = ToErrorResult(
                        httpContext,
                        StatusCodes.Status400BadRequest,
                        "MalformedACLError",
                        "Group ACL grants must include a non-empty 'URI' element.",
                        resource,
                        bucketName,
                        key);
                    return false;
                }

                if (string.Equals(grant.Grantee.Uri, AllUsersGroupUri, StringComparison.Ordinal)) {
                    if (!TryParseGroupAclPermission(grant.Permission, out var allUsersPermission)) {
                        acl = default!;
                        errorResult = ToErrorResult(
                            httpContext,
                            StatusCodes.Status501NotImplemented,
                            "NotImplemented",
                            $"ACL permission '{grant.Permission}' is not implemented for the AllUsers group.",
                            resource,
                            bucketName,
                            key);
                        return false;
                    }

                    grants.Add(new StorageAclGrant
                    {
                        Grantee = new StorageAclGrantee
                        {
                            Type = StorageAclGranteeType.Group,
                            Uri = AllUsersGroupUri
                        },
                        Permission = allUsersPermission
                    });

                    continue;
                }

                if (string.Equals(grant.Grantee.Uri, AuthenticatedUsersGroupUri, StringComparison.Ordinal)) {
                    if (!TryParseGroupAclPermission(grant.Permission, out var authUsersPermission)) {
                        acl = default!;
                        errorResult = ToErrorResult(
                            httpContext,
                            StatusCodes.Status501NotImplemented,
                            "NotImplemented",
                            $"ACL permission '{grant.Permission}' is not implemented for the AuthenticatedUsers group.",
                            resource,
                            bucketName,
                            key);
                        return false;
                    }

                    grants.Add(new StorageAclGrant
                    {
                        Grantee = new StorageAclGrantee
                        {
                            Type = StorageAclGranteeType.Group,
                            Uri = AuthenticatedUsersGroupUri
                        },
                        Permission = authUsersPermission
                    });

                    continue;
                }

                acl = default!;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status501NotImplemented,
                    "NotImplemented",
                    $"ACL group '{grant.Grantee.Uri}' is not implemented.",
                    resource,
                    bucketName,
                    key);
                return false;
            }

            acl = default!;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"ACL grantee type '{grant.Grantee.Type}' is not implemented.",
                resource,
                bucketName,
                key);
            return false;
        }

        acl = new ObjectAclCompatibilityState
        {
            CannedAcl = ResolveObjectCannedAclFromGrants(grants),
            AdditionalGrants = DeduplicateAclGrants(grants)
        };
        errorResult = null;
        return true;
    }

    private static bool TryResolveCannedAcl(
        S3AccessControlPolicy policy,
        HttpContext httpContext,
        string resource,
        string bucketName,
        string? key,
        out StorageCannedAcl cannedAcl,
        out IResult? errorResult)
    {
        ArgumentNullException.ThrowIfNull(policy);

        var allowsPublicRead = false;
        var allowsPublicWrite = false;
        var allowsAuthenticatedRead = false;
        foreach (var grant in policy.Grants) {
            if (string.Equals(grant.Grantee.Type, CanonicalUserGranteeType, StringComparison.OrdinalIgnoreCase)) {
                if (!string.Equals(grant.Permission, "FULL_CONTROL", StringComparison.OrdinalIgnoreCase)) {
                    cannedAcl = default;
                    errorResult = ToErrorResult(
                        httpContext,
                        StatusCodes.Status501NotImplemented,
                        "NotImplemented",
                        $"ACL permission '{grant.Permission}' is not implemented for canonical grantees.",
                        resource,
                        bucketName,
                        key);
                    return false;
                }

                continue;
            }

            if (string.Equals(grant.Grantee.Type, GroupGranteeType, StringComparison.OrdinalIgnoreCase)) {
                if (string.Equals(grant.Grantee.Uri, AllUsersGroupUri, StringComparison.Ordinal)) {
                    if (string.Equals(grant.Permission, "READ", StringComparison.OrdinalIgnoreCase)) {
                        allowsPublicRead = true;
                        continue;
                    }

                    if (string.Equals(grant.Permission, "WRITE", StringComparison.OrdinalIgnoreCase)) {
                        allowsPublicWrite = true;
                        continue;
                    }

                    cannedAcl = default;
                    errorResult = ToErrorResult(
                        httpContext,
                        StatusCodes.Status501NotImplemented,
                        "NotImplemented",
                        $"ACL permission '{grant.Permission}' is not implemented for the AllUsers group.",
                        resource,
                        bucketName,
                        key);
                    return false;
                }

                if (string.Equals(grant.Grantee.Uri, AuthenticatedUsersGroupUri, StringComparison.Ordinal)) {
                    if (string.Equals(grant.Permission, "READ", StringComparison.OrdinalIgnoreCase)) {
                        allowsAuthenticatedRead = true;
                        continue;
                    }

                    cannedAcl = default;
                    errorResult = ToErrorResult(
                        httpContext,
                        StatusCodes.Status501NotImplemented,
                        "NotImplemented",
                        $"ACL permission '{grant.Permission}' is not implemented for the AuthenticatedUsers group.",
                        resource,
                        bucketName,
                        key);
                    return false;
                }

                cannedAcl = default;
                errorResult = ToErrorResult(
                    httpContext,
                    StatusCodes.Status501NotImplemented,
                    "NotImplemented",
                    $"ACL group '{grant.Grantee.Uri}' is not implemented.",
                    resource,
                    bucketName,
                    key);
                return false;
            }

            cannedAcl = default;
            errorResult = ToErrorResult(
                httpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"ACL grantee type '{grant.Grantee.Type}' is not implemented.",
                resource,
                bucketName,
                key);
            return false;
        }

        if (allowsPublicRead && allowsPublicWrite) {
            cannedAcl = StorageCannedAcl.PublicReadWrite;
        }
        else if (allowsPublicRead) {
            cannedAcl = StorageCannedAcl.PublicRead;
        }
        else if (allowsAuthenticatedRead) {
            cannedAcl = StorageCannedAcl.AuthenticatedRead;
        }
        else {
            cannedAcl = StorageCannedAcl.Private;
        }

        errorResult = null;
        return true;
    }

    private static bool TryParseSupportedCannedAcl(string rawValue, out StorageCannedAcl cannedAcl)
    {
        switch (rawValue.Trim().ToLowerInvariant()) {
            case "private":
                cannedAcl = StorageCannedAcl.Private;
                return true;
            case "public-read":
                cannedAcl = StorageCannedAcl.PublicRead;
                return true;
            case "public-read-write":
                cannedAcl = StorageCannedAcl.PublicReadWrite;
                return true;
            case "authenticated-read":
                cannedAcl = StorageCannedAcl.AuthenticatedRead;
                return true;
            case "bucket-owner-full-control":
                cannedAcl = StorageCannedAcl.BucketOwnerFullControl;
                return true;
            default:
                cannedAcl = default;
                return false;
        }
    }

    private static bool TryParseSupportedCanonicalUserAclPermission(string rawValue, out StorageAclPermission permission)
    {
        switch (rawValue.Trim().ToUpperInvariant()) {
            case "READ":
                permission = StorageAclPermission.Read;
                return true;
            case "READ_ACP":
                permission = StorageAclPermission.ReadAcp;
                return true;
            case "WRITE_ACP":
                permission = StorageAclPermission.WriteAcp;
                return true;
            case "FULL_CONTROL":
                permission = StorageAclPermission.FullControl;
                return true;
            default:
                permission = default;
                return false;
        }
    }

    private static ObjectAclCompatibilityState CreateObjectAclCompatibilityState(StorageCannedAcl cannedAcl)
    {
        return new ObjectAclCompatibilityState
        {
            CannedAcl = cannedAcl,
            AdditionalGrants = []
        };
    }

    private static bool HasAnyObjectAclGrantHeader(HttpRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return request.Headers.ContainsKey(GrantFullControlHeaderName)
            || request.Headers.ContainsKey(GrantReadHeaderName)
            || request.Headers.ContainsKey(GrantReadAcpHeaderName)
            || request.Headers.ContainsKey(GrantWriteHeaderName)
            || request.Headers.ContainsKey(GrantWriteAcpHeaderName);
    }

    private static IReadOnlyList<StorageAclGrant> DeduplicateAclGrants(IEnumerable<StorageAclGrant> grants)
    {
        ArgumentNullException.ThrowIfNull(grants);

        List<StorageAclGrant> distinctGrants = [];
        HashSet<StorageAclGrant> seen = [];
        foreach (var grant in grants) {
            if (seen.Add(grant)) {
                distinctGrants.Add(grant);
            }
        }

        return distinctGrants;
    }

    private static bool IsAllUsersReadGrant(StorageAclGrant grant)
    {
        ArgumentNullException.ThrowIfNull(grant);

        return grant.Permission == StorageAclPermission.Read
            && grant.Grantee.Type == StorageAclGranteeType.Group
            && string.Equals(grant.Grantee.Uri, AllUsersGroupUri, StringComparison.Ordinal);
    }

    private static bool TryParseGroupAclPermission(string rawValue, out StorageAclPermission permission)
    {
        switch (rawValue.Trim().ToUpperInvariant()) {
            case "READ":
                permission = StorageAclPermission.Read;
                return true;
            case "WRITE":
                permission = StorageAclPermission.Write;
                return true;
            case "READ_ACP":
                permission = StorageAclPermission.ReadAcp;
                return true;
            case "WRITE_ACP":
                permission = StorageAclPermission.WriteAcp;
                return true;
            case "FULL_CONTROL":
                permission = StorageAclPermission.FullControl;
                return true;
            default:
                permission = default;
                return false;
        }
    }

    private static StorageCannedAcl ResolveObjectCannedAclFromGrants(IReadOnlyList<StorageAclGrant> grants)
    {
        var hasAllUsersRead = grants.Any(IsAllUsersReadGrant);
        var hasAllUsersWrite = grants.Any(static g =>
            g.Permission == StorageAclPermission.Write
            && g.Grantee.Type == StorageAclGranteeType.Group
            && string.Equals(g.Grantee.Uri, AllUsersGroupUri, StringComparison.Ordinal));
        var hasAuthenticatedRead = grants.Any(static g =>
            g.Permission == StorageAclPermission.Read
            && g.Grantee.Type == StorageAclGranteeType.Group
            && string.Equals(g.Grantee.Uri, AuthenticatedUsersGroupUri, StringComparison.Ordinal));

        if (hasAllUsersRead && hasAllUsersWrite) {
            return StorageCannedAcl.PublicReadWrite;
        }

        if (hasAllUsersRead) {
            return StorageCannedAcl.PublicRead;
        }

        if (hasAuthenticatedRead) {
            return StorageCannedAcl.AuthenticatedRead;
        }

        return StorageCannedAcl.Private;
    }

    private static void SkipWhitespace(string value, ref int index)
    {
        while (index < value.Length && char.IsWhiteSpace(value[index])) {
            index++;
        }
    }

    private static bool TryReadRequiredStringProperty(JsonElement element, string propertyName, out string value)
    {
        if (element.TryGetProperty(propertyName, out var property)
            && property.ValueKind == JsonValueKind.String
            && !string.IsNullOrWhiteSpace(property.GetString())) {
            value = property.GetString()!.Trim();
            return true;
        }

        value = string.Empty;
        return false;
    }

    private static bool TryReadStringListProperty(JsonElement element, string propertyName, out string[] values)
    {
        if (!element.TryGetProperty(propertyName, out var property)) {
            values = [];
            return false;
        }

        switch (property.ValueKind) {
            case JsonValueKind.String when !string.IsNullOrWhiteSpace(property.GetString()):
                values = [property.GetString()!.Trim()];
                return true;
            case JsonValueKind.Array:
                values = property.EnumerateArray()
                    .Where(static item => item.ValueKind == JsonValueKind.String)
                    .Select(static item => item.GetString()!.Trim())
                    .Where(static item => !string.IsNullOrWhiteSpace(item))
                    .ToArray();
                return values.Length > 0 && values.Length == property.GetArrayLength();
            default:
                values = [];
                return false;
        }
    }

    private static bool IsAnonymousPrincipal(JsonElement principalElement)
    {
        switch (principalElement.ValueKind) {
            case JsonValueKind.String:
                return string.Equals(principalElement.GetString(), "*", StringComparison.Ordinal);
            case JsonValueKind.Object:
                if (!principalElement.TryGetProperty("AWS", out var awsElement)) {
                    return false;
                }

                return awsElement.ValueKind switch
                {
                    JsonValueKind.String => string.Equals(awsElement.GetString(), "*", StringComparison.Ordinal),
                    JsonValueKind.Array => awsElement.EnumerateArray().All(static item => item.ValueKind == JsonValueKind.String && item.GetString() == "*"),
                    _ => false
                };
            default:
                return false;
        }
    }

    private static string NormalizeJson(JsonDocument document)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream)) {
            document.RootElement.WriteTo(writer);
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static string? FindUnsupportedAclGrantHeader(HttpRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.Headers.ContainsKey(GrantFullControlHeaderName)) {
            return GrantFullControlHeaderName;
        }

        if (request.Headers.ContainsKey(GrantReadHeaderName)) {
            return GrantReadHeaderName;
        }

        if (request.Headers.ContainsKey(GrantReadAcpHeaderName)) {
            return GrantReadAcpHeaderName;
        }

        if (request.Headers.ContainsKey(GrantWriteHeaderName)) {
            return GrantWriteHeaderName;
        }

        return request.Headers.ContainsKey(GrantWriteAcpHeaderName)
            ? GrantWriteAcpHeaderName
            : null;
    }

    private static bool RequestHasBody(HttpRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return request.ContentLength.GetValueOrDefault() > 0
            || request.Headers.ContainsKey(HeaderNames.TransferEncoding);
    }

    private static S3AccessControlPolicy CreateAccessControlPolicy(StorageCannedAcl cannedAcl, string ownerDisplayName)
    {
        List<S3AccessControlGrant> grants =
        [
            new()
            {
                Grantee = new S3AccessControlGrantee
                {
                    Type = CanonicalUserGranteeType,
                    Id = OwnerId,
                    DisplayName = ownerDisplayName
                },
                Permission = "FULL_CONTROL"
            }
        ];

        if (cannedAcl is StorageCannedAcl.PublicRead or StorageCannedAcl.PublicReadWrite) {
            grants.Add(new S3AccessControlGrant
            {
                Grantee = new S3AccessControlGrantee
                {
                    Type = GroupGranteeType,
                    Uri = AllUsersGroupUri
                },
                Permission = "READ"
            });
        }

        if (cannedAcl == StorageCannedAcl.PublicReadWrite) {
            grants.Add(new S3AccessControlGrant
            {
                Grantee = new S3AccessControlGrantee
                {
                    Type = GroupGranteeType,
                    Uri = AllUsersGroupUri
                },
                Permission = "WRITE"
            });
        }

        if (cannedAcl == StorageCannedAcl.AuthenticatedRead) {
            grants.Add(new S3AccessControlGrant
            {
                Grantee = new S3AccessControlGrantee
                {
                    Type = GroupGranteeType,
                    Uri = AuthenticatedUsersGroupUri
                },
                Permission = "READ"
            });
        }

        return new S3AccessControlPolicy
        {
            Owner = new S3BucketOwner
            {
                Id = OwnerId,
                DisplayName = ownerDisplayName
            },
            Grants = grants
        };
    }

    private static S3AccessControlPolicy CreateObjectAccessControlPolicy(ObjectAclCompatibilityState acl, string ownerDisplayName)
    {
        ArgumentNullException.ThrowIfNull(acl);

        List<S3AccessControlGrant> grants =
        [
            new()
            {
                Grantee = new S3AccessControlGrantee
                {
                    Type = CanonicalUserGranteeType,
                    Id = OwnerId,
                    DisplayName = ownerDisplayName
                },
                Permission = "FULL_CONTROL"
            }
        ];

        foreach (var grant in ExpandObjectAclCompatibilityGrants(acl)) {
            if (grant.Grantee.Type == StorageAclGranteeType.CanonicalUser
                && string.Equals(grant.Grantee.Id, OwnerId, StringComparison.Ordinal)
                && grant.Permission == StorageAclPermission.FullControl) {
                continue;
            }

            grants.Add(new S3AccessControlGrant
            {
                Grantee = new S3AccessControlGrantee
                {
                    Type = grant.Grantee.Type == StorageAclGranteeType.CanonicalUser ? CanonicalUserGranteeType : GroupGranteeType,
                    Id = grant.Grantee.Id,
                    Uri = grant.Grantee.Uri
                },
                Permission = ToS3AclPermission(grant.Permission)
            });
        }

        return new S3AccessControlPolicy
        {
            Owner = new S3BucketOwner
            {
                Id = OwnerId,
                DisplayName = ownerDisplayName
            },
            Grants = grants
        };
    }

    private static IReadOnlyList<StorageAclGrant> ExpandObjectAclCompatibilityGrants(ObjectAclCompatibilityState acl)
    {
        List<StorageAclGrant> expandedGrants = [];

        if (acl.CannedAcl is StorageCannedAcl.PublicRead or StorageCannedAcl.PublicReadWrite) {
            expandedGrants.Add(new StorageAclGrant
            {
                Grantee = new StorageAclGrantee
                {
                    Type = StorageAclGranteeType.Group,
                    Uri = AllUsersGroupUri
                },
                Permission = StorageAclPermission.Read
            });
        }

        if (acl.CannedAcl == StorageCannedAcl.PublicReadWrite) {
            expandedGrants.Add(new StorageAclGrant
            {
                Grantee = new StorageAclGrantee
                {
                    Type = StorageAclGranteeType.Group,
                    Uri = AllUsersGroupUri
                },
                Permission = StorageAclPermission.Write
            });
        }

        if (acl.CannedAcl == StorageCannedAcl.AuthenticatedRead) {
            expandedGrants.Add(new StorageAclGrant
            {
                Grantee = new StorageAclGrantee
                {
                    Type = StorageAclGranteeType.Group,
                    Uri = AuthenticatedUsersGroupUri
                },
                Permission = StorageAclPermission.Read
            });
        }

        expandedGrants.AddRange(acl.AdditionalGrants);
        return DeduplicateAclGrants(expandedGrants);
    }

    private static string ToS3AclPermission(StorageAclPermission permission)
    {
        return permission switch
        {
            StorageAclPermission.Read => "READ",
            StorageAclPermission.Write => "WRITE",
            StorageAclPermission.ReadAcp => "READ_ACP",
            StorageAclPermission.WriteAcp => "WRITE_ACP",
            StorageAclPermission.FullControl => "FULL_CONTROL",
            _ => throw new ArgumentOutOfRangeException(nameof(permission))
        };
    }

    private static HashSet<string> GetValidatedQueryKeys(HttpRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return request.Query.Keys
            .Where(static queryKey => !IsSigV4PresignQueryParameter(queryKey))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private static bool IsListObjectsV2Request(HttpRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return request.Query.ContainsKey(ListTypeQueryParameterName);
    }

    private static bool IsSigV4PresignQueryParameter(string? queryKey)
    {
        return !string.IsNullOrWhiteSpace(queryKey)
            && (queryKey.StartsWith("X-Amz-", StringComparison.OrdinalIgnoreCase)
                || string.Equals(queryKey, "x-id", StringComparison.OrdinalIgnoreCase));
    }

    private static HashSet<string> CreateQueryParameterSet(params string[] queryKeys)
    {
        return new HashSet<string>(queryKeys, StringComparer.OrdinalIgnoreCase);
    }

    private static S3BucketOwner ResolveS3ListingIdentity(ClaimsPrincipal principal)
    {
        ArgumentNullException.ThrowIfNull(principal);

        var identifier = principal.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        var displayName = principal.FindFirst(ClaimTypes.Name)?.Value;

        return new S3BucketOwner
        {
            Id = string.IsNullOrWhiteSpace(identifier) ? DefaultS3ListingIdentityId : identifier,
            DisplayName = string.IsNullOrWhiteSpace(displayName) ? string.Empty : displayName
        };
    }

    private static bool SetValidationSuccess(out string? errorCode, out string? errorMessage, out int statusCode)
    {
        errorCode = null;
        errorMessage = null;
        statusCode = StatusCodes.Status200OK;
        return true;
    }

    private static bool SetValidationError(string resultErrorCode, string resultErrorMessage, int resultStatusCode, out string? errorCode, out string? errorMessage, out int statusCode)
    {
        errorCode = resultErrorCode;
        errorMessage = resultErrorMessage;
        statusCode = resultStatusCode;
        return false;
    }

    private static bool CreateUnsupportedSubresourceValidationError(HashSet<string> queryKeys, HashSet<string> knownQueryKeys, string resourceKind, out string? errorCode, out string? errorMessage, out int statusCode)
    {
        ArgumentNullException.ThrowIfNull(queryKeys);
        ArgumentNullException.ThrowIfNull(knownQueryKeys);

        var unsupportedQueryKey = queryKeys
            .Where(queryKey => !knownQueryKeys.Contains(queryKey))
            .OrderBy(static queryKey => queryKey, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(unsupportedQueryKey)) {
            return SetValidationError(
                "NotImplemented",
                $"{GetSubresourceDisplayName(resourceKind)} subresource '{unsupportedQueryKey}' is not implemented.",
                StatusCodes.Status501NotImplemented,
                out errorCode,
                out errorMessage,
                out statusCode);
        }

        if (queryKeys.Count > 1) {
            return SetValidationError(
                "NotImplemented",
                $"The requested {resourceKind} subresource combination is not implemented.",
                StatusCodes.Status501NotImplemented,
                out errorCode,
                out errorMessage,
                out statusCode);
        }

        if (queryKeys.Count == 1) {
            return SetValidationError(
                "NotImplemented",
                $"{GetSubresourceDisplayName(resourceKind)} subresource '{queryKeys.OrderBy(static queryKey => queryKey, StringComparer.OrdinalIgnoreCase).First()}' is not implemented.",
                StatusCodes.Status501NotImplemented,
                out errorCode,
                out errorMessage,
                out statusCode);
        }

        return SetValidationSuccess(out errorCode, out errorMessage, out statusCode);
    }

    private static string GetSubresourceDisplayName(string resourceKind)
    {
        return resourceKind switch
        {
            "bucket" => "Bucket",
            "object" => "Object",
            _ when string.IsNullOrEmpty(resourceKind) => string.Empty,
            _ => char.ToUpperInvariant(resourceKind[0]) + resourceKind[1..]
        };
    }

    private static bool TryGetMultipartUploadId(HttpRequest request, out string? uploadId, out string? error)
    {
        if (!request.Query.TryGetValue(UploadIdQueryParameterName, out var values)) {
            uploadId = null;
            error = $"The '{UploadIdQueryParameterName}' query parameter is required.";
            return false;
        }

        uploadId = values.ToString();
        if (string.IsNullOrWhiteSpace(uploadId)) {
            error = $"The '{UploadIdQueryParameterName}' query parameter must not be empty.";
            return false;
        }

        error = null;
        return true;
    }

    private static bool TryGetPartNumber(HttpRequest request, out int? partNumber, out string? error)
    {
        if (!request.Query.TryGetValue(PartNumberQueryParameterName, out var values)) {
            partNumber = null;
            error = $"The '{PartNumberQueryParameterName}' query parameter is required.";
            return false;
        }

        if (!int.TryParse(values.ToString(), out var parsedPartNumber) || parsedPartNumber <= 0) {
            partNumber = null;
            error = $"The '{PartNumberQueryParameterName}' query parameter must be a positive integer.";
            return false;
        }

        partNumber = parsedPartNumber;
        error = null;
        return true;
    }

    private static string QuoteETag(string value)
    {
        return string.IsNullOrWhiteSpace(value)
            ? "\"\""
            : value.StartsWith('"') ? value : $"\"{value}\"";
    }

    private static ObjectRange? ParseRangeHeader(string? rangeHeader)
    {
        if (string.IsNullOrWhiteSpace(rangeHeader)) {
            return null;
        }

        var trimmed = rangeHeader.Trim();
        if (!trimmed.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase)) {
            throw new FormatException("Only byte range requests are supported.");
        }

        var value = trimmed[6..].Trim();
        if (value.Contains(',')) {
            throw new FormatException("Multiple byte ranges are not supported in single-range mode.");
        }

        return ParseSingleRangeSpec(value);
    }

    private static ObjectRange[] ParseMultipleRangeHeaders(string? rangeHeader)
    {
        if (string.IsNullOrWhiteSpace(rangeHeader)) {
            return [];
        }

        var trimmed = rangeHeader.Trim();
        if (!trimmed.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase)) {
            throw new FormatException("Only byte range requests are supported.");
        }

        var value = trimmed[6..].Trim();
        var parts = value.Split(',');

        var ranges = new ObjectRange[parts.Length];
        for (var i = 0; i < parts.Length; i++) {
            ranges[i] = ParseSingleRangeSpec(parts[i].Trim());
        }

        return ranges;
    }

    private static ObjectRange ParseSingleRangeSpec(string value)
    {
        var separatorIndex = value.IndexOf('-');
        if (separatorIndex < 0) {
            throw new FormatException("The Range header is malformed.");
        }

        var startText = value[..separatorIndex].Trim();
        var endText = value[(separatorIndex + 1)..].Trim();
        if (string.IsNullOrEmpty(startText) && string.IsNullOrEmpty(endText)) {
            throw new FormatException("The Range header must specify a start, an end, or both.");
        }

        long? start = null;
        long? end = null;

        if (!string.IsNullOrEmpty(startText)) {
            if (!long.TryParse(startText, out var parsedStart) || parsedStart < 0) {
                throw new FormatException("The Range header contains an invalid start offset.");
            }

            start = parsedStart;
        }

        if (!string.IsNullOrEmpty(endText)) {
            if (!long.TryParse(endText, out var parsedEnd) || parsedEnd < 0) {
                throw new FormatException("The Range header contains an invalid end offset.");
            }

            end = parsedEnd;
        }

        return new ObjectRange
        {
            Start = start,
            End = end
        };
    }

    private static bool TryGetCopySource(HttpRequest request, out CopySourceReference? copySource, out string? error)
    {
        var rawValue = request.Headers[CopySourceHeaderName].ToString();
        if (string.IsNullOrWhiteSpace(rawValue)) {
            copySource = null;
            error = null;
            return false;
        }

        try {
            copySource = ParseCopySource(rawValue);
            error = null;
            return true;
        }
        catch (FormatException exception) {
            copySource = null;
            error = exception.Message;
            return true;
        }
    }

    private static bool TryGetCopySourceRange(HttpRequest request, out ObjectRange? sourceRange, out string? error)
    {
        var rawValue = request.Headers[CopySourceRangeHeaderName].ToString();
        if (!request.Headers.ContainsKey(CopySourceRangeHeaderName)) {
            sourceRange = null;
            error = null;
            return true;
        }

        if (string.IsNullOrWhiteSpace(rawValue)) {
            sourceRange = null;
            error = $"The '{CopySourceRangeHeaderName}' header must not be empty.";
            return false;
        }

        try {
            sourceRange = ParseRangeHeader(rawValue);
            if (sourceRange is null || sourceRange.Start is null || sourceRange.End is null) {
                throw new FormatException($"The '{CopySourceRangeHeaderName}' header must specify both start and end byte offsets.");
            }

            if (sourceRange.End.Value < sourceRange.Start.Value) {
                throw new FormatException($"The '{CopySourceRangeHeaderName}' header must specify an end offset greater than or equal to the start offset.");
            }

            error = null;
            return true;
        }
        catch (FormatException exception) {
            sourceRange = null;
            error = exception.Message;
            return false;
        }
    }

    private static bool TryParseTaggingDirective(
        HttpRequest request,
        string resource,
        string bucketName,
        string key,
        out ObjectTaggingDirective taggingDirective,
        out IResult? errorResult)
    {
        var rawValue = request.Headers[TaggingDirectiveHeaderName].ToString();
        if (string.IsNullOrWhiteSpace(rawValue) || string.Equals(rawValue, "COPY", StringComparison.OrdinalIgnoreCase)) {
            taggingDirective = ObjectTaggingDirective.Copy;
            errorResult = null;
            return true;
        }

        if (string.Equals(rawValue, "REPLACE", StringComparison.OrdinalIgnoreCase)) {
            taggingDirective = ObjectTaggingDirective.Replace;
            errorResult = null;
            return true;
        }

        taggingDirective = default;
        errorResult = ToErrorResult(
            request.HttpContext,
            StatusCodes.Status400BadRequest,
            "InvalidArgument",
            $"The '{TaggingDirectiveHeaderName}' header must be 'COPY' or 'REPLACE'.",
            resource,
            bucketName,
            key);
        return false;
    }

    private static bool TryParseTaggingHeader(
        HttpRequest request,
        string resource,
        string bucketName,
        string key,
        out IReadOnlyDictionary<string, string>? tags,
        out IResult? errorResult)
    {
        var rawValue = request.Headers[TaggingHeaderName].ToString();
        if (string.IsNullOrWhiteSpace(rawValue)) {
            tags = null;
            errorResult = null;
            return true;
        }

        try {
            tags = ParseTaggingHeader(rawValue);
            var tagValidationError = ObjectTagValidation.Validate(tags);
            if (tagValidationError is not null) {
                tags = null;
                errorResult = ToErrorResult(
                    request.HttpContext,
                    StatusCodes.Status400BadRequest,
                    "InvalidTag",
                    tagValidationError,
                    resource,
                    bucketName,
                    key);
                return false;
            }

            errorResult = null;
            return true;
        }
        catch (ArgumentException exception) {
            tags = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                exception.Message,
                resource,
                bucketName,
                key);
            return false;
        }
    }

    private static async Task<PreparedRequestBody> PrepareRequestBodyAsync(HttpRequest request, CancellationToken cancellationToken)
    {
        if (!IsAwsChunkedContent(request)) {
            return new PreparedRequestBody(request.Body, request.ContentLength, tempFilePath: null, trailerHeaders: null, trailerHeaderEntries: null, finalChunkSignature: null);
        }

        var tempFilePath = Path.Combine(Path.GetTempPath(), $"integrateds3-aws-chunked-{Guid.NewGuid():N}.tmp");
        var trailerHeaders = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var trailerHeaderEntries = new List<KeyValuePair<string, string>>();
        string? finalChunkSignature = null;
        try {
            await using (var tempWriteStream = new FileStream(tempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                finalChunkSignature = await CopyAwsChunkedContentToAsync(request.Body, tempWriteStream, trailerHeaders, trailerHeaderEntries, cancellationToken);
                await tempWriteStream.FlushAsync(cancellationToken);
            }

            var decodedLength = TryParseDecodedContentLength(request.Headers["x-amz-decoded-content-length"].ToString());
            var tempReadStream = new FileStream(tempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
            var contentLength = decodedLength ?? tempReadStream.Length;
            return new PreparedRequestBody(tempReadStream, contentLength, tempFilePath, trailerHeaders, trailerHeaderEntries, finalChunkSignature);
        }
        catch {
            if (File.Exists(tempFilePath)) {
                File.Delete(tempFilePath);
            }

            throw;
        }
    }

    private static bool IsAwsChunkedContent(HttpRequest request)
    {
        if (!request.Headers.TryGetValue(HeaderNames.ContentEncoding, out var contentEncodingValues)) {
            return false;
        }

        return contentEncodingValues
            .Where(static value => value is not null)
            .SelectMany(static value => value!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .Any(static value => string.Equals(value, "aws-chunked", StringComparison.OrdinalIgnoreCase));
    }

    private static long? TryParseDecodedContentLength(string? rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue)) {
            return null;
        }

        return long.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedValue)
            ? parsedValue
            : null;
    }

    private static async Task<string?> CopyAwsChunkedContentToAsync(
        Stream source,
        Stream destination,
        Dictionary<string, string> trailerHeaders,
        List<KeyValuePair<string, string>> trailerHeaderEntries,
        CancellationToken cancellationToken)
    {
        while (true) {
            var chunkHeader = await ReadLineAsync(source, cancellationToken)
                ?? throw new FormatException("The aws-chunked request body ended unexpectedly.");
            var separatorIndex = chunkHeader.IndexOf(';');
            var chunkLengthText = (separatorIndex >= 0 ? chunkHeader[..separatorIndex] : chunkHeader).Trim();
            if (!long.TryParse(chunkLengthText, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var chunkLength) || chunkLength < 0) {
                throw new FormatException("The aws-chunked request body contains an invalid chunk length.");
            }

            if (chunkLength == 0) {
                await ConsumeChunkTrailersAsync(source, trailerHeaders, trailerHeaderEntries, cancellationToken);
                return TryGetAwsChunkSignature(chunkHeader);
            }

            await CopyExactBytesAsync(source, destination, chunkLength, cancellationToken);
            await ExpectCrLfAsync(source, cancellationToken);
        }
    }

    private static async Task<string?> ReadLineAsync(Stream source, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        while (true) {
            var nextByte = await ReadSingleByteAsync(source, cancellationToken);
            if (nextByte < 0) {
                return buffer.Length == 0 ? null : throw new FormatException("The aws-chunked request body contains an incomplete line.");
            }

            if (nextByte == '\r') {
                var lineFeed = await ReadSingleByteAsync(source, cancellationToken);
                if (lineFeed != '\n') {
                    throw new FormatException("The aws-chunked request body contains an invalid line terminator.");
                }

                return Encoding.ASCII.GetString(buffer.ToArray());
            }

            buffer.WriteByte((byte)nextByte);
        }
    }

    private static async Task ConsumeChunkTrailersAsync(
        Stream source,
        Dictionary<string, string> trailerHeaders,
        List<KeyValuePair<string, string>> trailerHeaderEntries,
        CancellationToken cancellationToken)
    {
        while (true) {
            var trailerLine = await ReadLineAsync(source, cancellationToken)
                ?? throw new FormatException("The aws-chunked request body ended before the terminating trailer section.");
            if (trailerLine.Length == 0) {
                return;
            }

            var separatorIndex = trailerLine.IndexOf(':');
            if (separatorIndex <= 0) {
                throw new FormatException("The aws-chunked request body contains an invalid trailer header.");
            }

            var trailerName = trailerLine[..separatorIndex].Trim();
            var trailerValue = trailerLine[(separatorIndex + 1)..].Trim();
            if (trailerName.Length == 0) {
                throw new FormatException("The aws-chunked request body contains an invalid trailer header.");
            }

            trailerHeaderEntries.Add(new KeyValuePair<string, string>(trailerName, trailerValue));
            trailerHeaders[trailerName] = trailerValue;
        }
    }

    private static string? TryGetAwsChunkSignature(string chunkHeader)
    {
        var separatorIndex = chunkHeader.IndexOf(';');
        if (separatorIndex < 0 || separatorIndex == chunkHeader.Length - 1) {
            return null;
        }

        foreach (var extension in chunkHeader[(separatorIndex + 1)..].Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            var equalsIndex = extension.IndexOf('=');
            if (equalsIndex <= 0 || equalsIndex == extension.Length - 1) {
                continue;
            }

            if (!string.Equals(extension[..equalsIndex].Trim(), "chunk-signature", StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            var signature = extension[(equalsIndex + 1)..].Trim();
            return signature.Length == 0 ? null : signature;
        }

        return null;
    }

    private static async Task CopyExactBytesAsync(Stream source, Stream destination, long byteCount, CancellationToken cancellationToken)
    {
        var remaining = byteCount;
        var buffer = new byte[81920];

        while (remaining > 0) {
            var read = await source.ReadAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, remaining)), cancellationToken);
            if (read == 0) {
                throw new FormatException("The aws-chunked request body ended unexpectedly while reading a chunk.");
            }

            await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
            remaining -= read;
        }
    }

    private static async Task ExpectCrLfAsync(Stream source, CancellationToken cancellationToken)
    {
        if (await ReadSingleByteAsync(source, cancellationToken) != '\r'
            || await ReadSingleByteAsync(source, cancellationToken) != '\n') {
            throw new FormatException("The aws-chunked request body is missing the expected chunk terminator.");
        }
    }

    private static async Task<int> ReadSingleByteAsync(Stream source, CancellationToken cancellationToken)
    {
        var buffer = new byte[1];
        var read = await source.ReadAsync(buffer.AsMemory(0, 1), cancellationToken);
        return read == 0 ? -1 : buffer[0];
    }

    private static CopySourceReference ParseCopySource(string rawValue)
    {
        var trimmed = rawValue.Trim();
        if (trimmed.StartsWith('/')) {
            trimmed = trimmed[1..];
        }

        trimmed = Uri.UnescapeDataString(trimmed);
        var querySeparatorIndex = trimmed.IndexOf('?');
        var pathValue = querySeparatorIndex >= 0
            ? trimmed[..querySeparatorIndex]
            : trimmed;

        var separatorIndex = pathValue.IndexOf('/');
        if (separatorIndex <= 0 || separatorIndex == pathValue.Length - 1) {
            throw new FormatException("The copy source header must be in the form '/bucket/key'.");
        }

        string? versionId = null;
        if (querySeparatorIndex >= 0 && querySeparatorIndex < trimmed.Length - 1) {
            var query = QueryHelpers.ParseQuery(trimmed[querySeparatorIndex..]);
            if (query.TryGetValue(VersionIdQueryParameterName, out var versionIdValues)) {
                versionId = versionIdValues.ToString();
            }
        }

        return new CopySourceReference(pathValue[..separatorIndex], pathValue[(separatorIndex + 1)..], versionId);
    }

    private static IReadOnlyDictionary<string, string> ParseTaggingHeader(string rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue)) {
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }

        var queryValue = rawValue.StartsWith('?')
            ? rawValue[1..]
            : rawValue;
        var tags = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var segment in queryValue.Split('&', StringSplitOptions.None)) {
            if (segment.Length == 0) {
                continue;
            }

            var separatorIndex = segment.IndexOf('=');
            var tagKey = DecodeTaggingHeaderComponent(separatorIndex >= 0 ? segment[..separatorIndex] : segment);
            if (string.IsNullOrEmpty(tagKey)) {
                throw new ArgumentException($"The '{TaggingHeaderName}' header must contain non-empty tag keys.");
            }

            var tagValue = DecodeTaggingHeaderComponent(separatorIndex >= 0 ? segment[(separatorIndex + 1)..] : string.Empty);
            if (!tags.TryAdd(tagKey, tagValue)) {
                throw new ArgumentException($"The '{TaggingHeaderName}' header cannot contain duplicate tag key '{tagKey}'.");
            }
        }

        return tags;
    }

    private static string DecodeTaggingHeaderComponent(string rawValue)
    {
        if (rawValue.IndexOf('%') < 0) {
            return rawValue;
        }

        var builder = new StringBuilder(rawValue.Length);
        var pendingBytes = new byte[rawValue.Length];
        var pendingByteCount = 0;

        for (var index = 0; index < rawValue.Length; index++) {
            if (rawValue[index] == '%') {
                pendingBytes[pendingByteCount++] = ParsePercentEncodedTaggingHeaderByte(rawValue, index);
                index += 2;
                continue;
            }

            FlushDecodedTaggingHeaderBytes(builder, pendingBytes, ref pendingByteCount);
            builder.Append(rawValue[index]);
        }

        FlushDecodedTaggingHeaderBytes(builder, pendingBytes, ref pendingByteCount);
        return builder.ToString();
    }

    private static byte ParsePercentEncodedTaggingHeaderByte(string rawValue, int percentIndex)
    {
        if (percentIndex + 2 >= rawValue.Length
            || !TryParseHexDigit(rawValue[percentIndex + 1], out var highNibble)
            || !TryParseHexDigit(rawValue[percentIndex + 2], out var lowNibble)) {
            throw new ArgumentException($"The '{TaggingHeaderName}' header contains invalid percent-encoding.");
        }

        return (byte)((highNibble << 4) | lowNibble);
    }

    private static void FlushDecodedTaggingHeaderBytes(StringBuilder builder, byte[] pendingBytes, ref int pendingByteCount)
    {
        if (pendingByteCount == 0) {
            return;
        }

        try {
            builder.Append(StrictTaggingHeaderEncoding.GetString(pendingBytes, 0, pendingByteCount));
        }
        catch (DecoderFallbackException) {
            throw new ArgumentException($"The '{TaggingHeaderName}' header contains invalid UTF-8.");
        }

        pendingByteCount = 0;
    }

    private static bool TryParseHexDigit(char value, out int digit)
    {
        digit = value switch
        {
            >= '0' and <= '9' => value - '0',
            >= 'A' and <= 'F' => value - 'A' + 10,
            >= 'a' and <= 'f' => value - 'a' + 10,
            _ => -1
        };

        return digit >= 0;
    }

    private static string? ParseVersionId(HttpRequest request)
    {
        if (!request.Query.TryGetValue(VersionIdQueryParameterName, out var values)) {
            return null;
        }

        var versionId = values.ToString();
        return string.IsNullOrWhiteSpace(versionId)
            ? null
            : versionId;
    }

    private static IReadOnlyList<string> ParseAccessControlRequestHeaders(string? rawHeaderValue)
    {
        if (string.IsNullOrWhiteSpace(rawHeaderValue)) {
            return [];
        }

        var headers = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var value in rawHeaderValue.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            if (seen.Add(value)) {
                headers.Add(value);
            }
        }

        return headers;
    }

    private static bool TryParseObjectServerSideEncryptionSettings(
        HttpRequest request,
        bool allowManagedRequestHeaders,
        string? resource,
        string? bucketName,
        string? key,
        out ObjectServerSideEncryptionSettings? serverSideEncryption,
        out IResult? errorResult)
    {
        var unsupportedHeaderName = FindUnsupportedServerSideEncryptionHeader(request);
        if (!string.IsNullOrWhiteSpace(unsupportedHeaderName)) {
            serverSideEncryption = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"Server-side encryption request header '{unsupportedHeaderName}' is not implemented.",
                resource,
                bucketName,
                key);
            return false;
        }

        var hasAlgorithmHeader = request.Headers.ContainsKey(ServerSideEncryptionHeaderName);
        var hasKmsKeyIdHeader = request.Headers.ContainsKey(ServerSideEncryptionAwsKmsKeyIdHeaderName);
        var hasContextHeader = request.Headers.ContainsKey(ServerSideEncryptionContextHeaderName);

        if (!allowManagedRequestHeaders) {
            var disallowedHeaderName = hasAlgorithmHeader
                ? ServerSideEncryptionHeaderName
                : hasKmsKeyIdHeader
                    ? ServerSideEncryptionAwsKmsKeyIdHeaderName
                    : hasContextHeader
                        ? ServerSideEncryptionContextHeaderName
                        : null;
            if (disallowedHeaderName is not null) {
                serverSideEncryption = null;
                errorResult = ToErrorResult(
                    request.HttpContext,
                    StatusCodes.Status400BadRequest,
                    "InvalidRequest",
                    $"The '{disallowedHeaderName}' header must not be sent for {request.Method} object requests when using S3-managed or KMS-managed server-side encryption.",
                    resource,
                    bucketName,
                    key);
                return false;
            }

            serverSideEncryption = null;
            errorResult = null;
            return true;
        }

        var rawAlgorithm = request.Headers[ServerSideEncryptionHeaderName].ToString();
        if (hasAlgorithmHeader && string.IsNullOrWhiteSpace(rawAlgorithm)) {
            serverSideEncryption = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                $"The '{ServerSideEncryptionHeaderName}' header must not be empty.",
                resource,
                bucketName,
                key);
            return false;
        }

        var rawKmsKeyId = request.Headers[ServerSideEncryptionAwsKmsKeyIdHeaderName].ToString();
        if (hasKmsKeyIdHeader && string.IsNullOrWhiteSpace(rawKmsKeyId)) {
            serverSideEncryption = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                $"The '{ServerSideEncryptionAwsKmsKeyIdHeaderName}' header must not be empty.",
                resource,
                bucketName,
                key);
            return false;
        }

        var rawContext = request.Headers[ServerSideEncryptionContextHeaderName].ToString();
        if (hasContextHeader && string.IsNullOrWhiteSpace(rawContext)) {
            serverSideEncryption = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                $"The '{ServerSideEncryptionContextHeaderName}' header must not be empty.",
                resource,
                bucketName,
                key);
            return false;
        }

        if (!hasAlgorithmHeader) {
            if (hasKmsKeyIdHeader || hasContextHeader) {
                var dependentHeaders = GetPresentServerSideEncryptionHeaderNames(hasKmsKeyIdHeader, hasContextHeader);
                serverSideEncryption = null;
                errorResult = ToErrorResult(
                    request.HttpContext,
                    StatusCodes.Status400BadRequest,
                    "InvalidRequest",
                    $"The '{ServerSideEncryptionHeaderName}' header must be 'aws:kms' or 'aws:kms:dsse' when {FormatQuotedHeaderNames(dependentHeaders)} {(dependentHeaders.Count == 1 ? "is" : "are")} supplied.",
                    resource,
                    bucketName,
                    key);
                return false;
            }

            serverSideEncryption = null;
            errorResult = null;
            return true;
        }

        rawAlgorithm = rawAlgorithm.Trim();
        if (!TryNormalizeServerSideEncryptionAlgorithm(rawAlgorithm, out var algorithm)) {
            serverSideEncryption = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"Server-side encryption algorithm '{rawAlgorithm}' is not implemented.",
                resource,
                bucketName,
                key);
            return false;
        }

        if (algorithm == ObjectServerSideEncryptionAlgorithm.Aes256) {
            if (hasKmsKeyIdHeader || hasContextHeader) {
                var invalidHeaders = GetPresentServerSideEncryptionHeaderNames(hasKmsKeyIdHeader, hasContextHeader);
                serverSideEncryption = null;
                errorResult = ToErrorResult(
                    request.HttpContext,
                    StatusCodes.Status400BadRequest,
                    "InvalidRequest",
                    $"{FormatQuotedHeaderNames(invalidHeaders)} {(invalidHeaders.Count == 1 ? "is" : "are")} only supported when '{ServerSideEncryptionHeaderName}=aws:kms' or '{ServerSideEncryptionHeaderName}=aws:kms:dsse' is supplied.",
                    resource,
                    bucketName,
                    key);
                return false;
            }

            serverSideEncryption = new ObjectServerSideEncryptionSettings
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256
            };
            errorResult = null;
            return true;
        }

        if (!TryParseServerSideEncryptionContext(rawContext, out var context, out var contextErrorMessage)) {
            serverSideEncryption = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                contextErrorMessage!,
                resource,
                bucketName,
                key);
            return false;
        }

        serverSideEncryption = new ObjectServerSideEncryptionSettings
        {
            Algorithm = algorithm,
            KeyId = hasKmsKeyIdHeader ? rawKmsKeyId.Trim() : null,
            Context = context
        };
        errorResult = null;
        return true;
    }

    private static string? FindUnsupportedServerSideEncryptionHeader(HttpRequest request)
    {
        foreach (var headerName in request.Headers.Keys) {
            if (headerName.StartsWith(CopySourceServerSideEncryptionHeaderPrefix, StringComparison.OrdinalIgnoreCase)
                && !SupportedCopySourceCustomerServerSideEncryptionRequestHeaders.Contains(headerName)) {
                return headerName;
            }

            if (headerName.StartsWith(ServerSideEncryptionHeaderPrefix, StringComparison.OrdinalIgnoreCase)
                && !SupportedManagedServerSideEncryptionRequestHeaders.Contains(headerName)
                && !SupportedCustomerServerSideEncryptionRequestHeaders.Contains(headerName)) {
                return headerName;
            }
        }

        return null;
    }

    private static (ObjectCustomerEncryptionSettings? Settings, IResult? Error) TryParseCustomerEncryptionSettings(
        HttpRequest request, string? resource = null, string? bucketName = null, string? key = null)
    {
        var algorithm = request.Headers[ServerSideEncryptionCustomerAlgorithmHeaderName].FirstOrDefault();
        if (string.IsNullOrEmpty(algorithm))
            return (null, null);

        if (!string.Equals(algorithm, "AES256", StringComparison.OrdinalIgnoreCase)) {
            return (null, ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                $"The SSE customer algorithm '{algorithm}' is not supported. Only AES256 is supported.",
                resource, bucketName, key));
        }

        var keyValue = request.Headers[ServerSideEncryptionCustomerKeyHeaderName].FirstOrDefault();
        var keyMd5 = request.Headers[ServerSideEncryptionCustomerKeyMd5HeaderName].FirstOrDefault();

        if (string.IsNullOrEmpty(keyValue) || string.IsNullOrEmpty(keyMd5)) {
            return (null, ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                "SSE-C requires x-amz-server-side-encryption-customer-algorithm, x-amz-server-side-encryption-customer-key, and x-amz-server-side-encryption-customer-key-MD5 headers.",
                resource, bucketName, key));
        }

        return (new ObjectCustomerEncryptionSettings
        {
            Algorithm = algorithm,
            Key = keyValue,
            KeyMd5 = keyMd5
        }, null);
    }

    private static (ObjectCustomerEncryptionSettings? Settings, IResult? Error) TryParseCopySourceCustomerEncryptionSettings(
        HttpRequest request, string? resource = null, string? bucketName = null, string? key = null)
    {
        var algorithm = request.Headers[CopySourceServerSideEncryptionCustomerAlgorithmHeaderName].FirstOrDefault();
        if (string.IsNullOrEmpty(algorithm))
            return (null, null);

        if (!string.Equals(algorithm, "AES256", StringComparison.OrdinalIgnoreCase)) {
            return (null, ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                $"The SSE-C copy source algorithm '{algorithm}' is not supported. Only AES256 is supported.",
                resource, bucketName, key));
        }

        var keyValue = request.Headers[CopySourceServerSideEncryptionCustomerKeyHeaderName].FirstOrDefault();
        var keyMd5 = request.Headers[CopySourceServerSideEncryptionCustomerKeyMd5HeaderName].FirstOrDefault();

        if (string.IsNullOrEmpty(keyValue) || string.IsNullOrEmpty(keyMd5)) {
            return (null, ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidArgument",
                "SSE-C copy source requires x-amz-copy-source-server-side-encryption-customer-algorithm, x-amz-copy-source-server-side-encryption-customer-key, and x-amz-copy-source-server-side-encryption-customer-key-MD5 headers.",
                resource, bucketName, key));
        }

        return (new ObjectCustomerEncryptionSettings
        {
            Algorithm = algorithm,
            Key = keyValue,
            KeyMd5 = keyMd5
        }, null);
    }

    private static bool TryNormalizeServerSideEncryptionAlgorithm(string rawValue, out ObjectServerSideEncryptionAlgorithm algorithm)
    {
        if (string.Equals(rawValue, "AES256", StringComparison.OrdinalIgnoreCase)) {
            algorithm = ObjectServerSideEncryptionAlgorithm.Aes256;
            return true;
        }

        if (string.Equals(rawValue, "aws:kms", StringComparison.OrdinalIgnoreCase)) {
            algorithm = ObjectServerSideEncryptionAlgorithm.Kms;
            return true;
        }

        if (string.Equals(rawValue, "aws:kms:dsse", StringComparison.OrdinalIgnoreCase)) {
            algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse;
            return true;
        }

        algorithm = default;
        return false;
    }

    private static bool TryParseServerSideEncryptionContext(string? rawContext, out IReadOnlyDictionary<string, string>? context, out string? errorMessage)
    {
        if (string.IsNullOrWhiteSpace(rawContext)) {
            context = null;
            errorMessage = null;
            return true;
        }

        try {
            using var document = JsonDocument.Parse(Convert.FromBase64String(rawContext.Trim()));
            if (document.RootElement.ValueKind != JsonValueKind.Object) {
                context = null;
                errorMessage = $"The '{ServerSideEncryptionContextHeaderName}' header must decode to a JSON object.";
                return false;
            }

            var parsedContext = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var property in document.RootElement.EnumerateObject()) {
                if (string.IsNullOrWhiteSpace(property.Name)) {
                    context = null;
                    errorMessage = $"The '{ServerSideEncryptionContextHeaderName}' header must contain only non-empty string keys.";
                    return false;
                }

                if (property.Value.ValueKind != JsonValueKind.String) {
                    context = null;
                    errorMessage = $"The '{ServerSideEncryptionContextHeaderName}' header must contain only string values.";
                    return false;
                }

                var value = property.Value.GetString();
                if (string.IsNullOrWhiteSpace(value)) {
                    context = null;
                    errorMessage = $"The '{ServerSideEncryptionContextHeaderName}' header must contain only non-empty string values.";
                    return false;
                }

                parsedContext[property.Name] = value;
            }

            if (parsedContext.Count == 0) {
                context = null;
                errorMessage = $"The '{ServerSideEncryptionContextHeaderName}' header must contain at least one key-value pair.";
                return false;
            }

            context = parsedContext;
            errorMessage = null;
            return true;
        }
        catch (FormatException) {
            context = null;
            errorMessage = $"The '{ServerSideEncryptionContextHeaderName}' header must be valid base64-encoded UTF-8 JSON.";
            return false;
        }
        catch (JsonException) {
            context = null;
            errorMessage = $"The '{ServerSideEncryptionContextHeaderName}' header must be valid base64-encoded UTF-8 JSON.";
            return false;
        }
    }

    private static IReadOnlyList<string> GetPresentServerSideEncryptionHeaderNames(bool hasKmsKeyIdHeader, bool hasContextHeader)
    {
        var headers = new List<string>(capacity: 2);
        if (hasKmsKeyIdHeader) {
            headers.Add(ServerSideEncryptionAwsKmsKeyIdHeaderName);
        }

        if (hasContextHeader) {
            headers.Add(ServerSideEncryptionContextHeaderName);
        }

        return headers;
    }

    private static string FormatQuotedHeaderNames(IReadOnlyList<string> headerNames)
    {
        return headerNames.Count switch
        {
            0 => string.Empty,
            1 => $"'{headerNames[0]}'",
            2 => $"'{headerNames[0]}' and '{headerNames[1]}'",
            _ => string.Join(", ", headerNames.Take(headerNames.Count - 1).Select(static headerName => $"'{headerName}'")) + $", and '{headerNames[^1]}'"
        };
    }

    private static S3CorsRule ToS3CorsRule(BucketCorsRule rule)
    {
        return new S3CorsRule
        {
            Id = rule.Id,
            AllowedOrigins = rule.AllowedOrigins,
            AllowedMethods = rule.AllowedMethods.Select(ToS3CorsMethod).ToArray(),
            AllowedHeaders = rule.AllowedHeaders,
            ExposeHeaders = rule.ExposeHeaders,
            MaxAgeSeconds = rule.MaxAgeSeconds
        };
    }

    private static BucketCorsRule ToBucketCorsRule(S3CorsRule rule)
    {
        if (rule.AllowedOrigins.Count == 0) {
            throw new ArgumentException("Each CORS rule must contain at least one AllowedOrigin value.", nameof(rule));
        }

        if (rule.AllowedMethods.Count == 0) {
            throw new ArgumentException("Each CORS rule must contain at least one AllowedMethod value.", nameof(rule));
        }

        return new BucketCorsRule
        {
            Id = string.IsNullOrWhiteSpace(rule.Id) ? null : rule.Id.Trim(),
            AllowedOrigins = rule.AllowedOrigins.Select(static origin => origin.Trim()).ToArray(),
            AllowedMethods = rule.AllowedMethods.Select(ToBucketCorsMethod).ToArray(),
            AllowedHeaders = rule.AllowedHeaders.Select(static header => header.Trim()).Where(static header => !string.IsNullOrWhiteSpace(header)).ToArray(),
            ExposeHeaders = rule.ExposeHeaders.Select(static header => header.Trim()).Where(static header => !string.IsNullOrWhiteSpace(header)).ToArray(),
            MaxAgeSeconds = rule.MaxAgeSeconds
        };
    }

    private static S3BucketEncryptionRule ToS3BucketEncryptionRule(BucketDefaultEncryptionRule rule)
    {
        if (rule.Algorithm == ObjectServerSideEncryptionAlgorithm.Aes256
            && !string.IsNullOrWhiteSpace(rule.KeyId)) {
            throw new InvalidOperationException("AES256 bucket default encryption does not support key identifiers.");
        }

        var algorithm = ToS3ServerSideEncryptionValue(rule.Algorithm)
            ?? throw new InvalidOperationException($"Unsupported bucket default encryption algorithm '{rule.Algorithm}'.");

        return new S3BucketEncryptionRule
        {
            DefaultEncryption = new S3BucketEncryptionByDefault
            {
                SseAlgorithm = algorithm,
                KmsMasterKeyId = string.IsNullOrWhiteSpace(rule.KeyId) ? null : rule.KeyId
            },
            BucketKeyEnabled = rule.BucketKeyEnabled ? true : null
        };
    }

    private static BucketCorsMethod ToBucketCorsMethod(string method)
    {
        return method switch
        {
            "GET" => BucketCorsMethod.Get,
            "PUT" => BucketCorsMethod.Put,
            "POST" => BucketCorsMethod.Post,
            "DELETE" => BucketCorsMethod.Delete,
            "HEAD" => BucketCorsMethod.Head,
            _ => throw new ArgumentException($"Unsupported CORS method '{method}'.", nameof(method))
        };
    }

    private static string ToS3CorsMethod(BucketCorsMethod method)
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

    private static bool TryParseRequestChecksums(
        HttpRequest request,
        PreparedRequestBody? preparedBody,
        bool requireChecksumValueForDeclaredAlgorithm,
        out string? checksumAlgorithm,
        out IReadOnlyDictionary<string, string>? checksums,
        out IResult? errorResult)
    {
        var trailerHeaders = preparedBody?.TrailerHeaders;

        if (!TryGetRequestChecksumAlgorithm(request, out checksumAlgorithm, out errorResult)) {
            checksums = null;
            return false;
        }

        if (!TryValidateAwsChunkedChecksumTrailers(request, trailerHeaders, preparedBody?.TrailerHeaderEntries, out var declaredTrailerHeaders, out errorResult)) {
            checksums = null;
            return false;
        }

        if (!TryValidateDeclaredChecksumTrailerHeaders(request, trailerHeaders, declaredTrailerHeaders, out errorResult)) {
            checksums = null;
            return false;
        }

        if (!TryValidateAwsChunkedTrailerSignature(request, preparedBody, out errorResult)) {
            checksums = null;
            return false;
        }

        var parsedChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (!TryGetRequestHeaderValue(request, trailerHeaders, ChecksumSha256HeaderName, out var checksumSha256, out errorResult)) {
            checksums = null;
            return false;
        }

        if (!string.IsNullOrWhiteSpace(checksumSha256)) {
            parsedChecksums["sha256"] = checksumSha256.Trim();
        }

        if (!TryGetRequestHeaderValue(request, trailerHeaders, ChecksumSha1HeaderName, out var checksumSha1, out errorResult)) {
            checksums = null;
            return false;
        }

        if (!string.IsNullOrWhiteSpace(checksumSha1)) {
            parsedChecksums["sha1"] = checksumSha1.Trim();
        }

        if (!TryGetRequestHeaderValue(request, trailerHeaders, ChecksumCrc32HeaderName, out var checksumCrc32, out errorResult)) {
            checksums = null;
            return false;
        }

        if (!string.IsNullOrWhiteSpace(checksumCrc32)) {
            parsedChecksums["crc32"] = checksumCrc32.Trim();
        }

        if (!TryGetRequestHeaderValue(request, trailerHeaders, ChecksumCrc32cHeaderName, out var checksumCrc32c, out errorResult)) {
            checksums = null;
            return false;
        }

        if (!string.IsNullOrWhiteSpace(checksumCrc32c)) {
            parsedChecksums["crc32c"] = checksumCrc32c.Trim();
        }

        if (parsedChecksums.Count > 1) {
            checksums = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                "Expecting a single x-amz-checksum-* header. Multiple checksum types are not allowed.",
                resource: null);
            return false;
        }

        if (requireChecksumValueForDeclaredAlgorithm
            && string.Equals(checksumAlgorithm, "sha256", StringComparison.OrdinalIgnoreCase)
            && !parsedChecksums.ContainsKey("sha256")) {
            checksums = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{ChecksumSha256HeaderName}' header is required when either '{SdkChecksumAlgorithmHeaderName}=SHA256' or '{ChecksumAlgorithmHeaderName}=SHA256' is supplied.",
                resource: null);
            return false;
        }

        if (requireChecksumValueForDeclaredAlgorithm
            && string.Equals(checksumAlgorithm, "sha1", StringComparison.OrdinalIgnoreCase)
            && !parsedChecksums.ContainsKey("sha1")) {
            checksums = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{ChecksumSha1HeaderName}' header is required when either '{SdkChecksumAlgorithmHeaderName}=SHA1' or '{ChecksumAlgorithmHeaderName}=SHA1' is supplied.",
                resource: null);
            return false;
        }

        if (requireChecksumValueForDeclaredAlgorithm
            && string.Equals(checksumAlgorithm, "crc32", StringComparison.OrdinalIgnoreCase)
            && !parsedChecksums.ContainsKey("crc32")) {
            checksums = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{ChecksumCrc32HeaderName}' header is required when either '{SdkChecksumAlgorithmHeaderName}=CRC32' or '{ChecksumAlgorithmHeaderName}=CRC32' is supplied.",
                resource: null);
            return false;
        }

        if (requireChecksumValueForDeclaredAlgorithm
            && string.Equals(checksumAlgorithm, "crc32c", StringComparison.OrdinalIgnoreCase)
            && !parsedChecksums.ContainsKey("crc32c")) {
            checksums = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{ChecksumCrc32cHeaderName}' header is required when either '{SdkChecksumAlgorithmHeaderName}=CRC32C' or '{ChecksumAlgorithmHeaderName}=CRC32C' is supplied.",
                resource: null);
            return false;
        }

        // Content-MD5 is the standard HTTP integrity header used by rclone and other S3 clients.
        // Unlike x-amz-checksum-* headers, Content-MD5 can coexist with them, so it is added
        // after the multiple-checksum-type validation.
        var contentMd5 = request.Headers[HeaderNames.ContentMD5].ToString();
        if (!string.IsNullOrWhiteSpace(contentMd5)) {
            parsedChecksums["md5"] = contentMd5.Trim();
        }

        if (parsedChecksums.Count == 0) {
            checksums = null;
            errorResult = null;
            return true;
        }

        checksums = parsedChecksums;
        errorResult = null;
        return true;
    }

    private static bool TryValidateAwsChunkedChecksumTrailers(
        HttpRequest request,
        IReadOnlyDictionary<string, string>? trailerHeaders,
        IReadOnlyList<KeyValuePair<string, string>>? trailerHeaderEntries,
        out HashSet<string>? declaredTrailerHeaders,
        out IResult? errorResult)
    {
        if (!TryParseTrailerHeaderNames(request, out declaredTrailerHeaders, out errorResult)) {
            return false;
        }

        var usesTrailerBackedPayloadHash = IsTrailerBackedStreamingPayloadHash(request.Headers[AwsContentSha256HeaderName].ToString());
        var requiresTrailerSignature = IsSignedTrailerBackedStreamingPayloadHash(request.Headers[AwsContentSha256HeaderName].ToString());
        var actualTrailerHeaderNames = trailerHeaderEntries is null
            ? []
            : trailerHeaderEntries
                .Where(static trailerHeader => !string.Equals(trailerHeader.Key, AwsTrailerSignatureHeaderName, StringComparison.OrdinalIgnoreCase))
                .Select(static trailerHeader => trailerHeader.Key)
                .ToArray();
        var actualChecksumTrailerHeaderNames = actualTrailerHeaderNames
            .Where(IsChecksumHeaderName)
            .ToArray();
        var declaredChecksumTrailerHeaders = declaredTrailerHeaders is null
            ? []
            : declaredTrailerHeaders.Where(IsChecksumHeaderName).ToArray();
        var hasDeclaredTrailers = declaredTrailerHeaders is { Count: > 0 };
        var requiresTrailerValidation = usesTrailerBackedPayloadHash
            || actualChecksumTrailerHeaderNames.Length > 0
            || declaredChecksumTrailerHeaders.Length > 0;

        if (!requiresTrailerValidation) {
            errorResult = null;
            return true;
        }

        if (!IsAwsChunkedContent(request)) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{request.Headers[AwsContentSha256HeaderName].ToString().Trim()}' payload hash requires the '{HeaderNames.ContentEncoding}: aws-chunked' request header.",
                resource: null);
            return false;
        }

        if (!hasDeclaredTrailers) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"Missing required header for this request: {AwsTrailerHeaderName}",
                resource: null);
            return false;
        }

        if (requiresTrailerSignature
            && (trailerHeaders is null
                || !trailerHeaders.ContainsKey(AwsTrailerSignatureHeaderName))) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The aws-chunked request body must include the '{AwsTrailerSignatureHeaderName}' trailer header when a signed streaming payload hash is used.",
                resource: null);
            return false;
        }

        var duplicateChecksumTrailerHeader = actualChecksumTrailerHeaderNames
            .GroupBy(static headerName => headerName, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault(static group => group.Count() > 1);
        if (duplicateChecksumTrailerHeader is not null) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The aws-chunked trailer header '{duplicateChecksumTrailerHeader.Key}' must not be repeated.",
                resource: null);
            return false;
        }

        if (usesTrailerBackedPayloadHash && actualTrailerHeaderNames.Length == 0) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The aws-chunked request body must include the trailer headers declared by '{AwsTrailerHeaderName}'.",
                resource: null);
            return false;
        }

        foreach (var trailerHeaderName in actualTrailerHeaderNames) {
            if (!usesTrailerBackedPayloadHash && !IsChecksumHeaderName(trailerHeaderName)) {
                continue;
            }

            if (declaredTrailerHeaders!.Contains(trailerHeaderName)) {
                continue;
            }

            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The aws-chunked trailer header '{trailerHeaderName}' must be declared in the '{AwsTrailerHeaderName}' header.",
                resource: null);
            return false;
        }

        errorResult = null;
        return true;
    }

    private static bool TryValidateAwsChunkedTrailerSignature(
        HttpRequest request,
        PreparedRequestBody? preparedBody,
        out IResult? errorResult)
    {
        if (!IsSignedTrailerBackedStreamingPayloadHash(request.Headers[AwsContentSha256HeaderName].ToString())) {
            errorResult = null;
            return true;
        }

        if (preparedBody?.TrailerHeaders is null
            || !preparedBody.TrailerHeaders.TryGetValue(AwsTrailerSignatureHeaderName, out var trailerSignature)
            || string.IsNullOrWhiteSpace(trailerSignature)) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The aws-chunked request body must include the '{AwsTrailerSignatureHeaderName}' trailer header when a signed streaming payload hash is used.",
                resource: null);
            return false;
        }

        if (!AwsChunkedTrailerSigningContextStore.TryGet(request.HttpContext, out var signingContext)) {
            errorResult = null;
            return true;
        }

        var finalChunkSignature = preparedBody.FinalChunkSignature;
        if (string.IsNullOrWhiteSpace(finalChunkSignature)) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The aws-chunked request body must include the zero-length chunk signature required to validate '{AwsTrailerSignatureHeaderName}'.",
                resource: null);
            return false;
        }

        var trailerPayload = S3SigV4Signer.BuildCanonicalStreamingTrailerHeaders(
            preparedBody.TrailerHeaderEntries ?? Array.Empty<KeyValuePair<string, string>>());

        if (signingContext.IsSigV4a) {
            var sigV4aCredentialScope = S3SigV4aSigner.BuildCredentialScopeString(
                signingContext.CredentialScope.DateStamp,
                signingContext.CredentialScope.Service);
            var sigV4aStringToSign = S3SigV4aSigner.BuildStreamingTrailerStringToSign(
                signingContext.SignedAtUtc,
                sigV4aCredentialScope,
                finalChunkSignature,
                S3SigV4Signer.ComputeSha256Hex(trailerPayload));
            using var ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(signingContext.SecretAccessKey, signingContext.AccessKeyId!);
            if (S3SigV4aSigner.VerifySignature(ecdsaKey, sigV4aStringToSign, trailerSignature.Trim())) {
                errorResult = null;
                return true;
            }
        }
        else {
            var stringToSign = S3SigV4Signer.BuildStreamingTrailerStringToSign(
                signingContext.SignedAtUtc,
                signingContext.CredentialScope,
                finalChunkSignature,
                S3SigV4Signer.ComputeSha256Hex(trailerPayload));
            var expectedSignature = S3SigV4Signer.ComputeSignature(signingContext.SecretAccessKey, signingContext.CredentialScope, stringToSign);
            if (FixedTimeEqualsOrdinalIgnoreCase(expectedSignature, trailerSignature.Trim())) {
                errorResult = null;
                return true;
            }
        }

        errorResult = ToErrorResult(
            request.HttpContext,
            StatusCodes.Status403Forbidden,
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature you provided.",
            resource: null);
        return false;
    }

    private static bool TryValidateDeclaredChecksumTrailerHeaders(
        HttpRequest request,
        IReadOnlyDictionary<string, string>? trailerHeaders,
        HashSet<string>? declaredTrailerHeaders,
        out IResult? errorResult)
    {
        if (declaredTrailerHeaders is null || declaredTrailerHeaders.Count == 0) {
            errorResult = null;
            return true;
        }

        foreach (var declaredTrailerHeader in declaredTrailerHeaders) {
            if (!IsChecksumHeaderName(declaredTrailerHeader)) {
                continue;
            }

            if (trailerHeaders is not null
                && trailerHeaders.TryGetValue(declaredTrailerHeader, out var trailerHeaderValue)
                && !string.IsNullOrWhiteSpace(trailerHeaderValue)) {
                continue;
            }

            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{AwsTrailerHeaderName}' header declared '{declaredTrailerHeader}', but the aws-chunked trailer did not include that checksum header.",
                resource: null);
            return false;
        }

        errorResult = null;
        return true;
    }

    private static bool TryParseTrailerHeaderNames(HttpRequest request, out HashSet<string>? trailerHeaders, out IResult? errorResult)
    {
        var rawValue = request.Headers[AwsTrailerHeaderName].ToString();
        if (string.IsNullOrWhiteSpace(rawValue)) {
            trailerHeaders = null;
            errorResult = null;
            return true;
        }

        trailerHeaders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var headerSegment in rawValue.Split(',')) {
            var headerName = headerSegment.Trim();
            if (headerName.Length == 0) {
                errorResult = ToErrorResult(
                    request.HttpContext,
                    StatusCodes.Status400BadRequest,
                    "InvalidRequest",
                    $"The '{AwsTrailerHeaderName}' header must contain a comma-separated list of trailer header names.",
                    resource: null);
                trailerHeaders = null;
                return false;
            }

            trailerHeaders.Add(headerName);
        }

        errorResult = null;
        return true;
    }

    private static bool TryGetRequestHeaderValue(
        HttpRequest request,
        IReadOnlyDictionary<string, string>? trailerHeaders,
        string headerName,
        out string value,
        out IResult? errorResult)
    {
        var requestHeaderValue = request.Headers[headerName].ToString();
        var hasRequestHeaderValue = !string.IsNullOrWhiteSpace(requestHeaderValue);

        var trailerHeaderValue = string.Empty;
        var hasTrailerHeaderValue = trailerHeaders is not null
            && trailerHeaders.TryGetValue(headerName, out trailerHeaderValue)
            && !string.IsNullOrWhiteSpace(trailerHeaderValue);

        if (hasRequestHeaderValue && hasTrailerHeaderValue) {
            value = string.Empty;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{headerName}' checksum cannot be sent in both the request headers and the aws-chunked trailers.",
                resource: null);
            return false;
        }

        if (hasRequestHeaderValue) {
            value = requestHeaderValue.Trim();
            errorResult = null;
            return true;
        }

        if (hasTrailerHeaderValue) {
            value = trailerHeaderValue!.Trim();
            errorResult = null;
            return true;
        }

        value = string.Empty;
        errorResult = null;
        return true;
    }

    private static bool IsTrailerBackedStreamingPayloadHash(string? payloadHash)
    {
        return string.Equals(payloadHash?.Trim(), StreamingAws4HmacSha256PayloadTrailer, StringComparison.OrdinalIgnoreCase)
            || string.Equals(payloadHash?.Trim(), StreamingAwsEcdsaSha256PayloadTrailer, StringComparison.OrdinalIgnoreCase)
            || string.Equals(payloadHash?.Trim(), StreamingUnsignedPayloadTrailer, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsSignedTrailerBackedStreamingPayloadHash(string? payloadHash)
    {
        return string.Equals(payloadHash?.Trim(), StreamingAws4HmacSha256PayloadTrailer, StringComparison.OrdinalIgnoreCase)
            || string.Equals(payloadHash?.Trim(), StreamingAwsEcdsaSha256PayloadTrailer, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsChecksumHeaderName(string headerName)
    {
        return string.Equals(headerName, ChecksumSha256HeaderName, StringComparison.OrdinalIgnoreCase)
            || string.Equals(headerName, ChecksumSha1HeaderName, StringComparison.OrdinalIgnoreCase)
            || string.Equals(headerName, ChecksumCrc32HeaderName, StringComparison.OrdinalIgnoreCase)
            || string.Equals(headerName, ChecksumCrc32cHeaderName, StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryGetRequestChecksumAlgorithm(HttpRequest request, out string? checksumAlgorithm, out IResult? errorResult)
    {
        var sdkChecksumAlgorithm = request.Headers[SdkChecksumAlgorithmHeaderName].ToString();
        var checksumAlgorithmHeader = request.Headers[ChecksumAlgorithmHeaderName].ToString();
        if (!string.IsNullOrWhiteSpace(sdkChecksumAlgorithm)
            && !string.IsNullOrWhiteSpace(checksumAlgorithmHeader)
            && !string.Equals(sdkChecksumAlgorithm, checksumAlgorithmHeader, StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{SdkChecksumAlgorithmHeaderName}' and '{ChecksumAlgorithmHeaderName}' headers must match when both are supplied.",
                resource: null);
            return false;
        }

        var rawChecksumAlgorithm = !string.IsNullOrWhiteSpace(checksumAlgorithmHeader)
            ? checksumAlgorithmHeader
            : sdkChecksumAlgorithm;

        if (!TryNormalizeChecksumAlgorithm(rawChecksumAlgorithm, out checksumAlgorithm)) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"Checksum algorithm '{rawChecksumAlgorithm}' is not implemented.",
                resource: null);
            return false;
        }

        errorResult = null;
        return true;
    }

    private static bool TryNormalizeChecksumAlgorithm(string? rawValue, out string? checksumAlgorithm)
    {
        if (string.IsNullOrWhiteSpace(rawValue)) {
            checksumAlgorithm = null;
            return true;
        }

        if (string.Equals(rawValue, "SHA256", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = "sha256";
            return true;
        }

        if (string.Equals(rawValue, "SHA1", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = "sha1";
            return true;
        }

        if (string.Equals(rawValue, "CRC32", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = "crc32";
            return true;
        }

        if (string.Equals(rawValue, "CRC32C", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = "crc32c";
            return true;
        }

        checksumAlgorithm = null;
        return false;
    }

    private static DateTimeOffset? ParseOptionalHttpDateHeader(string? rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue)) {
            return null;
        }

        if (!DateTimeOffset.TryParse(rawValue, out var parsedValue)) {
            throw new FormatException($"Invalid HTTP date header value '{rawValue}'.");
        }

        return parsedValue;
    }

    private static CopyObjectMetadataDirective ParseCopyObjectMetadataDirective(string? rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue)
            || string.Equals(rawValue, "COPY", StringComparison.OrdinalIgnoreCase)) {
            return CopyObjectMetadataDirective.Copy;
        }

        if (string.Equals(rawValue, "REPLACE", StringComparison.OrdinalIgnoreCase)) {
            return CopyObjectMetadataDirective.Replace;
        }

        throw new FormatException($"The '{MetadataDirectiveHeaderName}' header must be either 'COPY' or 'REPLACE'.");
    }

    private static IReadOnlyDictionary<string, string>? ParseObjectMetadataHeaders(IHeaderDictionary headers)
    {
        Dictionary<string, string>? metadata = null;

        AppendMetadataHeaders(headers, LegacyMetadataHeaderPrefix, overwriteExisting: false, ref metadata);
        AppendMetadataHeaders(headers, MetadataHeaderPrefix, overwriteExisting: true, ref metadata);

        return metadata is { Count: > 0 }
            ? metadata
            : null;
    }

    private static void AppendMetadataHeaders(
        IHeaderDictionary headers,
        string prefix,
        bool overwriteExisting,
        ref Dictionary<string, string>? metadata)
    {
        foreach (var header in headers.Where(pair => pair.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))) {
            metadata ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var metadataKey = header.Key[prefix.Length..];
            if (overwriteExisting || !metadata.ContainsKey(metadataKey)) {
                metadata[metadataKey] = header.Value.ToString();
            }
        }
    }

    private static string? GetOptionalHeaderValue(string? rawValue)
    {
        return string.IsNullOrWhiteSpace(rawValue)
            ? null
            : rawValue;
    }

    private static void ApplyDeleteObjectHeaders(HttpResponse httpResponse, DeleteObjectResult result)
    {
        ApplyVersionIdHeader(httpResponse, result.VersionId);

        if (result.IsDeleteMarker) {
            httpResponse.Headers[DeleteMarkerHeaderName] = "true";
        }
    }

    private static void ApplyObjectTaggingHeaders(HttpResponse httpResponse, ObjectTagSet tagSet)
    {
        ApplyVersionIdHeader(httpResponse, tagSet.VersionId);
    }

    private static void ApplyStorageErrorHeaders(HttpResponse httpResponse, StorageError error)
    {
        if (error.LastModifiedUtc is { } lastModifiedUtc) {
            httpResponse.Headers.LastModified = lastModifiedUtc.ToString("R");
        }

        ApplyVersionIdHeader(httpResponse, error.VersionId);

        if (error.IsDeleteMarker) {
            httpResponse.Headers[DeleteMarkerHeaderName] = "true";
        }
    }

    private static void ApplyBucketCorsActualHeaders(HttpResponse httpResponse, BucketCorsActualResponse response)
    {
        httpResponse.Headers[AccessControlAllowOriginHeaderName] = response.AllowOrigin;
        if (response.AllowCredentials) {
            httpResponse.Headers[AccessControlAllowCredentialsHeaderName] = "true";
        }

        if (response.ExposeHeaders.Count > 0) {
            httpResponse.Headers[AccessControlExposeHeadersHeaderName] = string.Join(", ", response.ExposeHeaders);
        }

        AppendVaryHeader(httpResponse, OriginHeaderName);
    }

    private static void ApplyBucketCorsPreflightHeaders(HttpResponse httpResponse, BucketCorsPreflightResponse response)
    {
        httpResponse.Headers[AccessControlAllowOriginHeaderName] = response.AllowOrigin;
        if (response.AllowCredentials) {
            httpResponse.Headers[AccessControlAllowCredentialsHeaderName] = "true";
        }

        httpResponse.Headers[AccessControlAllowMethodsHeaderName] = response.AllowMethod;
        if (response.AllowHeaders.Count > 0) {
            httpResponse.Headers[AccessControlAllowHeadersHeaderName] = string.Join(", ", response.AllowHeaders);
        }

        if (response.ExposeHeaders.Count > 0) {
            httpResponse.Headers[AccessControlExposeHeadersHeaderName] = string.Join(", ", response.ExposeHeaders);
        }

        if (response.MaxAgeSeconds is { } maxAgeSeconds) {
            httpResponse.Headers[AccessControlMaxAgeHeaderName] = maxAgeSeconds.ToString(CultureInfo.InvariantCulture);
        }

        AppendVaryHeader(httpResponse, OriginHeaderName, AccessControlRequestMethodHeaderName, AccessControlRequestHeadersHeaderName);
    }

    private static void AppendVaryHeader(HttpResponse httpResponse, params string[] values)
    {
        var combinedValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var existingValue in httpResponse.Headers.Vary.ToString().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            combinedValues.Add(existingValue);
        }

        foreach (var value in values) {
            if (!string.IsNullOrWhiteSpace(value)) {
                combinedValues.Add(value);
            }
        }

        if (combinedValues.Count > 0) {
            httpResponse.Headers.Vary = string.Join(", ", combinedValues);
        }
    }

    private static void ApplyObjectHeaders(HttpResponse httpResponse, ObjectInfo objectInfo)
    {
        ApplyObjectResultHeaders(httpResponse, objectInfo);
        ApplyObjectRepresentationHeaders(httpResponse, objectInfo);

        IEnumerable<KeyValuePair<string, string>> metadataPairs = objectInfo.Metadata ?? Enumerable.Empty<KeyValuePair<string, string>>();
        foreach (var metadataPair in metadataPairs) {
            httpResponse.Headers[$"{MetadataHeaderPrefix}{metadataPair.Key}"] = metadataPair.Value;
            httpResponse.Headers[$"{LegacyMetadataHeaderPrefix}{metadataPair.Key}"] = metadataPair.Value;
        }
    }

    private static void ApplyObjectResultHeaders(HttpResponse httpResponse, ObjectInfo objectInfo)
    {
        httpResponse.Headers.LastModified = objectInfo.LastModifiedUtc.ToString("R");
        ApplyObjectIdentityHeaders(httpResponse, objectInfo);
    }

    private static void ApplyObjectRepresentationHeaders(HttpResponse httpResponse, ObjectInfo objectInfo)
    {
        if (!string.IsNullOrWhiteSpace(objectInfo.CacheControl)) {
            httpResponse.Headers.CacheControl = objectInfo.CacheControl;
        }

        if (!string.IsNullOrWhiteSpace(objectInfo.ContentDisposition)) {
            httpResponse.Headers[HeaderNames.ContentDisposition] = objectInfo.ContentDisposition;
        }

        if (!string.IsNullOrWhiteSpace(objectInfo.ContentEncoding)) {
            httpResponse.Headers[HeaderNames.ContentEncoding] = objectInfo.ContentEncoding;
        }

        if (!string.IsNullOrWhiteSpace(objectInfo.ContentLanguage)) {
            httpResponse.Headers[HeaderNames.ContentLanguage] = objectInfo.ContentLanguage;
        }

        if (objectInfo.ExpiresUtc is { } expiresUtc) {
            httpResponse.Headers.Expires = expiresUtc.ToString("R");
        }
    }

    private static void ApplyObjectTaggingCountHeader(HttpResponse httpResponse, ObjectInfo objectInfo)
    {
        if (objectInfo.Tags is { Count: > 0 } tags) {
            httpResponse.Headers[TaggingCountHeaderName] = tags.Count.ToString(CultureInfo.InvariantCulture);
        }
    }

    private static void ApplyObjectIdentityHeaders(HttpResponse httpResponse, ObjectInfo objectInfo)
    {
        if (!string.IsNullOrWhiteSpace(objectInfo.ETag)) {
            httpResponse.Headers.ETag = QuoteETag(objectInfo.ETag);
        }

        ApplyVersionIdHeader(httpResponse, objectInfo.VersionId);

        ApplyChecksumHeaders(httpResponse, objectInfo.Checksums);
        ApplyObjectLockHeaders(httpResponse, objectInfo);
        ApplyServerSideEncryptionHeaders(httpResponse, objectInfo.ServerSideEncryption);
        EmitCustomerEncryptionResponseHeaders(httpResponse, objectInfo.CustomerEncryption);
    }

    private static void ApplyObjectLockHeaders(HttpResponse httpResponse, ObjectInfo objectInfo)
    {
        if (objectInfo.RetentionMode is { } retentionMode) {
            httpResponse.Headers[ObjectLockModeHeaderName] = ToS3RetentionMode(retentionMode);
        }

        if (objectInfo.RetainUntilDateUtc is { } retainUntilDateUtc) {
            httpResponse.Headers[ObjectLockRetainUntilDateHeaderName] = XmlConvert.ToString(retainUntilDateUtc.UtcDateTime, XmlDateTimeSerializationMode.Utc);
        }

        if (objectInfo.LegalHoldStatus is { } legalHoldStatus) {
            httpResponse.Headers[ObjectLockLegalHoldHeaderName] = ToS3LegalHoldStatus(legalHoldStatus);
        }
    }

    private static void ApplyVersionIdHeader(HttpResponse httpResponse, string? versionId)
    {
        if (!string.IsNullOrWhiteSpace(versionId)) {
            httpResponse.Headers[VersionIdHeaderName] = versionId;
        }
    }

    private static void ApplyChecksumHeaders(HttpResponse httpResponse, IReadOnlyDictionary<string, string>? checksums)
    {
        var checksumCrc32 = GetChecksumValue(checksums, "crc32");
        if (!string.IsNullOrWhiteSpace(checksumCrc32)) {
            httpResponse.Headers[ChecksumCrc32HeaderName] = checksumCrc32;
        }

        var checksumCrc32c = GetChecksumValue(checksums, "crc32c");
        if (!string.IsNullOrWhiteSpace(checksumCrc32c)) {
            httpResponse.Headers[ChecksumCrc32cHeaderName] = checksumCrc32c;
        }

        var checksumCrc64Nvme = GetChecksumValue(checksums, "crc64nvme");
        if (!string.IsNullOrWhiteSpace(checksumCrc64Nvme)) {
            httpResponse.Headers[ChecksumCrc64NvmeHeaderName] = checksumCrc64Nvme;
        }

        var checksumSha1 = GetChecksumValue(checksums, "sha1");
        if (!string.IsNullOrWhiteSpace(checksumSha1)) {
            httpResponse.Headers[ChecksumSha1HeaderName] = checksumSha1;
        }

        var checksumSha256 = GetChecksumValue(checksums, "sha256");
        if (!string.IsNullOrWhiteSpace(checksumSha256)) {
            httpResponse.Headers[ChecksumSha256HeaderName] = checksumSha256;
        }

        var checksumType = GetChecksumType(checksums);
        if (!string.IsNullOrWhiteSpace(checksumType)) {
            httpResponse.Headers[ChecksumTypeHeaderName] = checksumType;
        }
    }

    private static void ApplyChecksumAlgorithmHeader(HttpResponse httpResponse, string? checksumAlgorithm)
    {
        var responseValue = ToS3ChecksumAlgorithmValue(checksumAlgorithm);
        if (!string.IsNullOrWhiteSpace(responseValue)) {
            httpResponse.Headers[ChecksumAlgorithmHeaderName] = responseValue;
        }
    }

    private static bool IsChecksumModeEnabled(HttpRequest request)
    {
        return string.Equals(
            request.Headers[ChecksumModeHeaderName].ToString().Trim(),
            "ENABLED",
            StringComparison.OrdinalIgnoreCase);
    }

    private static void RemoveChecksumValueHeaders(HttpResponse httpResponse)
    {
        httpResponse.Headers.Remove(ChecksumCrc32HeaderName);
        httpResponse.Headers.Remove(ChecksumCrc32cHeaderName);
        httpResponse.Headers.Remove(ChecksumSha1HeaderName);
        httpResponse.Headers.Remove(ChecksumSha256HeaderName);
        httpResponse.Headers.Remove(ChecksumTypeHeaderName);
    }

    private static void ApplyServerSideEncryptionHeaders(HttpResponse httpResponse, ObjectServerSideEncryptionInfo? serverSideEncryption)
    {
        if (serverSideEncryption is null) {
            return;
        }

        var responseAlgorithm = ToS3ServerSideEncryptionValue(serverSideEncryption.Algorithm);
        if (!string.IsNullOrWhiteSpace(responseAlgorithm)) {
            httpResponse.Headers[ServerSideEncryptionHeaderName] = responseAlgorithm;
        }

        if ((serverSideEncryption.Algorithm == ObjectServerSideEncryptionAlgorithm.Kms
            || serverSideEncryption.Algorithm == ObjectServerSideEncryptionAlgorithm.KmsDsse)
            && !string.IsNullOrWhiteSpace(serverSideEncryption.KeyId)) {
            httpResponse.Headers[ServerSideEncryptionAwsKmsKeyIdHeaderName] = serverSideEncryption.KeyId;
        }

        if (serverSideEncryption.BucketKeyEnabled) {
            httpResponse.Headers[ServerSideEncryptionBucketKeyEnabledHeaderName] = "true";
        }
    }

    private static void EmitCustomerEncryptionResponseHeaders(HttpResponse httpResponse, ObjectCustomerEncryptionInfo? customerEncryption)
    {
        if (customerEncryption is null) {
            return;
        }

        httpResponse.Headers[ServerSideEncryptionCustomerAlgorithmHeaderName] = customerEncryption.Algorithm;
        httpResponse.Headers[ServerSideEncryptionCustomerKeyMd5HeaderName] = customerEncryption.KeyMd5;
    }

    private static string? ToS3RetentionMode(ObjectRetentionMode? mode)
    {
        return mode switch
        {
            ObjectRetentionMode.Governance => "GOVERNANCE",
            ObjectRetentionMode.Compliance => "COMPLIANCE",
            null => null,
            _ => throw new InvalidOperationException($"Unsupported object retention mode '{mode}'.")
        };
    }

    private static string? ToS3LegalHoldStatus(ObjectLegalHoldStatus? status)
    {
        return status switch
        {
            ObjectLegalHoldStatus.On => "ON",
            ObjectLegalHoldStatus.Off => "OFF",
            null => null,
            _ => throw new InvalidOperationException($"Unsupported object legal-hold status '{status}'.")
        };
    }

    private static string? GetChecksumValue(IReadOnlyDictionary<string, string>? checksums, string algorithm)
    {
        if (checksums is null || string.IsNullOrWhiteSpace(algorithm)) {
            return null;
        }

        if (checksums.TryGetValue(algorithm, out var directValue) && !string.IsNullOrWhiteSpace(directValue)) {
            return directValue;
        }

        foreach (var checksum in checksums) {
            if (string.Equals(checksum.Key, algorithm, StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrWhiteSpace(checksum.Value)) {
                return checksum.Value;
            }
        }

        return null;
    }

    private static string? GetChecksumType(IReadOnlyDictionary<string, string>? checksums)
    {
        if (checksums is null) {
            return null;
        }

        foreach (var checksum in checksums.Values) {
            if (IsCompositeChecksumValue(checksum)) {
                return "COMPOSITE";
            }
        }

        return null;
    }

    private static string? GetResponseChecksumAlgorithm(IReadOnlyDictionary<string, string>? checksums)
    {
        if (checksums is null) {
            return null;
        }

        string? algorithm = null;
        foreach (var checksum in checksums) {
            if (string.IsNullOrWhiteSpace(checksum.Value)) {
                continue;
            }

            var normalizedAlgorithm = NormalizeResponseChecksumAlgorithm(checksum.Key);
            if (normalizedAlgorithm is null) {
                continue;
            }

            if (algorithm is null) {
                algorithm = normalizedAlgorithm;
                continue;
            }

            if (!string.Equals(algorithm, normalizedAlgorithm, StringComparison.Ordinal)) {
                return null;
            }
        }

        return algorithm;
    }

    private static bool IsCompositeChecksumValue(string? checksum)
    {
        if (string.IsNullOrWhiteSpace(checksum)) {
            return false;
        }

        var separatorIndex = checksum.LastIndexOf('-');
        if (separatorIndex <= 0 || separatorIndex >= checksum.Length - 1) {
            return false;
        }

        return int.TryParse(checksum[(separatorIndex + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out var partCount)
            && partCount > 0;
    }

    private static string? NormalizeResponseChecksumAlgorithm(string? checksumAlgorithm)
    {
        if (string.IsNullOrWhiteSpace(checksumAlgorithm)) {
            return null;
        }

        if (string.Equals(checksumAlgorithm, "sha256", StringComparison.OrdinalIgnoreCase)) {
            return "sha256";
        }

        if (string.Equals(checksumAlgorithm, "sha1", StringComparison.OrdinalIgnoreCase)) {
            return "sha1";
        }

        if (string.Equals(checksumAlgorithm, "crc32", StringComparison.OrdinalIgnoreCase)) {
            return "crc32";
        }

        if (string.Equals(checksumAlgorithm, "crc32c", StringComparison.OrdinalIgnoreCase)) {
            return "crc32c";
        }

        if (string.Equals(checksumAlgorithm, "crc64nvme", StringComparison.OrdinalIgnoreCase)) {
            return "crc64nvme";
        }

        return null;
    }

    private static string? ToS3ChecksumAlgorithmValue(string? checksumAlgorithm)
    {
        return checksumAlgorithm switch
        {
            "sha256" => "SHA256",
            "sha1" => "SHA1",
            "crc32" => "CRC32",
            "crc32c" => "CRC32C",
            "crc64nvme" => "CRC64NVME",
            _ => null
        };
    }

    private static string? ToS3ServerSideEncryptionValue(ObjectServerSideEncryptionAlgorithm algorithm)
    {
        return algorithm switch
        {
            ObjectServerSideEncryptionAlgorithm.Aes256 => "AES256",
            ObjectServerSideEncryptionAlgorithm.Kms => "aws:kms",
            ObjectServerSideEncryptionAlgorithm.KmsDsse => "aws:kms:dsse",
            _ => null
        };
    }

    private static bool MatchesIfMatch(string? rawHeader, string? currentETag)
    {
        if (string.IsNullOrWhiteSpace(rawHeader)) {
            return true;
        }

        if (rawHeader.Trim() == "*") {
            return true;
        }

        return MatchesAnyETag(rawHeader, currentETag);
    }

    private static bool MatchesAnyETag(string? rawHeader, string? currentETag)
    {
        if (string.IsNullOrWhiteSpace(rawHeader) || string.IsNullOrWhiteSpace(currentETag)) {
            return false;
        }

        var normalizedCurrent = NormalizeETag(currentETag);
        foreach (var candidate in rawHeader.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            if (candidate == "*" || NormalizeETag(candidate) == normalizedCurrent) {
                return true;
            }
        }

        return false;
    }

    private static bool IsVersionAfterMarker(ObjectInfo version, string? keyMarker, string? versionIdMarker)
    {
        if (string.IsNullOrWhiteSpace(keyMarker)) {
            return true;
        }

        var keyComparison = StringComparer.Ordinal.Compare(version.Key, keyMarker);
        if (keyComparison > 0) {
            return true;
        }

        if (keyComparison < 0) {
            return false;
        }

        return !string.IsNullOrWhiteSpace(versionIdMarker)
               && StringComparer.Ordinal.Compare(version.VersionId, versionIdMarker) < 0;
    }

    private static bool IsMultipartUploadAfterMarker(MultipartUploadInfo upload, string? keyMarker, string? uploadIdMarker)
    {
        if (string.IsNullOrWhiteSpace(keyMarker)) {
            return true;
        }

        var keyComparison = StringComparer.Ordinal.Compare(upload.Key, keyMarker);
        if (keyComparison > 0) {
            return true;
        }

        if (keyComparison < 0) {
            return false;
        }

        return !string.IsNullOrWhiteSpace(uploadIdMarker)
               && StringComparer.Ordinal.Compare(upload.UploadId, uploadIdMarker) > 0;
    }

    private static string NormalizeETag(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.StartsWith("W/", StringComparison.OrdinalIgnoreCase)) {
            trimmed = trimmed[2..].Trim();
        }

        if (trimmed.Length >= 2 && trimmed.StartsWith('"') && trimmed.EndsWith('"')) {
            trimmed = trimmed[1..^1];
        }

        return trimmed;
    }

    private static bool WasModifiedAfter(DateTimeOffset lastModifiedUtc, DateTimeOffset comparisonUtc)
    {
        return TruncateToWholeSeconds(lastModifiedUtc) > TruncateToWholeSeconds(comparisonUtc);
    }

    private static DateTimeOffset TruncateToWholeSeconds(DateTimeOffset value)
    {
        var utcValue = value.ToUniversalTime();
        return utcValue.AddTicks(-(utcValue.Ticks % TimeSpan.TicksPerSecond));
    }

    private sealed class BucketCorsResult(string bucketName, IResult innerResult) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);

            var origin = httpContext.Request.Headers[OriginHeaderName].ToString();
            if (!string.IsNullOrWhiteSpace(origin)) {
                AppendVaryHeader(httpContext.Response, OriginHeaderName);
            }

            var runtimeService = httpContext.RequestServices.GetRequiredService<BucketCorsRuntimeService>();
            var response = await runtimeService.GetActualResponseAsync(
                bucketName,
                origin,
                httpContext.Request.Method,
                httpContext.RequestAborted);

            if (response is not null) {
                ApplyBucketCorsActualHeaders(httpContext.Response, response);
            }

            await innerResult.ExecuteAsync(httpContext);
        }
    }

    private sealed class StreamObjectResult(GetObjectResponse objectResponse) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);

            await using var response = objectResponse;

            ApplyObjectHeaders(httpContext.Response, response.Object);
            ApplyObjectTaggingCountHeader(httpContext.Response, response.Object);
            httpContext.Response.Headers.AcceptRanges = "bytes";

            if (response.IsNotModified) {
                httpContext.Response.StatusCode = StatusCodes.Status304NotModified;
                return;
            }

            httpContext.Response.ContentType = response.Object.ContentType ?? "application/octet-stream";
            httpContext.Response.ContentLength = response.Object.ContentLength;

            if (response.Range is not null) {
                httpContext.Response.StatusCode = StatusCodes.Status206PartialContent;
                httpContext.Response.Headers.ContentRange = $"bytes {response.Range.Start}-{response.Range.End}/{response.TotalContentLength}";
            }
            else {
                httpContext.Response.StatusCode = StatusCodes.Status200OK;
            }

            await response.Content.CopyToAsync(httpContext.Response.Body, httpContext.RequestAborted);
        }
    }

    private sealed class MultiRangeStreamObjectResult(
        IStorageService storageService,
        string bucketName,
        string key,
        string? versionId,
        string? ifMatchETag,
        string? ifNoneMatchETag,
        DateTimeOffset? ifModifiedSinceUtc,
        DateTimeOffset? ifUnmodifiedSinceUtc,
        ObjectCustomerEncryptionSettings? customerEncryption,
        ObjectRange[] ranges,
        bool checksumModeEnabled) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);

            var boundary = Guid.NewGuid().ToString("N");

            // Fetch the first range to get object metadata and total size.
            var firstResult = await storageService.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId,
                Range = ranges[0],
                IfMatchETag = ifMatchETag,
                IfNoneMatchETag = ifNoneMatchETag,
                IfModifiedSinceUtc = ifModifiedSinceUtc,
                IfUnmodifiedSinceUtc = ifUnmodifiedSinceUtc,
                CustomerEncryption = customerEncryption
            }, httpContext.RequestAborted);

            if (!firstResult.IsSuccess) {
                httpContext.Response.StatusCode = StatusCodes.Status416RangeNotSatisfiable;
                return;
            }

            await using var firstResponse = firstResult.Value!;

            ApplyObjectHeaders(httpContext.Response, firstResponse.Object);
            ApplyObjectTaggingCountHeader(httpContext.Response, firstResponse.Object);
            httpContext.Response.Headers.AcceptRanges = "bytes";

            if (firstResponse.IsNotModified) {
                httpContext.Response.StatusCode = StatusCodes.Status304NotModified;
                return;
            }

            var totalLength = firstResponse.TotalContentLength;
            var contentType = firstResponse.Object.ContentType ?? "application/octet-stream";

            httpContext.Response.StatusCode = StatusCodes.Status206PartialContent;
            httpContext.Response.ContentType = $"multipart/byteranges; boundary={boundary}";
            httpContext.Response.ContentLength = null;

            var body = httpContext.Response.Body;
            var ct = httpContext.RequestAborted;
            var encoding = Encoding.UTF8;

            // Write first range part.
            await WriteRangePartAsync(body, boundary, contentType, firstResponse.Range!, totalLength, firstResponse.Content, encoding, ct);

            // Fetch and write subsequent range parts.
            for (var i = 1; i < ranges.Length; i++) {
                var rangeResult = await storageService.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = versionId,
                    Range = ranges[i],
                    CustomerEncryption = customerEncryption
                }, ct);

                if (!rangeResult.IsSuccess) {
                    break;
                }

                await using var rangeResponse = rangeResult.Value!;
                await WriteRangePartAsync(body, boundary, contentType, rangeResponse.Range!, totalLength, rangeResponse.Content, encoding, ct);
            }

            // Write closing boundary.
            await body.WriteAsync(encoding.GetBytes($"\r\n--{boundary}--\r\n"), ct);
        }

        private static async Task WriteRangePartAsync(
            Stream body, string boundary, string contentType,
            ObjectRange range, long totalLength, Stream content,
            Encoding encoding, CancellationToken cancellationToken)
        {
            var header = $"\r\n--{boundary}\r\nContent-Type: {contentType}\r\nContent-Range: bytes {range.Start}-{range.End}/{totalLength}\r\n\r\n";
            await body.WriteAsync(encoding.GetBytes(header), cancellationToken);
            await content.CopyToAsync(body, cancellationToken);
        }
    }

    private sealed class EndpointStorageAuthorizationException(StorageError error) : Exception(error.Message)
    {
        public StorageError Error { get; } = error;
    }

    private sealed class XmlContentResult(string content, int statusCode, string contentType) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);

            httpContext.Response.StatusCode = statusCode;
            httpContext.Response.ContentType = contentType;
            await httpContext.Response.WriteAsync(content, httpContext.RequestAborted);
        }
    }

    private static bool FixedTimeEqualsOrdinal(string expected, string actual)
    {
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(expected),
            Encoding.UTF8.GetBytes(actual));
    }

    private static bool FixedTimeEqualsOrdinalIgnoreCase(string expected, string actual)
    {
        return FixedTimeEqualsOrdinal(expected.ToUpperInvariant(), actual.ToUpperInvariant());
    }

    private sealed class PreparedRequestBody(
        Stream content,
        long? contentLength,
        string? tempFilePath,
        IReadOnlyDictionary<string, string>? trailerHeaders,
        IReadOnlyList<KeyValuePair<string, string>>? trailerHeaderEntries,
        string? finalChunkSignature) : IAsyncDisposable
    {
        public Stream Content { get; } = content;

        public long? ContentLength { get; } = contentLength;

        public IReadOnlyDictionary<string, string>? TrailerHeaders { get; } = trailerHeaders;

        public IReadOnlyList<KeyValuePair<string, string>>? TrailerHeaderEntries { get; } = trailerHeaderEntries;

        public string? FinalChunkSignature { get; } = finalChunkSignature;

        public async ValueTask DisposeAsync()
        {
            if (tempFilePath is null) {
                return;
            }

            await Content.DisposeAsync();
            if (File.Exists(tempFilePath)) {
                File.Delete(tempFilePath);
            }
        }
    }

    private enum S3AddressingStyle
    {
        Path,
        VirtualHosted
    }

    private sealed record ResolvedS3Request(
        string BucketName,
        string? Key,
        S3AddressingStyle AddressingStyle,
        string CanonicalResourcePath,
        string CanonicalPath);

    private readonly record struct AclHeaderGrantee(string Type, string Value);

    private sealed record CopySourceReference(string BucketName, string Key, string? VersionId);

    private sealed record ListBucketResultEntry(ObjectInfo? Object, string? CommonPrefix, string ContinuationToken)
    {
        public static ListBucketResultEntry ForObject(ObjectInfo @object)
        {
            ArgumentNullException.ThrowIfNull(@object);
            return new ListBucketResultEntry(@object, null, @object.Key);
        }

        public static ListBucketResultEntry ForCommonPrefix(string commonPrefix, string continuationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(commonPrefix);
            ArgumentException.ThrowIfNullOrWhiteSpace(continuationToken);
            return new ListBucketResultEntry(null, commonPrefix, continuationToken);
        }
    }

    private sealed record ListObjectVersionsResultEntry(ObjectInfo? Version, string? CommonPrefix, string NextKeyMarker, string? NextVersionIdMarker)
    {
        public static ListObjectVersionsResultEntry ForVersion(ObjectInfo version)
        {
            ArgumentNullException.ThrowIfNull(version);
            return new ListObjectVersionsResultEntry(version, null, version.Key, version.VersionId);
        }

        public static ListObjectVersionsResultEntry ForCommonPrefix(string commonPrefix, string nextKeyMarker, string? nextVersionIdMarker)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(commonPrefix);
            ArgumentException.ThrowIfNullOrWhiteSpace(nextKeyMarker);
            return new ListObjectVersionsResultEntry(null, commonPrefix, nextKeyMarker, nextVersionIdMarker);
        }
    }

    private sealed record ListMultipartUploadsResultEntry(MultipartUploadInfo? Upload, string? CommonPrefix, string NextKeyMarker, string? NextUploadIdMarker)
    {
        public static ListMultipartUploadsResultEntry ForUpload(MultipartUploadInfo upload)
        {
            ArgumentNullException.ThrowIfNull(upload);
            return new ListMultipartUploadsResultEntry(upload, null, upload.Key, upload.UploadId);
        }

        public static ListMultipartUploadsResultEntry ForCommonPrefix(string commonPrefix, string nextKeyMarker, string? nextUploadIdMarker)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(commonPrefix);
            ArgumentException.ThrowIfNullOrWhiteSpace(nextKeyMarker);
            return new ListMultipartUploadsResultEntry(null, commonPrefix, nextKeyMarker, nextUploadIdMarker);
        }
    }

    // ── Bucket Tagging ──────────────────────────────────────────────────────

    private static async Task<IResult> GetBucketTaggingAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketTaggingAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteBucketTagging(new S3BucketTagging
                    {
                        TagSet = result.Value!.Tags
                            .OrderBy(static kv => kv.Key, StringComparer.Ordinal)
                            .Select(static kv => new S3Tag { Key = kv.Key, Value = kv.Value })
                            .ToArray()
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketTaggingAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3BucketTagging requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadBucketTaggingAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketTaggingAsync(new PutBucketTaggingRequest
                {
                    BucketName = bucketName,
                    Tags = requestBody.TagSet.ToDictionary(static tag => tag.Key, static tag => tag.Value, StringComparer.Ordinal)
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketTaggingAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketTaggingAsync(new DeleteBucketTaggingRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Logging ──────────────────────────────────────────────────────

    private static async Task<IResult> GetBucketLoggingAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketLoggingAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var config = result.Value!;
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteBucketLoggingStatus(new S3BucketLoggingStatus
                    {
                        LoggingEnabled = config.TargetBucket is not null
                            ? new S3LoggingEnabled { TargetBucket = config.TargetBucket, TargetPrefix = config.TargetPrefix ?? string.Empty }
                            : null
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketLoggingAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3BucketLoggingStatus requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadBucketLoggingStatusAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketLoggingAsync(new PutBucketLoggingRequest
                {
                    BucketName = bucketName,
                    TargetBucket = requestBody.LoggingEnabled?.TargetBucket,
                    TargetPrefix = requestBody.LoggingEnabled?.TargetPrefix
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Website ──────────────────────────────────────────────────────

    private static async Task<IResult> GetBucketWebsiteAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketWebsiteAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var config = result.Value!;
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteWebsiteConfiguration(new S3WebsiteConfiguration
                    {
                        IndexDocument = config.IndexDocumentSuffix is not null ? new S3WebsiteIndexDocument { Suffix = config.IndexDocumentSuffix } : null,
                        ErrorDocument = config.ErrorDocumentKey is not null ? new S3WebsiteErrorDocument { Key = config.ErrorDocumentKey } : null,
                        RedirectAllRequestsTo = config.RedirectAllRequestsTo is not null
                            ? new S3WebsiteRedirectAllRequestsTo { HostName = config.RedirectAllRequestsTo.HostName, Protocol = config.RedirectAllRequestsTo.Protocol }
                            : null,
                        RoutingRules = config.RoutingRules.Select(static r => new S3WebsiteRoutingRule
                        {
                            Condition = r.Condition is not null ? new S3WebsiteRoutingRuleCondition { KeyPrefixEquals = r.Condition.KeyPrefixEquals, HttpErrorCodeReturnedEquals = r.Condition.HttpErrorCodeReturnedEquals } : null,
                            Redirect = new S3WebsiteRoutingRuleRedirect
                            {
                                HostName = r.Redirect.HostName,
                                Protocol = r.Redirect.Protocol,
                                ReplaceKeyPrefixWith = r.Redirect.ReplaceKeyPrefixWith,
                                ReplaceKeyWith = r.Redirect.ReplaceKeyWith,
                                HttpRedirectCode = r.Redirect.HttpRedirectCode
                            }
                        }).ToArray()
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketWebsiteAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3WebsiteConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadWebsiteConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketWebsiteAsync(new PutBucketWebsiteRequest
                {
                    BucketName = bucketName,
                    IndexDocumentSuffix = requestBody.IndexDocument?.Suffix,
                    ErrorDocumentKey = requestBody.ErrorDocument?.Key,
                    RedirectAllRequestsTo = requestBody.RedirectAllRequestsTo is not null
                        ? new BucketWebsiteRedirectAllRequestsTo { HostName = requestBody.RedirectAllRequestsTo.HostName, Protocol = requestBody.RedirectAllRequestsTo.Protocol }
                        : null,
                    RoutingRules = requestBody.RoutingRules.Select(static r => new BucketWebsiteRoutingRule
                    {
                        Condition = r.Condition is not null ? new BucketWebsiteRoutingRuleCondition { KeyPrefixEquals = r.Condition.KeyPrefixEquals, HttpErrorCodeReturnedEquals = r.Condition.HttpErrorCodeReturnedEquals } : null,
                        Redirect = new BucketWebsiteRoutingRuleRedirect
                        {
                            HostName = r.Redirect.HostName,
                            Protocol = r.Redirect.Protocol,
                            ReplaceKeyPrefixWith = r.Redirect.ReplaceKeyPrefixWith,
                            ReplaceKeyWith = r.Redirect.ReplaceKeyWith,
                            HttpRedirectCode = r.Redirect.HttpRedirectCode
                        }
                    }).ToArray()
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketWebsiteAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketWebsiteAsync(new DeleteBucketWebsiteRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Request Payment ──────────────────────────────────────────────

    private static async Task<IResult> GetBucketRequestPaymentAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketRequestPaymentAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var payer = result.Value!.Payer == BucketPayer.Requester ? "Requester" : "BucketOwner";
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteRequestPaymentConfiguration(new S3RequestPaymentConfiguration
                    {
                        Payer = payer
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketRequestPaymentAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3RequestPaymentConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadRequestPaymentConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        var payer = requestBody.Payer switch
        {
            "Requester" => BucketPayer.Requester,
            "BucketOwner" => BucketPayer.BucketOwner,
            _ => (BucketPayer?)null
        };

        if (payer is null) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "Payer must be 'BucketOwner' or 'Requester'.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketRequestPaymentAsync(new PutBucketRequestPaymentRequest
                {
                    BucketName = bucketName,
                    Payer = payer.Value
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Accelerate ───────────────────────────────────────────────────

    private static async Task<IResult> GetBucketAccelerateAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketAccelerateAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var status = result.Value!.Status == BucketAccelerateStatus.Enabled ? "Enabled" : "Suspended";
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteAccelerateConfiguration(new S3AccelerateConfiguration
                    {
                        Status = status
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketAccelerateAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3AccelerateConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadAccelerateConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        var status = requestBody.Status switch
        {
            "Enabled" => BucketAccelerateStatus.Enabled,
            "Suspended" => BucketAccelerateStatus.Suspended,
            _ => (BucketAccelerateStatus?)null
        };

        if (status is null) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "Accelerate status must be 'Enabled' or 'Suspended'.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketAccelerateAsync(new PutBucketAccelerateRequest
                {
                    BucketName = bucketName,
                    Status = status.Value
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Lifecycle ────────────────────────────────────────────────────

    private static async Task<IResult> GetBucketLifecycleAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketLifecycleAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteLifecycleConfiguration(new S3LifecycleConfiguration
                    {
                        Rules = result.Value!.Rules.Select(ToS3LifecycleRule).ToArray()
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketLifecycleAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3LifecycleConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadLifecycleConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketLifecycleAsync(new PutBucketLifecycleRequest
                {
                    BucketName = bucketName,
                    Rules = requestBody.Rules.Select(ToBucketLifecycleRule).ToArray()
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketLifecycleAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketLifecycleAsync(new DeleteBucketLifecycleRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Replication ──────────────────────────────────────────────────

    private static async Task<IResult> GetBucketReplicationAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketReplicationAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var config = result.Value!;
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteReplicationConfiguration(new S3ReplicationConfiguration
                    {
                        Role = config.Role,
                        Rules = config.Rules.Select(ToS3ReplicationRule).ToArray()
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketReplicationAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3ReplicationConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadReplicationConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketReplicationAsync(new PutBucketReplicationRequest
                {
                    BucketName = bucketName,
                    Role = requestBody.Role,
                    Rules = requestBody.Rules.Select(ToBucketReplicationRule).ToArray()
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketReplicationAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketReplicationAsync(new DeleteBucketReplicationRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Notification ─────────────────────────────────────────────────

    private static async Task<IResult> GetBucketNotificationAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketNotificationConfigurationAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var config = result.Value!;
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteNotificationConfiguration(new S3NotificationConfiguration
                    {
                        TopicConfigurations = config.TopicConfigurations.Select(static t => new S3TopicConfiguration
                        {
                            Id = t.Id,
                            Topic = t.TopicArn,
                            Events = t.Events,
                            Filter = ToS3NotificationFilter(t.Filter)
                        }).ToArray(),
                        QueueConfigurations = config.QueueConfigurations.Select(static q => new S3QueueConfiguration
                        {
                            Id = q.Id,
                            Queue = q.QueueArn,
                            Events = q.Events,
                            Filter = ToS3NotificationFilter(q.Filter)
                        }).ToArray(),
                        CloudFunctionConfigurations = config.LambdaFunctionConfigurations.Select(static l => new S3CloudFunctionConfiguration
                        {
                            Id = l.Id,
                            CloudFunction = l.LambdaFunctionArn,
                            Events = l.Events,
                            Filter = ToS3NotificationFilter(l.Filter)
                        }).ToArray()
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketNotificationAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3NotificationConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadNotificationConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketNotificationConfigurationAsync(new PutBucketNotificationConfigurationRequest
                {
                    BucketName = bucketName,
                    TopicConfigurations = requestBody.TopicConfigurations.Select(static t => new BucketNotificationTopicConfiguration
                    {
                        Id = t.Id,
                        TopicArn = t.Topic,
                        Events = t.Events,
                        Filter = ToBucketNotificationFilter(t.Filter)
                    }).ToArray(),
                    QueueConfigurations = requestBody.QueueConfigurations.Select(static q => new BucketNotificationQueueConfiguration
                    {
                        Id = q.Id,
                        QueueArn = q.Queue,
                        Events = q.Events,
                        Filter = ToBucketNotificationFilter(q.Filter)
                    }).ToArray(),
                    LambdaFunctionConfigurations = requestBody.CloudFunctionConfigurations.Select(static l => new BucketNotificationLambdaConfiguration
                    {
                        Id = l.Id,
                        LambdaFunctionArn = l.CloudFunction,
                        Events = l.Events,
                        Filter = ToBucketNotificationFilter(l.Filter)
                    }).ToArray()
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Object Lock ──────────────────────────────────────────────────

    private static async Task<IResult> GetBucketObjectLockAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetObjectLockConfigurationAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var config = result.Value!;
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteObjectLockConfiguration(new S3ObjectLockConfiguration
                    {
                        ObjectLockEnabled = config.ObjectLockEnabled ? "Enabled" : null,
                        DefaultRetention = config.DefaultRetention is not null
                            ? new S3ObjectLockDefaultRetention
                            {
                                Mode = ToS3RetentionMode(config.DefaultRetention.Mode),
                                Days = config.DefaultRetention.Days,
                                Years = config.DefaultRetention.Years
                            }
                            : null
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketObjectLockAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3ObjectLockConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadObjectLockConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        ObjectLockDefaultRetention? defaultRetention = null;
        if (requestBody.DefaultRetention is not null) {
            var retentionMode = requestBody.DefaultRetention.Mode switch
            {
                "GOVERNANCE" => ObjectRetentionMode.Governance,
                "COMPLIANCE" => ObjectRetentionMode.Compliance,
                _ => (ObjectRetentionMode?)null
            };

            if (retentionMode is null && requestBody.DefaultRetention.Mode is not null) {
                return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "Object lock default retention mode must be 'GOVERNANCE' or 'COMPLIANCE'.", BuildObjectResource(bucketName, null), bucketName);
            }

            defaultRetention = new ObjectLockDefaultRetention
            {
                Mode = retentionMode ?? ObjectRetentionMode.Governance,
                Days = requestBody.DefaultRetention.Days,
                Years = requestBody.DefaultRetention.Years
            };
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutObjectLockConfigurationAsync(new PutObjectLockConfigurationRequest
                {
                    BucketName = bucketName,
                    ObjectLockEnabled = string.Equals(requestBody.ObjectLockEnabled, "Enabled", StringComparison.OrdinalIgnoreCase),
                    DefaultRetention = defaultRetention
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Analytics ────────────────────────────────────────────────────

    private static async Task<IResult> ListBucketAnalyticsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.ListBucketAnalyticsConfigurationsAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var configs = result.Value!;
                var s3Result = new S3ListAnalyticsConfigurationsResult
                {
                    AnalyticsConfigurations = configs.Select(ToS3AnalyticsConfiguration).ToList(),
                    IsTruncated = false
                };
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteListAnalyticsConfigurations(s3Result),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> GetBucketAnalyticsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var id = httpContext.Request.Query[IdQueryParameterName].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "An analytics configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketAnalyticsConfigurationAsync(bucketName, id, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var config = result.Value!;
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteAnalyticsConfiguration(ToS3AnalyticsConfiguration(config)),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketAnalyticsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3AnalyticsConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadAnalyticsConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        if (string.IsNullOrWhiteSpace(requestBody.Id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "An analytics configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketAnalyticsConfigurationAsync(new PutBucketAnalyticsConfigurationRequest
                {
                    BucketName = bucketName,
                    Id = requestBody.Id,
                    FilterPrefix = requestBody.FilterPrefix,
                    FilterTags = requestBody.FilterTags?.ToDictionary(static t => t.Key, static t => t.Value, StringComparer.Ordinal),
                    StorageClassAnalysis = requestBody.StorageClassAnalysis is not null
                        ? new BucketAnalyticsStorageClassAnalysis
                        {
                            DataExport = requestBody.StorageClassAnalysis.DataExport is not null
                                ? new BucketAnalyticsDataExport
                                {
                                    OutputSchemaVersion = requestBody.StorageClassAnalysis.DataExport.OutputSchemaVersion,
                                    Destination = requestBody.StorageClassAnalysis.DataExport.Destination is not null
                                        ? new BucketAnalyticsS3BucketDestination
                                        {
                                            Format = requestBody.StorageClassAnalysis.DataExport.Destination.Format,
                                            BucketAccountId = requestBody.StorageClassAnalysis.DataExport.Destination.BucketAccountId,
                                            Bucket = requestBody.StorageClassAnalysis.DataExport.Destination.Bucket,
                                            Prefix = requestBody.StorageClassAnalysis.DataExport.Destination.Prefix
                                        }
                                        : null
                                }
                                : null
                        }
                        : null
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketAnalyticsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var id = httpContext.Request.Query[IdQueryParameterName].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "An analytics configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketAnalyticsConfigurationAsync(new DeleteBucketAnalyticsConfigurationRequest
                {
                    BucketName = bucketName,
                    Id = id
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Metrics ──────────────────────────────────────────────────────

    private static async Task<IResult> ListBucketMetricsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.ListBucketMetricsConfigurationsAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var configs = result.Value!;
                var s3Result = new S3ListMetricsConfigurationsResult
                {
                    MetricsConfigurations = configs.Select(ToS3MetricsConfiguration).ToList(),
                    IsTruncated = false
                };
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteListMetricsConfigurations(s3Result),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> GetBucketMetricsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var id = httpContext.Request.Query[IdQueryParameterName].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "A metrics configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketMetricsConfigurationAsync(bucketName, id, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var config = result.Value!;
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteMetricsConfiguration(ToS3MetricsConfiguration(config)),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketMetricsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3MetricsConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadMetricsConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        if (string.IsNullOrWhiteSpace(requestBody.Id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "A metrics configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketMetricsConfigurationAsync(new PutBucketMetricsConfigurationRequest
                {
                    BucketName = bucketName,
                    Id = requestBody.Id,
                    Filter = requestBody.Filter is not null
                        ? new BucketMetricsFilter
                        {
                            Prefix = requestBody.Filter.Prefix,
                            AccessPointArn = requestBody.Filter.AccessPointArn,
                            Tags = requestBody.Filter.Tags.ToDictionary(static t => t.Key, static t => t.Value, StringComparer.Ordinal)
                        }
                        : null
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketMetricsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var id = httpContext.Request.Query[IdQueryParameterName].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "A metrics configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketMetricsConfigurationAsync(new DeleteBucketMetricsConfigurationRequest
                {
                    BucketName = bucketName,
                    Id = id
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Inventory ────────────────────────────────────────────────────

    private static async Task<IResult> ListBucketInventoryAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.ListBucketInventoryConfigurationsAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var configs = result.Value!;
                var s3Result = new S3ListInventoryConfigurationsResult
                {
                    InventoryConfigurations = configs.Select(ToS3InventoryConfiguration).ToList(),
                    IsTruncated = false
                };
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteListInventoryConfigurations(s3Result),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> GetBucketInventoryAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var id = httpContext.Request.Query[IdQueryParameterName].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "An inventory configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketInventoryConfigurationAsync(bucketName, id, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var config = result.Value!;
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteInventoryConfiguration(ToS3InventoryConfiguration(config)),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketInventoryAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3InventoryConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadInventoryConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        if (string.IsNullOrWhiteSpace(requestBody.Id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "An inventory configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketInventoryConfigurationAsync(new PutBucketInventoryConfigurationRequest
                {
                    BucketName = bucketName,
                    Id = requestBody.Id,
                    IsEnabled = requestBody.IsEnabled,
                    Destination = requestBody.Destination is not null
                        ? new BucketInventoryDestination
                        {
                            S3BucketDestination = requestBody.Destination.S3BucketDestination is not null
                                ? new BucketInventoryS3BucketDestination
                                {
                                    Format = requestBody.Destination.S3BucketDestination.Format,
                                    AccountId = requestBody.Destination.S3BucketDestination.AccountId,
                                    Bucket = requestBody.Destination.S3BucketDestination.Bucket,
                                    Prefix = requestBody.Destination.S3BucketDestination.Prefix
                                }
                                : null
                        }
                        : null,
                    Schedule = requestBody.Schedule is not null
                        ? new BucketInventorySchedule { Frequency = requestBody.Schedule.Frequency }
                        : null,
                    Filter = requestBody.Filter is not null
                        ? new BucketInventoryFilter { Prefix = requestBody.Filter.Prefix }
                        : null,
                    IncludedObjectVersions = requestBody.IncludedObjectVersions,
                    OptionalFields = requestBody.OptionalFields
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketInventoryAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var id = httpContext.Request.Query[IdQueryParameterName].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "An inventory configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketInventoryConfigurationAsync(new DeleteBucketInventoryConfigurationRequest
                {
                    BucketName = bucketName,
                    Id = id
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Bucket Intelligent-Tiering ──────────────────────────────────────────

    private static async Task<IResult> ListBucketIntelligentTieringAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.ListBucketIntelligentTieringConfigurationsAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var configs = result.Value!;
                var s3Result = new S3ListIntelligentTieringConfigurationsResult
                {
                    IntelligentTieringConfigurations = configs.Select(ToS3IntelligentTieringConfiguration).ToList(),
                    IsTruncated = false
                };
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteListIntelligentTieringConfigurations(s3Result),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> GetBucketIntelligentTieringAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var id = httpContext.Request.Query[IdQueryParameterName].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "An intelligent-tiering configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketIntelligentTieringConfigurationAsync(bucketName, id, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var config = result.Value!;
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteIntelligentTieringConfiguration(ToS3IntelligentTieringConfiguration(config)),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketIntelligentTieringAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3IntelligentTieringConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadIntelligentTieringConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        if (string.IsNullOrWhiteSpace(requestBody.Id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "An intelligent-tiering configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketIntelligentTieringConfigurationAsync(new PutBucketIntelligentTieringConfigurationRequest
                {
                    BucketName = bucketName,
                    Id = requestBody.Id,
                    Status = requestBody.Status,
                    Filter = requestBody.Filter is not null
                        ? new BucketIntelligentTieringFilter
                        {
                            Prefix = requestBody.Filter.Prefix,
                            Tags = requestBody.Filter.Tags.ToDictionary(static t => t.Key, static t => t.Value, StringComparer.Ordinal)
                        }
                        : null,
                    Tierings = requestBody.Tierings.Select(static t => new BucketIntelligentTiering
                    {
                        AccessTier = t.AccessTier,
                        Days = t.Days
                    }).ToArray()
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> DeleteBucketIntelligentTieringAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var id = httpContext.Request.Query[IdQueryParameterName].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(id)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "An intelligent-tiering configuration id is required.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteBucketIntelligentTieringConfigurationAsync(new DeleteBucketIntelligentTieringConfigurationRequest
                {
                    BucketName = bucketName,
                    Id = id
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    // ── Object Retention (PUT) ──────────────────────────────────────────────

    private static async Task<IResult> PutObjectRetentionAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3ObjectRetention requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadObjectRetentionAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
        }

        ObjectRetentionMode? mode = requestBody.Mode switch
        {
            "GOVERNANCE" => ObjectRetentionMode.Governance,
            "COMPLIANCE" => ObjectRetentionMode.Compliance,
            null => null,
            _ => (ObjectRetentionMode?)null
        };

        if (requestBody.Mode is not null && mode is null) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "Object retention mode must be 'GOVERNANCE' or 'COMPLIANCE'.", BuildObjectResource(bucketName, key), bucketName, key);
        }

        var bypassGovernance = string.Equals(
            httpContext.Request.Headers["x-amz-bypass-governance-retention"].FirstOrDefault(),
            "true",
            StringComparison.OrdinalIgnoreCase);

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutObjectRetentionAsync(new PutObjectRetentionRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request),
                    Mode = mode,
                    RetainUntilDateUtc = requestBody.RetainUntilDateUtc,
                    BypassGovernanceRetention = bypassGovernance
                }, innerCancellationToken);

                if (result.IsSuccess && result.Value is not null) {
                    ApplyVersionIdHeader(httpContext.Response, result.Value.VersionId);
                }

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
    }

    // ── Object Legal Hold (PUT) ─────────────────────────────────────────────

    private static async Task<IResult> PutObjectLegalHoldAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3ObjectLegalHold requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadObjectLegalHoldAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
        }

        var status = requestBody.Status switch
        {
            "ON" => ObjectLegalHoldStatus.On,
            "OFF" => ObjectLegalHoldStatus.Off,
            _ => (ObjectLegalHoldStatus?)null
        };

        if (status is null) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "Legal hold status must be 'ON' or 'OFF'.", BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutObjectLegalHoldAsync(new PutObjectLegalHoldRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request),
                    Status = status.Value
                }, innerCancellationToken);

                if (result.IsSuccess && result.Value is not null) {
                    ApplyVersionIdHeader(httpContext.Response, result.Value.VersionId);
                }

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
    }

    // ── Restore Object ──────────────────────────────────────────────────────

    private static async Task<IResult> RestoreObjectAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3RestoreRequest requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadRestoreRequestAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.RestoreObjectAsync(new RestoreObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request),
                    Days = requestBody.Days,
                    GlacierTier = requestBody.GlacierJobTier
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                return result.Value!.IsAlreadyRestored
                    ? TypedResults.Ok()
                    : TypedResults.StatusCode(StatusCodes.Status202Accepted);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
    }

    // ── Select Object Content ───────────────────────────────────────────────

    private static async Task<IResult> SelectObjectContentAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        SelectObjectContentRequest serviceRequest;
        try {
            serviceRequest = await ParseSelectObjectContentRequestAsync(bucketName, key, httpContext.Request, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.SelectObjectContentAsync(serviceRequest, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var response = result.Value!;
                return new SelectObjectContentStreamResult(response);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<SelectObjectContentRequest> ParseSelectObjectContentRequestAsync(
        string bucketName, string key, HttpRequest request, CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(request.Body, leaveOpen: true);
        var xml = await reader.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(xml)) {
            throw new FormatException("The select object content request body is required.");
        }

        try {
            var document = XDocument.Parse(xml, LoadOptions.None);
            var root = document.Root;
            if (root is null || !string.Equals(root.Name.LocalName, "SelectObjectContentRequest", StringComparison.Ordinal)) {
                throw new FormatException("The select object content request body must contain a root 'SelectObjectContentRequest' element.");
            }

            var inputSerialization = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "InputSerialization", StringComparison.Ordinal));
            var outputSerialization = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "OutputSerialization", StringComparison.Ordinal));

            return new SelectObjectContentRequest
            {
                BucketName = bucketName,
                Key = key,
                Expression = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "Expression", StringComparison.Ordinal))?.Value
                    ?? throw new FormatException("Expression element is required."),
                ExpressionType = root.Elements().FirstOrDefault(static element => string.Equals(element.Name.LocalName, "ExpressionType", StringComparison.Ordinal))?.Value
                    ?? throw new FormatException("ExpressionType element is required."),
                InputSerializationJson = GetSerializationElementXml(inputSerialization, "JSON"),
                InputSerializationCsv = GetSerializationElementXml(inputSerialization, "CSV"),
                InputSerializationParquet = GetSerializationElementXml(inputSerialization, "Parquet"),
                OutputSerializationJson = GetSerializationElementXml(outputSerialization, "JSON"),
                OutputSerializationCsv = GetSerializationElementXml(outputSerialization, "CSV")
            };
        }
        catch (FormatException) {
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException) {
            throw new FormatException("The select object content request body is not valid XML.", exception);
        }
    }

    private static string? GetSerializationElementXml(XElement? parentElement, string elementName)
    {
        return parentElement?.Elements().FirstOrDefault(element => string.Equals(element.Name.LocalName, elementName, StringComparison.Ordinal))
            ?.ToString(SaveOptions.DisableFormatting);
    }

    private sealed class SelectObjectContentStreamResult : IResult
    {
        private readonly SelectObjectContentResponse _response;

        public SelectObjectContentStreamResult(SelectObjectContentResponse response) => _response = response;

        public async Task ExecuteAsync(HttpContext httpContext)
        {
            httpContext.Response.StatusCode = StatusCodes.Status200OK;
            httpContext.Response.ContentType = _response.ContentType ?? "application/octet-stream";
            await using var eventStream = _response.EventStream;
            await eventStream.CopyToAsync(httpContext.Response.Body, httpContext.RequestAborted);
        }
    }

    // ── Domain ↔ Protocol Conversion Helpers ────────────────────────────────

    private static S3LifecycleRule ToS3LifecycleRule(BucketLifecycleRule rule)
    {
        return new S3LifecycleRule
        {
            Id = rule.Id,
            FilterPrefix = rule.FilterPrefix,
            FilterTags = rule.FilterTags?.Select(static kv => new S3LifecycleFilterTag { Key = kv.Key, Value = kv.Value }).ToArray(),
            Status = rule.Status == BucketLifecycleRuleStatus.Enabled ? "Enabled" : "Disabled",
            ExpirationDays = rule.ExpirationDays,
            ExpirationDate = rule.ExpirationDate?.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture),
            ExpiredObjectDeleteMarker = rule.ExpiredObjectDeleteMarker,
            NoncurrentVersionExpirationDays = rule.NoncurrentVersionExpirationDays,
            AbortIncompleteMultipartUploadDaysAfterInitiation = rule.AbortIncompleteMultipartUploadDaysAfterInitiation,
            Transitions = rule.Transitions.Select(static t => new S3LifecycleTransition
            {
                Days = t.Days,
                Date = t.Date?.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture),
                StorageClass = t.StorageClass
            }).ToArray(),
            NoncurrentVersionTransitions = rule.NoncurrentVersionTransitions.Select(static t => new S3LifecycleNoncurrentVersionTransition
            {
                NoncurrentDays = t.NoncurrentDays,
                StorageClass = t.StorageClass
            }).ToArray()
        };
    }

    private static BucketLifecycleRule ToBucketLifecycleRule(S3LifecycleRule rule)
    {
        return new BucketLifecycleRule
        {
            Id = rule.Id,
            FilterPrefix = rule.FilterPrefix,
            FilterTags = rule.FilterTags?.ToDictionary(static t => t.Key, static t => t.Value, StringComparer.Ordinal),
            Status = string.Equals(rule.Status, "Enabled", StringComparison.OrdinalIgnoreCase) ? BucketLifecycleRuleStatus.Enabled : BucketLifecycleRuleStatus.Disabled,
            ExpirationDays = rule.ExpirationDays,
            ExpirationDate = !string.IsNullOrEmpty(rule.ExpirationDate) ? DateTimeOffset.Parse(rule.ExpirationDate, CultureInfo.InvariantCulture) : null,
            ExpiredObjectDeleteMarker = rule.ExpiredObjectDeleteMarker,
            NoncurrentVersionExpirationDays = rule.NoncurrentVersionExpirationDays,
            AbortIncompleteMultipartUploadDaysAfterInitiation = rule.AbortIncompleteMultipartUploadDaysAfterInitiation,
            Transitions = rule.Transitions.Select(static t => new BucketLifecycleTransition
            {
                Days = t.Days,
                Date = !string.IsNullOrEmpty(t.Date) ? DateTimeOffset.Parse(t.Date, CultureInfo.InvariantCulture) : null,
                StorageClass = t.StorageClass
            }).ToArray(),
            NoncurrentVersionTransitions = rule.NoncurrentVersionTransitions.Select(static t => new BucketLifecycleNoncurrentVersionTransition
            {
                NoncurrentDays = t.NoncurrentDays,
                StorageClass = t.StorageClass
            }).ToArray()
        };
    }

    private static S3ReplicationRule ToS3ReplicationRule(BucketReplicationRule rule)
    {
        return new S3ReplicationRule
        {
            Id = rule.Id,
            Status = rule.Status == BucketReplicationRuleStatus.Enabled ? "Enabled" : "Disabled",
            FilterPrefix = rule.FilterPrefix,
            Destination = new S3ReplicationDestination
            {
                Bucket = rule.Destination.Bucket,
                StorageClass = rule.Destination.StorageClass,
                Account = rule.Destination.Account
            },
            Priority = rule.Priority,
            DeleteMarkerReplication = rule.DeleteMarkerReplication
        };
    }

    private static BucketReplicationRule ToBucketReplicationRule(S3ReplicationRule rule)
    {
        return new BucketReplicationRule
        {
            Id = rule.Id,
            Status = string.Equals(rule.Status, "Enabled", StringComparison.OrdinalIgnoreCase) ? BucketReplicationRuleStatus.Enabled : BucketReplicationRuleStatus.Disabled,
            FilterPrefix = rule.FilterPrefix,
            Destination = new BucketReplicationDestination
            {
                Bucket = rule.Destination.Bucket,
                StorageClass = rule.Destination.StorageClass,
                Account = rule.Destination.Account
            },
            Priority = rule.Priority,
            DeleteMarkerReplication = rule.DeleteMarkerReplication ?? false
        };
    }

    private static S3NotificationFilterRuleSet? ToS3NotificationFilter(BucketNotificationFilter? filter)
    {
        if (filter is null) return null;
        return new S3NotificationFilterRuleSet
        {
            S3KeyRules = filter.KeyFilterRules.Select(static r => new S3NotificationFilterRule
            {
                Name = r.Name,
                Value = r.Value
            }).ToArray()
        };
    }

    private static BucketNotificationFilter? ToBucketNotificationFilter(S3NotificationFilterRuleSet? filter)
    {
        if (filter is null) return null;
        return new BucketNotificationFilter
        {
            KeyFilterRules = filter.S3KeyRules.Select(static r => new BucketNotificationFilterRule
            {
                Name = r.Name,
                Value = r.Value
            }).ToArray()
        };
    }

    private static S3AnalyticsConfiguration ToS3AnalyticsConfiguration(BucketAnalyticsConfiguration config)
    {
        return new S3AnalyticsConfiguration
        {
            Id = config.Id,
            FilterPrefix = config.FilterPrefix,
            FilterTags = config.FilterTags?.Select(static kv => new S3AnalyticsFilterTag { Key = kv.Key, Value = kv.Value }).ToArray(),
            StorageClassAnalysis = config.StorageClassAnalysis is not null
                ? new S3StorageClassAnalysis
                {
                    DataExport = config.StorageClassAnalysis.DataExport is not null
                        ? new S3StorageClassAnalysisDataExport
                        {
                            OutputSchemaVersion = config.StorageClassAnalysis.DataExport.OutputSchemaVersion,
                            Destination = config.StorageClassAnalysis.DataExport.Destination is not null
                                ? new S3AnalyticsS3BucketDestination
                                {
                                    Format = config.StorageClassAnalysis.DataExport.Destination.Format,
                                    BucketAccountId = config.StorageClassAnalysis.DataExport.Destination.BucketAccountId,
                                    Bucket = config.StorageClassAnalysis.DataExport.Destination.Bucket,
                                    Prefix = config.StorageClassAnalysis.DataExport.Destination.Prefix
                                }
                                : null
                        }
                        : null
                }
                : null
        };
    }

    private static S3MetricsConfiguration ToS3MetricsConfiguration(BucketMetricsConfiguration config)
    {
        return new S3MetricsConfiguration
        {
            Id = config.Id,
            Filter = config.Filter is not null
                ? new S3MetricsFilter
                {
                    Prefix = config.Filter.Prefix,
                    AccessPointArn = config.Filter.AccessPointArn,
                    Tags = config.Filter.Tags.Select(static kv => new S3Tag { Key = kv.Key, Value = kv.Value }).ToArray()
                }
                : null
        };
    }

    private static S3InventoryConfiguration ToS3InventoryConfiguration(BucketInventoryConfiguration config)
    {
        return new S3InventoryConfiguration
        {
            Id = config.Id,
            IsEnabled = config.IsEnabled,
            Destination = config.Destination is not null
                ? new S3InventoryDestination
                {
                    S3BucketDestination = config.Destination.S3BucketDestination is not null
                        ? new S3InventoryS3BucketDestination
                        {
                            Format = config.Destination.S3BucketDestination.Format,
                            AccountId = config.Destination.S3BucketDestination.AccountId,
                            Bucket = config.Destination.S3BucketDestination.Bucket,
                            Prefix = config.Destination.S3BucketDestination.Prefix
                        }
                        : null
                }
                : null,
            Schedule = config.Schedule is not null
                ? new S3InventorySchedule { Frequency = config.Schedule.Frequency }
                : null,
            Filter = config.Filter is not null
                ? new S3InventoryFilter { Prefix = config.Filter.Prefix }
                : null,
            IncludedObjectVersions = config.IncludedObjectVersions,
            OptionalFields = config.OptionalFields
        };
    }

    private static S3IntelligentTieringConfiguration ToS3IntelligentTieringConfiguration(BucketIntelligentTieringConfiguration config)
    {
        return new S3IntelligentTieringConfiguration
        {
            Id = config.Id,
            Status = config.Status,
            Filter = config.Filter is not null
                ? new S3IntelligentTieringFilter
                {
                    Prefix = config.Filter.Prefix,
                    Tags = config.Filter.Tags.Select(static kv => new S3Tag { Key = kv.Key, Value = kv.Value }).ToArray()
                }
                : null,
            Tierings = config.Tierings.Select(static t => new S3Tiering
            {
                AccessTier = t.AccessTier,
                Days = t.Days
            }).ToArray()
        };
    }
}

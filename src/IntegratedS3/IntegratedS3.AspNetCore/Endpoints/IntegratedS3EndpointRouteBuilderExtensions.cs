using System.Net;
using System.Globalization;
using System.Text;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore.Services;
using IntegratedS3.Protocol;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Net.Http.Headers;
using IntegratedS3.Core.Services;

namespace IntegratedS3.AspNetCore.Endpoints;

public static class IntegratedS3EndpointRouteBuilderExtensions
{
    private const string SigV4AuthenticationClaimType = "integrateds3:auth-type";
    private const string SigV4AuthenticationClaimValue = "sigv4";
    private const string MetadataHeaderPrefix = "x-integrateds3-meta-";
    private const string ContinuationTokenHeaderName = "x-integrateds3-continuation-token";
    private const string CopySourceHeaderName = "x-amz-copy-source";
    private const string CopySourceIfMatchHeaderName = "x-amz-copy-source-if-match";
    private const string CopySourceIfNoneMatchHeaderName = "x-amz-copy-source-if-none-match";
    private const string CopySourceIfModifiedSinceHeaderName = "x-amz-copy-source-if-modified-since";
    private const string CopySourceIfUnmodifiedSinceHeaderName = "x-amz-copy-source-if-unmodified-since";
    private const string XmlContentType = "application/xml";
    private const string ListTypeQueryParameterName = "list-type";
    private const string PrefixQueryParameterName = "prefix";
    private const string DelimiterQueryParameterName = "delimiter";
    private const string StartAfterQueryParameterName = "start-after";
    private const string MaxKeysQueryParameterName = "max-keys";
    private const string ContinuationTokenQueryParameterName = "continuation-token";
    private const string UploadsQueryParameterName = "uploads";
    private const string UploadIdQueryParameterName = "uploadId";
    private const string PartNumberQueryParameterName = "partNumber";
    private static readonly HashSet<string> SupportedBucketGetQueryParameters = [ListTypeQueryParameterName, PrefixQueryParameterName, DelimiterQueryParameterName, StartAfterQueryParameterName, MaxKeysQueryParameterName, ContinuationTokenQueryParameterName];
    private static readonly HashSet<string> SupportedBucketPostQueryParameters = ["delete"];

    public static RouteGroupBuilder MapIntegratedS3Endpoints(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var options = endpoints.ServiceProvider.GetRequiredService<IOptions<IntegratedS3Options>>().Value;
        var group = endpoints.MapGroup(options.RoutePrefix);
        group.AddEndpointFilter<IntegratedS3RequestAuthenticationEndpointFilter>();

        group.MapGet("/", HandleRootGetAsync)
            .WithName("GetIntegratedS3ServiceDocument");

        group.MapMethods("/", ["PUT", "HEAD", "DELETE", "POST"], HandleS3CompatibleRootAsync)
            .WithName("HandleIntegratedS3CompatibleRoot");

        group.MapGet("/capabilities", GetCapabilitiesAsync)
            .WithName("GetIntegratedS3Capabilities");

        group.MapGet("/buckets", ListBucketsAsync)
            .WithName("ListIntegratedS3Buckets");

        group.MapPut("/buckets/{bucketName}", CreateBucketAsync)
            .WithName("CreateIntegratedS3Bucket");

        group.MapMethods("/buckets/{bucketName}", ["HEAD"], HeadBucketAsync)
            .WithName("HeadIntegratedS3Bucket");

        group.MapDelete("/buckets/{bucketName}", DeleteBucketAsync)
            .WithName("DeleteIntegratedS3Bucket");

        group.MapGet("/buckets/{bucketName}/objects", ListObjectsAsync)
            .WithName("ListIntegratedS3Objects");

        group.MapPut("/buckets/{bucketName}/objects/{**key}", PutObjectAsync)
            .WithName("PutIntegratedS3Object");

        group.MapGet("/buckets/{bucketName}/objects/{**key}", GetObjectAsync)
            .WithName("GetIntegratedS3Object");

        group.MapMethods("/buckets/{bucketName}/objects/{**key}", ["HEAD"], HeadObjectAsync)
            .WithName("HeadIntegratedS3Object");

        group.MapDelete("/buckets/{bucketName}/objects/{**key}", DeleteObjectAsync)
            .WithName("DeleteIntegratedS3Object");

        group.MapMethods("/{**s3Path}", ["GET", "PUT", "HEAD", "DELETE", "POST"], HandleS3CompatiblePathAsync)
            .WithName("HandleIntegratedS3CompatiblePath");

        return group;
    }

    private static async Task<IResult> HandleRootGetAsync(
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        IStorageServiceDescriptorProvider descriptorProvider,
        CancellationToken cancellationToken)
    {
        if (TryResolveCompatibleRequest(httpContext.Request, options.Value, null, out var resolvedRequest, out var resolutionError)
            && resolvedRequest is not null
            && !string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            if (!string.IsNullOrWhiteSpace(resolutionError)) {
                return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError, resource: null);
            }

            return await ExecuteS3CompatibleBucketRequestAsync(resolvedRequest, httpContext, requestContextAccessor, storageService, cancellationToken);
        }

        if (IsSigV4AuthenticatedRequest(httpContext)) {
            return await ListBucketsS3CompatibleAsync(httpContext, requestContextAccessor, storageService, descriptorProvider, cancellationToken);
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
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.CreateBucketAsync(new CreateBucketRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken).AsTask(), cancellationToken);

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
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryResolveCompatibleRequest(httpContext.Request, options.Value, null, out var resolvedRequest, out var resolutionError)
            || resolvedRequest is null
            || string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError ?? "A bucket name could not be resolved from the request.", resource: null);
        }

        return await ExecuteS3CompatibleBucketRequestAsync(resolvedRequest, httpContext, requestContextAccessor, storageService, cancellationToken);
    }

    private static async Task<IResult> HandleS3CompatiblePathAsync(
        string s3Path,
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryResolveCompatibleRequest(httpContext.Request, options.Value, s3Path, out var resolvedRequest, out var resolutionError)
            || resolvedRequest is null
            || string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError ?? "A bucket name could not be resolved from the request.", resource: null);
        }

        return resolvedRequest.Key is null
            ? await ExecuteS3CompatibleBucketRequestAsync(resolvedRequest, httpContext, requestContextAccessor, storageService, cancellationToken)
            : await ExecuteS3CompatibleObjectRequestAsync(resolvedRequest, httpContext, requestContextAccessor, storageService, cancellationToken);
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
                innerCancellationToken => storageService.HeadBucketAsync(bucketName, innerCancellationToken).AsTask(),
                cancellationToken);
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
        try {
            var preparedBody = await PrepareRequestBodyAsync(request, cancellationToken);
            try {
                return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                if (TryGetCopySource(request, out var copySource, out var copySourceError)) {
                    if (copySourceError is not null) {
                        return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", copySourceError, BuildObjectResource(bucketName, key), bucketName, key);
                    }

                    var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
                    {
                        SourceBucketName = copySource!.BucketName,
                        SourceKey = copySource.Key,
                        DestinationBucketName = bucketName,
                        DestinationKey = key,
                        SourceIfMatchETag = request.Headers[CopySourceIfMatchHeaderName].ToString(),
                        SourceIfNoneMatchETag = request.Headers[CopySourceIfNoneMatchHeaderName].ToString(),
                        SourceIfModifiedSinceUtc = ParseOptionalHttpDateHeader(request.Headers[CopySourceIfModifiedSinceHeaderName].ToString()),
                        SourceIfUnmodifiedSinceUtc = ParseOptionalHttpDateHeader(request.Headers[CopySourceIfUnmodifiedSinceHeaderName].ToString())
                    }, innerCancellationToken);

                    return copyResult.IsSuccess
                        ? ToCopyObjectResult(copyResult.Value!)
                        : ToErrorResult(httpContext, copyResult.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var metadata = request.Headers
                    .Where(static pair => pair.Key.StartsWith(MetadataHeaderPrefix, StringComparison.OrdinalIgnoreCase))
                    .ToDictionary(
                        static pair => pair.Key[MetadataHeaderPrefix.Length..],
                        static pair => pair.Value.ToString(),
                        StringComparer.OrdinalIgnoreCase);

                var result = await storageService.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    Content = preparedBody.Content,
                    ContentLength = preparedBody.ContentLength,
                    ContentType = request.ContentType,
                    Metadata = metadata.Count == 0 ? null : metadata
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok(result.Value)
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
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
                var result = await storageService.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    Range = ParseRangeHeader(request.Headers.Range.ToString()),
                    IfMatchETag = request.Headers.IfMatch.ToString(),
                    IfNoneMatchETag = request.Headers.IfNoneMatch.ToString(),
                    IfModifiedSinceUtc = headers.IfModifiedSince,
                    IfUnmodifiedSinceUtc = headers.IfUnmodifiedSince
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
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
                var result = await storageService.HeadObjectAsync(new HeadObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    IfMatchETag = httpContext.Request.Headers.IfMatch.ToString(),
                    IfNoneMatchETag = httpContext.Request.Headers.IfNoneMatch.ToString(),
                    IfModifiedSinceUtc = headers.IfModifiedSince,
                    IfUnmodifiedSinceUtc = headers.IfUnmodifiedSince
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var objectInfo = result.Value!;
                ApplyObjectHeaders(httpContext.Response, objectInfo);
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
                    Key = key
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

    private static async Task<IResult> ExecuteS3CompatibleBucketRequestAsync(
        ResolvedS3Request resolvedRequest,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryValidateBucketRequestSubresources(httpContext.Request, out var validationErrorCode, out var validationMessage, out var validationStatusCode)) {
            return ToErrorResult(httpContext, validationStatusCode, validationErrorCode!, validationMessage!, resolvedRequest.CanonicalResourcePath, resolvedRequest.BucketName);
        }

        return httpContext.Request.Method switch
        {
            "GET" => await ListObjectsV2Async(
                resolvedRequest.BucketName,
                ParsePrefix(httpContext.Request),
                ParseDelimiter(httpContext.Request),
                ParseStartAfter(httpContext.Request),
                ParseContinuationToken(httpContext.Request),
                ParseMaxKeys(httpContext.Request),
                httpContext,
                requestContextAccessor,
                storageService,
                cancellationToken),
            "PUT" => await CreateBucketS3CompatibleAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "HEAD" => await HeadBucketAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" => await DeleteBucketAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when httpContext.Request.Query.ContainsKey("delete") => await DeleteObjectsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" => ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", "Unsupported bucket subresource request.", resolvedRequest.CanonicalResourcePath, resolvedRequest.BucketName),
            _ => TypedResults.StatusCode(StatusCodes.Status405MethodNotAllowed)
        };
    }

    private static async Task<IResult> CreateBucketS3CompatibleAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.CreateBucketAsync(new CreateBucketRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken).AsTask(), cancellationToken);

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
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var key = resolvedRequest.Key!;

        if (!TryValidateObjectRequestSubresources(httpContext.Request, out var validationErrorCode, out var validationMessage, out var validationStatusCode)) {
            return ToErrorResult(httpContext, validationStatusCode, validationErrorCode!, validationMessage!, resolvedRequest.CanonicalResourcePath, resolvedRequest.BucketName, key);
        }

        return httpContext.Request.Method switch
        {
            "POST" when httpContext.Request.Query.ContainsKey(UploadsQueryParameterName) => await InitiateMultipartUploadAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
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

    private static async Task<IResult> InitiateMultipartUploadAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var metadata = httpContext.Request.Headers
                    .Where(static pair => pair.Key.StartsWith(MetadataHeaderPrefix, StringComparison.OrdinalIgnoreCase))
                    .ToDictionary(
                        static pair => pair.Key[MetadataHeaderPrefix.Length..],
                        static pair => pair.Value.ToString(),
                        StringComparer.OrdinalIgnoreCase);

                var result = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentType = httpContext.Request.ContentType,
                    Metadata = metadata.Count == 0 ? null : metadata
                }, innerCancellationToken);

                return result.IsSuccess
                    ? new XmlContentResult(
                        S3XmlResponseWriter.WriteInitiateMultipartUploadResult(new S3InitiateMultipartUploadResult
                        {
                            Bucket = bucketName,
                            Key = key,
                            UploadId = result.Value!.UploadId
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
                return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                    var result = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        UploadId = uploadId!,
                        PartNumber = partNumber!.Value,
                        Content = preparedBody.Content,
                        ContentLength = preparedBody.ContentLength
                    }, innerCancellationToken);

                    if (!result.IsSuccess) {
                        return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                    }

                    httpContext.Response.Headers.ETag = QuoteETag(result.Value!.ETag);
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
                        LastModifiedUtc = default
                    }).ToArray()
                }, innerCancellationToken);

                return result.IsSuccess
                    ? new XmlContentResult(
                        S3XmlResponseWriter.WriteCompleteMultipartUploadResult(new S3CompleteMultipartUploadResult
                        {
                            Location = $"{httpContext.Request.Scheme}://{httpContext.Request.Host}{httpContext.Request.PathBase}{httpContext.Request.Path}",
                            Bucket = bucketName,
                            Key = key,
                            ETag = result.Value!.ETag ?? string.Empty
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

    private static async Task<IResult> ListObjectsV2Async(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? startAfter,
        string? continuationToken,
        int? maxKeys,
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
                        startAfter,
                        continuationToken,
                        requestedPageSize,
                        objects);

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

                    if (result.IsSuccess || result.Error?.Code == StorageErrorCode.ObjectNotFound) {
                        if (!deleteRequest.Quiet) {
                            deleted.Add(new S3DeletedObjectResult
                            {
                                Key = objectIdentifier.Key,
                                VersionId = objectIdentifier.VersionId
                            });
                        }

                        continue;
                    }

                    errors.Add(new S3DeleteObjectError
                    {
                        Key = objectIdentifier.Key,
                        VersionId = objectIdentifier.VersionId,
                        Code = ToS3ErrorCode(result.Error!.Code),
                        Message = result.Error.Message
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
            Principal = httpContext.User
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

    private static IResult ToErrorResult(HttpContext httpContext, StorageError? error, string? resourceOverride = null)
    {
        if (error is null) {
            return ToErrorResult(httpContext, StatusCodes.Status500InternalServerError, "InternalError", "Storage operation failed.", resourceOverride);
        }

        return ToErrorResult(
            httpContext,
            error.SuggestedHttpStatusCode ?? ToStatusCode(error.Code),
            ToS3ErrorCode(error.Code),
            error.Message,
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

    private static IResult ToCopyObjectResult(ObjectInfo @object)
    {
        return new XmlContentResult(
            S3XmlResponseWriter.WriteCopyObjectResult(new S3CopyObjectResult
            {
                ETag = @object.ETag ?? string.Empty,
                LastModifiedUtc = @object.LastModifiedUtc
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
            StorageErrorCode.AccessDenied => StatusCodes.Status403Forbidden,
            StorageErrorCode.InvalidRange => StatusCodes.Status416RangeNotSatisfiable,
            StorageErrorCode.PreconditionFailed => StatusCodes.Status412PreconditionFailed,
            StorageErrorCode.VersionConflict => StatusCodes.Status409Conflict,
            StorageErrorCode.BucketAlreadyExists => StatusCodes.Status409Conflict,
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
            StorageErrorCode.AccessDenied => "AccessDenied",
            StorageErrorCode.InvalidRange => "InvalidRange",
            StorageErrorCode.PreconditionFailed => "PreconditionFailed",
            StorageErrorCode.VersionConflict => "OperationAborted",
            StorageErrorCode.BucketAlreadyExists => "BucketAlreadyExists",
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

    private static string? ParseContinuationToken(HttpRequest request)
    {
        return request.Query.TryGetValue(ContinuationTokenQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static S3ListBucketResult BuildListBucketResult(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? startAfter,
        string? continuationToken,
        int maxKeys,
        IReadOnlyList<ObjectInfo> objects)
    {
        var normalizedPrefix = prefix ?? string.Empty;
        var normalizedDelimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter;
        var marker = string.IsNullOrWhiteSpace(continuationToken)
            ? startAfter
            : continuationToken;

        var entries = new List<ListBucketResultEntry>();

        for (var index = 0; index < objects.Count; index++) {
            var currentObject = objects[index];
            if (!currentObject.Key.StartsWith(normalizedPrefix, StringComparison.Ordinal)) {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(marker)
                && StringComparer.Ordinal.Compare(currentObject.Key, marker) <= 0) {
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
            Prefix = prefix,
            Delimiter = normalizedDelimiter,
            StartAfter = startAfter,
            ContinuationToken = continuationToken,
            NextContinuationToken = isTruncated ? page[^1].ContinuationToken : null,
            KeyCount = page.Length,
            MaxKeys = maxKeys,
            IsTruncated = isTruncated,
            Contents = page
                .Where(static entry => entry.Object is not null)
                .Select(static entry => new S3ListBucketObject
                {
                    Key = entry.Object!.Key,
                    ETag = entry.Object.ETag,
                    Size = entry.Object.ContentLength,
                    LastModifiedUtc = entry.Object.LastModifiedUtc
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

    private static bool TryValidateBucketRequestSubresources(HttpRequest request, out string? errorCode, out string? errorMessage, out int statusCode)
    {
        var queryKeys = request.Query.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
        switch (request.Method) {
            case "GET":
                foreach (var queryKey in queryKeys) {
                    if (!SupportedBucketGetQueryParameters.Contains(queryKey)) {
                        errorCode = "NotImplemented";
                        errorMessage = $"Bucket subresource '{queryKey}' is not implemented.";
                        statusCode = StatusCodes.Status501NotImplemented;
                        return false;
                    }
                }

                if (request.Query.TryGetValue(ListTypeQueryParameterName, out var listTypeValue)
                    && !string.IsNullOrWhiteSpace(listTypeValue.ToString())
                    && !string.Equals(listTypeValue.ToString(), "2", StringComparison.Ordinal)) {
                    errorCode = "InvalidArgument";
                    errorMessage = "Only list-type=2 is supported for S3-compatible bucket listing.";
                    statusCode = StatusCodes.Status400BadRequest;
                    return false;
                }

                break;

            case "POST":
                foreach (var queryKey in queryKeys) {
                    if (!SupportedBucketPostQueryParameters.Contains(queryKey)) {
                        errorCode = "NotImplemented";
                        errorMessage = $"Bucket subresource '{queryKey}' is not implemented.";
                        statusCode = StatusCodes.Status501NotImplemented;
                        return false;
                    }
                }

                if (!queryKeys.Contains("delete")) {
                    errorCode = "InvalidRequest";
                    errorMessage = "Only POST ?delete is supported for bucket-compatible subresource operations.";
                    statusCode = StatusCodes.Status400BadRequest;
                    return false;
                }

                break;

            case "PUT":
            case "HEAD":
            case "DELETE":
                if (queryKeys.Count > 0) {
                    errorCode = "NotImplemented";
                    errorMessage = $"Bucket subresource '{queryKeys.OrderBy(static key => key, StringComparer.OrdinalIgnoreCase).First()}' is not implemented.";
                    statusCode = StatusCodes.Status501NotImplemented;
                    return false;
                }

                break;
        }

        errorCode = null;
        errorMessage = null;
        statusCode = StatusCodes.Status200OK;
        return true;
    }

    private static bool TryValidateObjectRequestSubresources(HttpRequest request, out string? errorCode, out string? errorMessage, out int statusCode)
    {
        var hasUploads = request.Query.ContainsKey(UploadsQueryParameterName);
        var hasUploadId = request.Query.ContainsKey(UploadIdQueryParameterName);
        var hasPartNumber = request.Query.ContainsKey(PartNumberQueryParameterName);
        if (!hasUploads && !hasUploadId && !hasPartNumber) {
            errorCode = null;
            errorMessage = null;
            statusCode = StatusCodes.Status200OK;
            return true;
        }

        var isValidMultipartRequest = request.Method switch
        {
            "POST" when hasUploads && !hasUploadId && !hasPartNumber => true,
            "PUT" when hasUploadId && hasPartNumber && !hasUploads => true,
            "POST" when hasUploadId && !hasUploads && !hasPartNumber => true,
            "DELETE" when hasUploadId && !hasUploads && !hasPartNumber => true,
            _ => false
        };

        if (isValidMultipartRequest) {
            errorCode = null;
            errorMessage = null;
            statusCode = StatusCodes.Status200OK;
            return true;
        }

        errorCode = "NotImplemented";
        errorMessage = "The requested object subresource combination is not implemented.";
        statusCode = StatusCodes.Status501NotImplemented;
        return false;
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
            throw new FormatException("Only single byte range requests are supported.");
        }

        var value = trimmed[6..].Trim();
        if (value.Contains(',')) {
            throw new FormatException("Multiple byte ranges are not supported.");
        }

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

    private static async Task<PreparedRequestBody> PrepareRequestBodyAsync(HttpRequest request, CancellationToken cancellationToken)
    {
        if (!IsAwsChunkedContent(request)) {
            return new PreparedRequestBody(request.Body, request.ContentLength, tempFilePath: null);
        }

        var tempFilePath = Path.Combine(Path.GetTempPath(), $"integrateds3-aws-chunked-{Guid.NewGuid():N}.tmp");
        try {
            await using (var tempWriteStream = new FileStream(tempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                await CopyAwsChunkedContentToAsync(request.Body, tempWriteStream, cancellationToken);
                await tempWriteStream.FlushAsync(cancellationToken);
            }

            var decodedLength = TryParseDecodedContentLength(request.Headers["x-amz-decoded-content-length"].ToString());
            var tempReadStream = new FileStream(tempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
            var contentLength = decodedLength ?? tempReadStream.Length;
            return new PreparedRequestBody(tempReadStream, contentLength, tempFilePath);
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

    private static async Task CopyAwsChunkedContentToAsync(Stream source, Stream destination, CancellationToken cancellationToken)
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
                await ConsumeChunkTrailersAsync(source, cancellationToken);
                return;
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

    private static async Task ConsumeChunkTrailersAsync(Stream source, CancellationToken cancellationToken)
    {
        while (true) {
            var trailerLine = await ReadLineAsync(source, cancellationToken)
                ?? throw new FormatException("The aws-chunked request body ended before the terminating trailer section.");
            if (trailerLine.Length == 0) {
                return;
            }
        }
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
        var separatorIndex = trimmed.IndexOf('/');
        if (separatorIndex <= 0 || separatorIndex == trimmed.Length - 1) {
            throw new FormatException("The copy source header must be in the form '/bucket/key'.");
        }

        return new CopySourceReference(trimmed[..separatorIndex], trimmed[(separatorIndex + 1)..]);
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

    private static void ApplyObjectHeaders(HttpResponse httpResponse, ObjectInfo objectInfo)
    {
        httpResponse.Headers.LastModified = objectInfo.LastModifiedUtc.ToString("R");
        if (!string.IsNullOrWhiteSpace(objectInfo.ETag)) {
            httpResponse.Headers.ETag = QuoteETag(objectInfo.ETag);
        }

        IEnumerable<KeyValuePair<string, string>> metadataPairs = objectInfo.Metadata ?? Enumerable.Empty<KeyValuePair<string, string>>();
        foreach (var metadataPair in metadataPairs) {
            httpResponse.Headers[$"{MetadataHeaderPrefix}{metadataPair.Key}"] = metadataPair.Value;
        }
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

    private sealed class StreamObjectResult(GetObjectResponse objectResponse) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);

            await using var response = objectResponse;

            ApplyObjectHeaders(httpContext.Response, response.Object);
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

    private sealed class PreparedRequestBody(Stream content, long? contentLength, string? tempFilePath) : IAsyncDisposable
    {
        public Stream Content { get; } = content;

        public long? ContentLength { get; } = contentLength;

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

    private sealed record CopySourceReference(string BucketName, string Key);

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
}

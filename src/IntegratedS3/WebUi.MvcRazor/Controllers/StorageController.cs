using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using WebUi.MvcRazor.Models;

namespace WebUi.MvcRazor.Controllers;

public sealed class StorageController(IStorageService storageService, IOptions<IntegratedS3Options> options) : Controller
{
    private readonly IStorageService _storageService = storageService ?? throw new ArgumentNullException(nameof(storageService));
    private readonly IntegratedS3Options _options = (options ?? throw new ArgumentNullException(nameof(options))).Value;

    [HttpGet("/")]
    [HttpGet("/storage")]
    public async Task<IActionResult> Index(CancellationToken cancellationToken)
    {
        var buckets = await GetBucketsAsync(cancellationToken);
        var bucketModels = new List<StorageBucketViewModel>(buckets.Count);
        foreach (var bucket in buckets) {
            var objects = await GetObjectsAsync(bucket.Name, cancellationToken);
            bucketModels.Add(new StorageBucketViewModel
            {
                Name = bucket.Name,
                CreatedAtUtc = bucket.CreatedAtUtc,
                Objects = [.. objects.Select(static objectInfo => new StorageObjectViewModel
                {
                    Key = objectInfo.Key,
                    ContentLength = objectInfo.ContentLength,
                    ContentType = objectInfo.ContentType,
                    LastModifiedUtc = objectInfo.LastModifiedUtc
                })]
            });
        }

        var model = new StorageDashboardViewModel
        {
            ServiceName = _options.ServiceName,
            RoutePrefix = _options.RoutePrefix,
            Buckets = bucketModels
        };

        return View(model);
    }

    private async Task<List<BucketInfo>> GetBucketsAsync(CancellationToken cancellationToken)
    {
        var buckets = new List<BucketInfo>();
        await foreach (var bucket in _storageService.ListBucketsAsync(cancellationToken)) {
            buckets.Add(bucket);
        }

        buckets.Sort(static (left, right) => StringComparer.Ordinal.Compare(left.Name, right.Name));
        return buckets;
    }

    private async Task<List<ObjectInfo>> GetObjectsAsync(string bucketName, CancellationToken cancellationToken)
    {
        var objects = new List<ObjectInfo>();
        await foreach (var objectInfo in _storageService.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = bucketName
        }, cancellationToken)) {
            objects.Add(objectInfo);
        }

        objects.Sort(static (left, right) => StringComparer.Ordinal.Compare(left.Key, right.Key));
        return objects;
    }
}

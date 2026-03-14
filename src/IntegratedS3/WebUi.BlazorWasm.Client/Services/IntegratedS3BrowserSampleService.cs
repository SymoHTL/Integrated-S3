using System.Net;
using System.Net.Http.Json;
using System.Text;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Client;
using WebUi.BlazorWasm.Client.Models;
using WebUi.BlazorWasm.Client.Serialization;

namespace WebUi.BlazorWasm.Client.Services;

public sealed class IntegratedS3BrowserSampleService(HttpClient httpClient, IIntegratedS3Client integratedS3Client)
{
    private const string RoutePrefix = "integrated-s3";
    private readonly HttpClient _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    private readonly IIntegratedS3Client _integratedS3Client = integratedS3Client ?? throw new ArgumentNullException(nameof(integratedS3Client));

    public async Task<BrowserSampleDashboardModel> GetDashboardAsync(CancellationToken cancellationToken = default)
    {
        var serviceDocument = await _httpClient.GetFromJsonAsync(
            RoutePrefix,
            WebUiBlazorWasmClientJsonSerializerContext.Default.BrowserSampleServiceDocument,
            cancellationToken)
            ?? throw new InvalidOperationException("The IntegratedS3 service document response was empty.");

        var buckets = await _httpClient.GetFromJsonAsync(
            $"{RoutePrefix}/buckets",
            WebUiBlazorWasmClientJsonSerializerContext.Default.BucketInfoArray,
            cancellationToken)
            ?? throw new InvalidOperationException("The IntegratedS3 bucket listing response was empty.");

        Array.Sort(buckets, static (left, right) => StringComparer.Ordinal.Compare(left.Name, right.Name));
        var bucketModels = new List<BrowserSampleBucketModel>(buckets.Length);
        foreach (var bucket in buckets) {
            var objects = await GetObjectsAsync(bucket.Name, cancellationToken);
            bucketModels.Add(new BrowserSampleBucketModel
            {
                Name = bucket.Name,
                CreatedAtUtc = bucket.CreatedAtUtc,
                Objects = [.. objects.Select(static objectInfo => new BrowserSampleObjectModel
                {
                    Key = objectInfo.Key,
                    ContentLength = objectInfo.ContentLength,
                    ContentType = objectInfo.ContentType,
                    LastModifiedUtc = objectInfo.LastModifiedUtc
                })]
            });
        }

        return new BrowserSampleDashboardModel
        {
            ServiceName = serviceDocument.ServiceName,
            ProviderNames = serviceDocument.Providers.Select(static provider => provider.Name).ToArray(),
            Buckets = bucketModels
        };
    }

    public async Task<BrowserSampleTransferResult> UploadTextAsync(string bucketName, string key, string content, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(content);

        await EnsureBucketAsync(bucketName, cancellationToken);

        var contentBytes = Encoding.UTF8.GetBytes(content);
        await using var stream = new MemoryStream(contentBytes, writable: false);
        var presignedRequest = await _integratedS3Client.PresignPutObjectAsync(
            bucketName,
            key,
            expiresInSeconds: 300,
            contentType: "text/plain; charset=utf-8",
            cancellationToken: cancellationToken);

        using var request = presignedRequest.CreateHttpRequestMessage(new StreamContent(stream));
        using var response = await _httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();

        return new BrowserSampleTransferResult(bucketName, key, presignedRequest.AccessMode, presignedRequest.Url);
    }

    public async Task<BrowserSampleDownloadResult> DownloadTextAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        var presignedRequest = await _integratedS3Client.PresignGetObjectAsync(
            bucketName,
            key,
            expiresInSeconds: 300,
            cancellationToken: cancellationToken);

        using var request = presignedRequest.CreateHttpRequestMessage();
        using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
        return new BrowserSampleDownloadResult(bucketName, key, responseBody, presignedRequest.AccessMode, presignedRequest.Url);
    }

    private async Task EnsureBucketAsync(string bucketName, CancellationToken cancellationToken)
    {
        using var response = await _httpClient.PutAsync($"{RoutePrefix}/buckets/{Uri.EscapeDataString(bucketName)}", content: null, cancellationToken);
        if (response.StatusCode == HttpStatusCode.Conflict) {
            return;
        }

        response.EnsureSuccessStatusCode();
    }

    private async Task<ObjectInfo[]> GetObjectsAsync(string bucketName, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        var objects = await _httpClient.GetFromJsonAsync(
            $"{RoutePrefix}/buckets/{Uri.EscapeDataString(bucketName)}/objects",
            WebUiBlazorWasmClientJsonSerializerContext.Default.ObjectInfoArray,
            cancellationToken)
            ?? throw new InvalidOperationException($"The IntegratedS3 object listing response for bucket '{bucketName}' was empty.");

        Array.Sort(objects, static (left, right) => StringComparer.Ordinal.Compare(left.Key, right.Key));
        return objects;
    }
}

using System.Text;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;

namespace WebUi.BlazorWasm;

public static class BlazorWasmSampleDefaults
{
    public const string BucketName = "browser-sample";
    public const string ObjectKey = "welcome.txt";
    public const string ContentType = "text/plain; charset=utf-8";
    public static readonly string WelcomeText = "This object was seeded by the hosted Blazor WebAssembly sample so the browser client has a known object to read.";
}

internal sealed class BlazorWasmSampleDataSeeder(IStorageService storageService) : IHostedService
{
    private readonly IStorageService _storageService = storageService ?? throw new ArgumentNullException(nameof(storageService));

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var createBucketResult = await _storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = BlazorWasmSampleDefaults.BucketName
        }, cancellationToken);

        if (!createBucketResult.IsSuccess && createBucketResult.Error?.Code != StorageErrorCode.BucketAlreadyExists) {
            throw new InvalidOperationException($"Unable to seed bucket '{BlazorWasmSampleDefaults.BucketName}': {createBucketResult.Error?.Message}");
        }

        var contentBytes = Encoding.UTF8.GetBytes(BlazorWasmSampleDefaults.WelcomeText);
        await using var content = new MemoryStream(contentBytes, writable: false);
        var putObjectResult = await _storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = BlazorWasmSampleDefaults.BucketName,
            Key = BlazorWasmSampleDefaults.ObjectKey,
            Content = content,
            ContentLength = contentBytes.Length,
            ContentType = BlazorWasmSampleDefaults.ContentType
        }, cancellationToken);

        if (!putObjectResult.IsSuccess) {
            throw new InvalidOperationException($"Unable to seed object '{BlazorWasmSampleDefaults.ObjectKey}': {putObjectResult.Error?.Message}");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

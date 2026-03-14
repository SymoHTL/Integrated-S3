using System.Text;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;

namespace WebUi.MvcRazor;

public static class MvcRazorSampleDefaults
{
    public const string BucketName = "mvc-sample";
    public const string ObjectKey = "welcome.txt";
    public const string ContentType = "text/plain; charset=utf-8";
    public static readonly string WelcomeText = "This object was seeded by the MVC/Razor sample so the dashboard is useful on first run.";
}

internal sealed class MvcRazorSampleDataSeeder(IStorageService storageService) : IHostedService
{
    private readonly IStorageService _storageService = storageService ?? throw new ArgumentNullException(nameof(storageService));

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var createBucketResult = await _storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = MvcRazorSampleDefaults.BucketName
        }, cancellationToken);

        if (!createBucketResult.IsSuccess && createBucketResult.Error?.Code != StorageErrorCode.BucketAlreadyExists) {
            throw new InvalidOperationException($"Unable to seed bucket '{MvcRazorSampleDefaults.BucketName}': {createBucketResult.Error?.Message}");
        }

        var contentBytes = Encoding.UTF8.GetBytes(MvcRazorSampleDefaults.WelcomeText);
        await using var content = new MemoryStream(contentBytes, writable: false);
        var putObjectResult = await _storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MvcRazorSampleDefaults.BucketName,
            Key = MvcRazorSampleDefaults.ObjectKey,
            Content = content,
            ContentLength = contentBytes.Length,
            ContentType = MvcRazorSampleDefaults.ContentType
        }, cancellationToken);

        if (!putObjectResult.IsSuccess) {
            throw new InvalidOperationException($"Unable to seed object '{MvcRazorSampleDefaults.ObjectKey}': {putObjectResult.Error?.Message}");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

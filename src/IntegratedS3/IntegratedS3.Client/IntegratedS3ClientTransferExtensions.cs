using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

/// <summary>
/// Typed upload and download helpers for <see cref="IIntegratedS3Client"/> that compose presign issuance
/// with HTTP transfer execution, making common file and stream transfer scenarios easy to implement.
/// </summary>
/// <remarks>
/// Each method obtains a presigned URL via <paramref name="client"/> and then uses
/// <paramref name="transferClient"/> for the actual data transfer, keeping the two concerns
/// (authorization/presign issuance vs. data movement) on separate <see cref="HttpClient"/> instances.
/// This allows callers to apply different auth, timeout, or handler policies to each leg of the request.
/// </remarks>
public static class IntegratedS3ClientTransferExtensions
{
    /// <summary>
    /// Obtains a presigned PUT URL and uploads <paramref name="content"/> to storage.
    /// The stream is forwarded without buffering the full payload into memory.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to obtain the presigned URL.</param>
    /// <param name="transferClient">The <see cref="HttpClient"/> used to execute the upload transfer.</param>
    /// <param name="bucketName">The target bucket name.</param>
    /// <param name="key">The target object key.</param>
    /// <param name="content">The stream to upload.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="contentType">Optional MIME type for the object. When supplied it is enforced as a signed header.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task UploadStreamAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        Stream content,
        int expiresInSeconds,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentNullException.ThrowIfNull(content);

        var presigned = await client.PresignPutObjectAsync(
            bucketName, key, expiresInSeconds, contentType, cancellationToken);

        using var httpContent = new StreamContent(content);
        using var request = presigned.CreateHttpRequestMessage(httpContent);
        using var response = await transferClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Obtains a presigned PUT URL and uploads <paramref name="content"/> to storage,
    /// requesting the specified <paramref name="preferredAccessMode"/> from the server.
    /// The stream is forwarded without buffering the full payload into memory.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to obtain the presigned URL.</param>
    /// <param name="transferClient">The <see cref="HttpClient"/> used to execute the upload transfer.</param>
    /// <param name="bucketName">The target bucket name.</param>
    /// <param name="key">The target object key.</param>
    /// <param name="content">The stream to upload.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="preferredAccessMode">
    /// The preferred access mode hint forwarded to the server with the presign request.
    /// Note that current server behavior only honors access-mode preferences for read (GET) operations;
    /// write (PUT) presign requests typically fall back to <see cref="StorageAccessMode.Proxy"/> regardless.
    /// </param>
    /// <param name="contentType">Optional MIME type for the object. When supplied it is enforced as a signed header.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task UploadStreamAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        Stream content,
        int expiresInSeconds,
        StorageAccessMode preferredAccessMode,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentNullException.ThrowIfNull(content);

        var presigned = await client.PresignPutObjectAsync(
            bucketName, key, expiresInSeconds, preferredAccessMode, contentType, cancellationToken);

        using var httpContent = new StreamContent(content);
        using var request = presigned.CreateHttpRequestMessage(httpContent);
        using var response = await transferClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Opens <paramref name="filePath"/> and uploads it to storage via a presigned PUT URL.
    /// The file is read as a stream without loading the full contents into memory.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to obtain the presigned URL.</param>
    /// <param name="transferClient">The <see cref="HttpClient"/> used to execute the upload transfer.</param>
    /// <param name="bucketName">The target bucket name.</param>
    /// <param name="key">The target object key.</param>
    /// <param name="filePath">The local file path to upload.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="contentType">Optional MIME type for the object. When supplied it is enforced as a signed header.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task UploadFileAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        string filePath,
        int expiresInSeconds,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await using var fileStream = new FileStream(
            filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 65536, useAsync: true);

        await UploadStreamAsync(
            client, transferClient, bucketName, key,
            fileStream, expiresInSeconds, contentType, cancellationToken);
    }

    /// <summary>
    /// Opens <paramref name="filePath"/> and uploads it to storage via a presigned PUT URL,
    /// requesting the specified <paramref name="preferredAccessMode"/> from the server.
    /// The file is read as a stream without loading the full contents into memory.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to obtain the presigned URL.</param>
    /// <param name="transferClient">The <see cref="HttpClient"/> used to execute the upload transfer.</param>
    /// <param name="bucketName">The target bucket name.</param>
    /// <param name="key">The target object key.</param>
    /// <param name="filePath">The local file path to upload.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="preferredAccessMode">
    /// The preferred access mode hint forwarded to the server with the presign request.
    /// Note that current server behavior only honors access-mode preferences for read (GET) operations;
    /// write (PUT) presign requests typically fall back to <see cref="StorageAccessMode.Proxy"/> regardless.
    /// </param>
    /// <param name="contentType">Optional MIME type for the object. When supplied it is enforced as a signed header.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task UploadFileAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        string filePath,
        int expiresInSeconds,
        StorageAccessMode preferredAccessMode,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await using var fileStream = new FileStream(
            filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 65536, useAsync: true);

        await UploadStreamAsync(
            client, transferClient, bucketName, key,
            fileStream, expiresInSeconds, preferredAccessMode, contentType, cancellationToken);
    }

    /// <summary>
    /// Obtains a presigned GET URL and downloads the object into <paramref name="destination"/>.
    /// The response body is streamed directly without buffering the full payload into memory.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to obtain the presigned URL.</param>
    /// <param name="transferClient">The <see cref="HttpClient"/> used to execute the download transfer.</param>
    /// <param name="bucketName">The source bucket name.</param>
    /// <param name="key">The source object key.</param>
    /// <param name="destination">The stream to write the downloaded object body into.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="versionId">Optional version identifier for the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task DownloadToStreamAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        Stream destination,
        int expiresInSeconds,
        string? versionId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentNullException.ThrowIfNull(destination);

        var presigned = await client.PresignGetObjectAsync(
            bucketName, key, expiresInSeconds, versionId, cancellationToken);

        using var request = presigned.CreateHttpRequestMessage();
        using var response = await transferClient.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        await response.Content.CopyToAsync(destination, cancellationToken);
    }

    /// <summary>
    /// Obtains a presigned GET URL and downloads the object into <paramref name="destination"/>,
    /// requesting the specified <paramref name="preferredAccessMode"/> from the server.
    /// The response body is streamed directly without buffering the full payload into memory.
    /// </summary>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to obtain the presigned URL.</param>
    /// <param name="transferClient">The <see cref="HttpClient"/> used to execute the download transfer.</param>
    /// <param name="bucketName">The source bucket name.</param>
    /// <param name="key">The source object key.</param>
    /// <param name="destination">The stream to write the downloaded object body into.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="preferredAccessMode">
    /// The preferred access mode hint forwarded to the server with the presign request.
    /// Use <see cref="StorageAccessMode.Direct"/> to request a public URL redirect,
    /// <see cref="StorageAccessMode.Delegated"/> to request a provider-signed URL,
    /// or <see cref="StorageAccessMode.Proxy"/> to force proxy streaming through the server.
    /// The server may fall back to <see cref="StorageAccessMode.Proxy"/> if the requested mode is unavailable.
    /// </param>
    /// <param name="versionId">Optional version identifier for the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task DownloadToStreamAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        Stream destination,
        int expiresInSeconds,
        StorageAccessMode preferredAccessMode,
        string? versionId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentNullException.ThrowIfNull(destination);

        var presigned = await client.PresignGetObjectAsync(
            bucketName, key, expiresInSeconds, preferredAccessMode, versionId, cancellationToken);

        using var request = presigned.CreateHttpRequestMessage();
        using var response = await transferClient.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        await response.Content.CopyToAsync(destination, cancellationToken);
    }

    /// <summary>
    /// Obtains a presigned GET URL and downloads the object to <paramref name="filePath"/>,
    /// creating or overwriting the file. The response body is streamed without buffering
    /// the full payload into memory.
    /// </summary>
    /// <remarks>
    /// If the presign request or transfer fails the destination file is deleted so that
    /// callers never see an empty or partial file left behind by a failed download.
    /// </remarks>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to obtain the presigned URL.</param>
    /// <param name="transferClient">The <see cref="HttpClient"/> used to execute the download transfer.</param>
    /// <param name="bucketName">The source bucket name.</param>
    /// <param name="key">The source object key.</param>
    /// <param name="filePath">The local file path to write the downloaded object to.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="versionId">Optional version identifier for the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task DownloadToFileAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        string filePath,
        int expiresInSeconds,
        string? versionId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        // Obtain the presigned URL before creating the file so that a presign failure
        // does not leave an empty file at the destination.
        var presigned = await client.PresignGetObjectAsync(
            bucketName, key, expiresInSeconds, versionId, cancellationToken);

        var fileCreated = false;
        var transferCompleted = false;
        try
        {
            await using var fileStream = new FileStream(
                filePath, FileMode.Create, FileAccess.Write, FileShare.None,
                bufferSize: 65536, useAsync: true);
            fileCreated = true;

            using var request = presigned.CreateHttpRequestMessage();
            using var response = await transferClient.SendAsync(
                request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();
            await response.Content.CopyToAsync(fileStream, cancellationToken);
            transferCompleted = true;
        }
        finally
        {
            if (fileCreated && !transferCompleted)
            {
                // Best-effort cleanup: remove the partial/empty destination file so
                // failures do not leave misleading files behind.
                try { File.Delete(filePath); } catch { }
            }
        }
    }

    /// <summary>
    /// Obtains a presigned GET URL and downloads the object to <paramref name="filePath"/>,
    /// requesting the specified <paramref name="preferredAccessMode"/> from the server.
    /// Creates or overwrites the file. The response body is streamed without buffering
    /// the full payload into memory.
    /// </summary>
    /// <remarks>
    /// If the presign request or transfer fails the destination file is deleted so that
    /// callers never see an empty or partial file left behind by a failed download.
    /// </remarks>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to obtain the presigned URL.</param>
    /// <param name="transferClient">The <see cref="HttpClient"/> used to execute the download transfer.</param>
    /// <param name="bucketName">The source bucket name.</param>
    /// <param name="key">The source object key.</param>
    /// <param name="filePath">The local file path to write the downloaded object to.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="preferredAccessMode">
    /// The preferred access mode hint forwarded to the server with the presign request.
    /// Use <see cref="StorageAccessMode.Direct"/> to request a public URL redirect,
    /// <see cref="StorageAccessMode.Delegated"/> to request a provider-signed URL,
    /// or <see cref="StorageAccessMode.Proxy"/> to force proxy streaming through the server.
    /// The server may fall back to <see cref="StorageAccessMode.Proxy"/> if the requested mode is unavailable.
    /// </param>
    /// <param name="versionId">Optional version identifier for the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task DownloadToFileAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        string filePath,
        int expiresInSeconds,
        StorageAccessMode preferredAccessMode,
        string? versionId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        // Obtain the presigned URL before creating the file so that a presign failure
        // does not leave an empty file at the destination.
        var presigned = await client.PresignGetObjectAsync(
            bucketName, key, expiresInSeconds, preferredAccessMode, versionId, cancellationToken);

        var fileCreated = false;
        var transferCompleted = false;
        try
        {
            await using var fileStream = new FileStream(
                filePath, FileMode.Create, FileAccess.Write, FileShare.None,
                bufferSize: 65536, useAsync: true);
            fileCreated = true;

            using var request = presigned.CreateHttpRequestMessage();
            using var response = await transferClient.SendAsync(
                request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();
            await response.Content.CopyToAsync(fileStream, cancellationToken);
            transferCompleted = true;
        }
        finally
        {
            if (fileCreated && !transferCompleted)
            {
                // Best-effort cleanup: remove the partial/empty destination file so
                // failures do not leave misleading files behind.
                try { File.Delete(filePath); } catch { }
            }
        }
    }
}

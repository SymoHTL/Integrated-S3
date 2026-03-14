using System.Net;
using System.Net.Http.Headers;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

/// <summary>
/// Typed upload and download helpers for <see cref="IIntegratedS3Client"/> that compose presign issuance
/// with HTTP transfer execution, making common file and stream transfer scenarios easy to implement.
/// </summary>
/// <remarks>
/// Each method obtains a presigned URL via the calling <see cref="IIntegratedS3Client"/> and then uses
/// the supplied transfer <see cref="HttpClient"/> for the actual data transfer, keeping the two concerns
/// (authorization/presign issuance vs. data movement) on separate <see cref="HttpClient"/> instances.
/// This allows callers to apply different auth, timeout, or handler policies to each leg of the request.
/// Overloads without a <c>preferredAccessMode</c> parameter intentionally keep access-mode selection
/// explicit at the caller boundary. They do not infer <see cref="StorageAccessMode.Direct" /> or
/// <see cref="StorageAccessMode.Delegated" /> from service/provider discovery and therefore preserve
/// proxy-mode as the stable default until the caller opts into another mode explicitly.
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

        await UploadPresignedAsync(transferClient, presigned, content, checksum: null, cancellationToken);
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

        await UploadPresignedAsync(transferClient, presigned, content, checksum: null, cancellationToken);
    }

    /// <summary>
    /// Obtains a checksum-aware presigned PUT URL and uploads <paramref name="content"/> to storage.
    /// The stream is forwarded without buffering the full payload into memory, but checksum-aware uploads
    /// require a seekable source so the checksum can be computed before the upload begins.
    /// </summary>
    public static async Task UploadStreamAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        Stream content,
        int expiresInSeconds,
        IntegratedS3TransferChecksumAlgorithm checksumAlgorithm,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentNullException.ThrowIfNull(content);

        var checksum = await IntegratedS3ClientTransferChecksumHelper.PrepareUploadChecksumAsync(
            content,
            checksumAlgorithm,
            cancellationToken);

        var presigned = await client.PresignPutObjectAsync(
            bucketName,
            key,
            expiresInSeconds,
            checksumAlgorithm,
            checksum.ChecksumValue,
            contentType,
            cancellationToken);

        await UploadPresignedAsync(transferClient, presigned, content, checksum, cancellationToken);
    }

    /// <summary>
    /// Obtains a checksum-aware presigned PUT URL and uploads <paramref name="content"/> to storage,
    /// forwarding <paramref name="preferredAccessMode"/> to the presign request.
    /// The stream is forwarded without buffering the full payload into memory, but checksum-aware uploads
    /// require a seekable source so the checksum can be computed before the upload begins.
    /// </summary>
    public static async Task UploadStreamAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        Stream content,
        int expiresInSeconds,
        StorageAccessMode preferredAccessMode,
        IntegratedS3TransferChecksumAlgorithm checksumAlgorithm,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentNullException.ThrowIfNull(content);

        var checksum = await IntegratedS3ClientTransferChecksumHelper.PrepareUploadChecksumAsync(
            content,
            checksumAlgorithm,
            cancellationToken);

        var presigned = await client.PresignPutObjectAsync(
            bucketName,
            key,
            expiresInSeconds,
            preferredAccessMode,
            checksumAlgorithm,
            checksum.ChecksumValue,
            contentType,
            cancellationToken);

        await UploadPresignedAsync(transferClient, presigned, content, checksum, cancellationToken);
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
            bufferSize: 65536, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

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
            bufferSize: 65536, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

        await UploadStreamAsync(
            client, transferClient, bucketName, key,
            fileStream, expiresInSeconds, preferredAccessMode, contentType, cancellationToken);
    }

    /// <summary>
    /// Opens <paramref name="filePath"/>, computes the requested checksum in a streaming pre-pass,
    /// and uploads the file through a checksum-aware presigned PUT URL.
    /// </summary>
    public static async Task UploadFileAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        string filePath,
        int expiresInSeconds,
        IntegratedS3TransferChecksumAlgorithm checksumAlgorithm,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await using var fileStream = new FileStream(
            filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 65536, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

        await UploadStreamAsync(
            client, transferClient, bucketName, key,
            fileStream, expiresInSeconds, checksumAlgorithm, contentType, cancellationToken);
    }

    /// <summary>
    /// Opens <paramref name="filePath"/>, computes the requested checksum in a streaming pre-pass,
    /// and uploads the file through a checksum-aware presigned PUT URL while forwarding
    /// <paramref name="preferredAccessMode"/> to the presign request.
    /// </summary>
    public static async Task UploadFileAsync(
        this IIntegratedS3Client client,
        HttpClient transferClient,
        string bucketName,
        string key,
        string filePath,
        int expiresInSeconds,
        StorageAccessMode preferredAccessMode,
        IntegratedS3TransferChecksumAlgorithm checksumAlgorithm,
        string? contentType = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transferClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await using var fileStream = new FileStream(
            filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 65536, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

        await UploadStreamAsync(
            client, transferClient, bucketName, key,
            fileStream, expiresInSeconds, preferredAccessMode, checksumAlgorithm, contentType, cancellationToken);
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

        var validation = IntegratedS3ClientTransferChecksumHelper.CreateDownloadValidation(response);
        await IntegratedS3ClientTransferChecksumHelper.CopyToAsync(
            response.Content,
            destination,
            validation,
            cancellationToken);
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

        var validation = IntegratedS3ClientTransferChecksumHelper.CreateDownloadValidation(response);
        await IntegratedS3ClientTransferChecksumHelper.CopyToAsync(
            response.Content,
            destination,
            validation,
            cancellationToken);
    }

    private static async Task UploadPresignedAsync(
        HttpClient transferClient,
        StoragePresignedRequest presigned,
        Stream content,
        IntegratedS3ClientTransferChecksumHelper.PreparedUploadChecksum? checksum,
        CancellationToken cancellationToken)
    {
        using var httpContent = new StreamContent(content);
        using var request = presigned.CreateHttpRequestMessage(httpContent);
        using var response = await transferClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();

        if (checksum is { } preparedChecksum) {
            IntegratedS3ClientTransferChecksumHelper.ValidateUploadResponseChecksum(response, preparedChecksum);
        }
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

        await DownloadPresignedToFileAsync(transferClient, presigned, filePath, cancellationToken);
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

        await DownloadPresignedToFileAsync(transferClient, presigned, filePath, cancellationToken);
    }

    /// <summary>
    /// Obtains a presigned GET URL and downloads the object to <paramref name="filePath"/>,
    /// resuming from an existing destination file when possible.
    /// The response body is streamed without buffering the full payload into memory.
    /// </summary>
    /// <remarks>
    /// If the destination file already exists, the helper requests the remaining bytes with an HTTP range request.
    /// When the server ignores that range and returns a full <c>200 OK</c> response instead, the file is rewritten
    /// from the start. When the server returns <c>416 Requested Range Not Satisfiable</c> and reports the same total
    /// length as the destination file, the download is treated as already complete.
    /// Pre-existing partial files are preserved on resume failures so callers can retry, while files created by the
    /// current call are removed on failure.
    /// </remarks>
    /// <param name="client">The <see cref="IIntegratedS3Client"/> used to obtain the presigned URL.</param>
    /// <param name="transferClient">The <see cref="HttpClient"/> used to execute the download transfer.</param>
    /// <param name="bucketName">The source bucket name.</param>
    /// <param name="key">The source object key.</param>
    /// <param name="filePath">The local file path to write the downloaded object to.</param>
    /// <param name="expiresInSeconds">How long the presigned URL should remain valid, in seconds.</param>
    /// <param name="versionId">Optional version identifier for the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task DownloadToFileWithResumeAsync(
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

        var presigned = await client.PresignGetObjectAsync(
            bucketName, key, expiresInSeconds, versionId, cancellationToken);

        await DownloadPresignedToFileWithResumeAsync(transferClient, presigned, filePath, cancellationToken);
    }

    /// <summary>
    /// Obtains a presigned GET URL and downloads the object to <paramref name="filePath"/>,
    /// resuming from an existing destination file when possible and forwarding
    /// <paramref name="preferredAccessMode"/> to the presign request.
    /// The response body is streamed without buffering the full payload into memory.
    /// </summary>
    /// <remarks>
    /// If the destination file already exists, the helper requests the remaining bytes with an HTTP range request.
    /// When the server ignores that range and returns a full <c>200 OK</c> response instead, the file is rewritten
    /// from the start. When the server returns <c>416 Requested Range Not Satisfiable</c> and reports the same total
    /// length as the destination file, the download is treated as already complete.
    /// Pre-existing partial files are preserved on resume failures so callers can retry, while files created by the
    /// current call are removed on failure.
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
    public static async Task DownloadToFileWithResumeAsync(
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

        var presigned = await client.PresignGetObjectAsync(
            bucketName, key, expiresInSeconds, preferredAccessMode, versionId, cancellationToken);

        await DownloadPresignedToFileWithResumeAsync(transferClient, presigned, filePath, cancellationToken);
    }

    private static async Task DownloadPresignedToFileAsync(
        HttpClient transferClient,
        StoragePresignedRequest presigned,
        string filePath,
        CancellationToken cancellationToken)
    {
        try {
            await using var fileStream = new FileStream(
                filePath, FileMode.Create, FileAccess.Write, FileShare.None,
                bufferSize: 65536, useAsync: true);

            using var request = presigned.CreateHttpRequestMessage();
            using var response = await transferClient.SendAsync(
                request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();
            var validation = IntegratedS3ClientTransferChecksumHelper.CreateDownloadValidation(response);
            await IntegratedS3ClientTransferChecksumHelper.CopyToAsync(
                response.Content,
                fileStream,
                validation,
                cancellationToken);
        }
        catch (HttpRequestException exception) {
            DeletePartialDownload(filePath, exception);
            throw;
        }
        catch (InvalidDataException exception) {
            DeletePartialDownload(filePath, exception);
            throw;
        }
        catch (IOException exception) {
            DeletePartialDownload(filePath, exception);
            throw CreateTransferHttpRequestException("The download failed while writing the response body.", exception);
        }
        catch (OperationCanceledException exception) {
            DeletePartialDownload(filePath, exception);
            throw;
        }
    }

    private static async Task DownloadPresignedToFileWithResumeAsync(
        HttpClient transferClient,
        StoragePresignedRequest presigned,
        string filePath,
        CancellationToken cancellationToken)
    {
        if (!File.Exists(filePath)) {
            await DownloadPresignedToFileAsync(transferClient, presigned, filePath, cancellationToken);
            return;
        }

        var existingLength = new FileInfo(filePath).Length;

        using var request = presigned.CreateHttpRequestMessage();
        request.Headers.Range = new RangeHeaderValue(existingLength, null);

        using var response = await transferClient.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

        if (response.StatusCode == HttpStatusCode.PartialContent) {
            await AppendPartialDownloadAsync(response, filePath, cancellationToken);
            return;
        }

        if (response.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable) {
            if (TryGetReportedObjectLength(response, out var totalLength) && totalLength == existingLength) {
                return;
            }

            await RewriteDownloadFromStartAsync(
                transferClient, presigned, filePath, responseToReuse: null, cancellationToken);
            return;
        }

        if (response.IsSuccessStatusCode) {
            await RewriteDownloadFromStartAsync(
                transferClient, presigned, filePath, response, cancellationToken);
            return;
        }

        response.EnsureSuccessStatusCode();
    }

    private static async Task AppendPartialDownloadAsync(
        HttpResponseMessage response,
        string filePath,
        CancellationToken cancellationToken)
    {
        var validation = IntegratedS3ClientTransferChecksumHelper.CreateDownloadValidation(response);
        if (validation is not null) {
            await IntegratedS3ClientTransferChecksumHelper.SeedExistingBytesAsync(
                validation,
                filePath,
                cancellationToken);
        }

        await using var fileStream = new FileStream(
            filePath, FileMode.Append, FileAccess.Write, FileShare.None,
            bufferSize: 65536, useAsync: true);

        try {
            await IntegratedS3ClientTransferChecksumHelper.CopyToAsync(
                response.Content,
                fileStream,
                validation,
                cancellationToken);
        }
        catch (Exception exception) {
            if (exception is IOException ioException) {
                throw CreateTransferHttpRequestException("The resumed download failed while appending to the destination file.", ioException);
            }

            throw;
        }
    }

    private static async Task RewriteDownloadFromStartAsync(
        HttpClient transferClient,
        StoragePresignedRequest presigned,
        string filePath,
        HttpResponseMessage? responseToReuse,
        CancellationToken cancellationToken)
    {
        var temporaryFilePath = CreateTemporaryDownloadPath(filePath);

        try {
            if (responseToReuse is null) {
                using var request = presigned.CreateHttpRequestMessage();
                using var response = await transferClient.SendAsync(
                    request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                response.EnsureSuccessStatusCode();
                await WriteResponseToNewFileAsync(response, temporaryFilePath, cancellationToken);
            }
            else {
                await WriteResponseToNewFileAsync(responseToReuse, temporaryFilePath, cancellationToken);
            }

            File.Move(temporaryFilePath, filePath, overwrite: true);
        }
        catch (HttpRequestException exception) {
            DeletePartialDownload(temporaryFilePath, exception);
            throw;
        }
        catch (InvalidDataException exception) {
            DeletePartialDownload(temporaryFilePath, exception);
            throw;
        }
        catch (IOException exception) {
            DeletePartialDownload(temporaryFilePath, exception);
            throw CreateTransferHttpRequestException("The download failed while rewriting the destination file.", exception);
        }
        catch (OperationCanceledException exception) {
            DeletePartialDownload(temporaryFilePath, exception);
            throw;
        }
        catch (UnauthorizedAccessException exception) {
            DeletePartialDownload(temporaryFilePath, exception);
            throw;
        }
    }

    private static async Task WriteResponseToNewFileAsync(
        HttpResponseMessage response,
        string filePath,
        CancellationToken cancellationToken)
    {
        await using var fileStream = new FileStream(
            filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None,
            bufferSize: 65536, useAsync: true);

        var validation = IntegratedS3ClientTransferChecksumHelper.CreateDownloadValidation(response);
        await IntegratedS3ClientTransferChecksumHelper.CopyToAsync(
            response.Content,
            fileStream,
            validation,
            cancellationToken);
    }

    private static string CreateTemporaryDownloadPath(string destinationFilePath)
    {
        var destinationDirectory = Path.GetDirectoryName(Path.GetFullPath(destinationFilePath))
            ?? Directory.GetCurrentDirectory();

        return Path.Combine(destinationDirectory, Path.GetRandomFileName());
    }

    private static bool TryGetReportedObjectLength(HttpResponseMessage response, out long totalLength)
    {
        if (response.Content.Headers.ContentRange?.Length is long reportedLength) {
            totalLength = reportedLength;
            return true;
        }

        totalLength = 0;
        return false;
    }

    private static HttpRequestException CreateTransferHttpRequestException(string message, IOException innerException)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        ArgumentNullException.ThrowIfNull(innerException);

        return new HttpRequestException(message, innerException);
    }

    private static void DeletePartialDownload(string filePath, Exception transferFailure)
    {
        if (!File.Exists(filePath)) {
            return;
        }

        try {
            File.Delete(filePath);
        }
        catch (IOException exception) {
            throw new IOException(
                $"The download failed and the partial destination file '{filePath}' could not be removed.",
                new AggregateException(transferFailure, exception));
        }
        catch (UnauthorizedAccessException exception) {
            throw new IOException(
                $"The download failed and the partial destination file '{filePath}' could not be removed.",
                new AggregateException(transferFailure, exception));
        }
    }
}

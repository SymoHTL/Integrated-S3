using System.Net.Http.Headers;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

/// <summary>
/// Extension methods for converting a <see cref="StoragePresignedRequest"/> into an <see cref="HttpRequestMessage"/>.
/// </summary>
public static class StoragePresignedRequestExtensions
{
    /// <summary>
    /// Creates an <see cref="HttpRequestMessage"/> from the presigned request, applying the presigned URL,
    /// HTTP method, required headers, and optional <paramref name="content"/>.
    /// </summary>
    /// <param name="request">The <see cref="StoragePresignedRequest"/> to convert.</param>
    /// <param name="content">Optional <see cref="HttpContent"/> to attach as the request body (required for PUT uploads).</param>
    /// <returns>A fully configured <see cref="HttpRequestMessage"/> ready to be sent via an <see cref="HttpClient"/>.</returns>
    public static HttpRequestMessage CreateHttpRequestMessage(
        this StoragePresignedRequest request,
        HttpContent? content = null)
    {
        ArgumentNullException.ThrowIfNull(request);

        var httpRequest = new HttpRequestMessage(new HttpMethod(request.Method), request.Url);
        httpRequest.Content = content;

        foreach (var header in request.Headers) {
            ApplyHeader(httpRequest, header);
        }

        return httpRequest;
    }

    private static void ApplyHeader(HttpRequestMessage request, StoragePresignedHeader header)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(header);

        if (string.Equals(header.Name, "Content-Type", StringComparison.OrdinalIgnoreCase)) {
            if (request.Content is null) {
                throw new InvalidOperationException("The presigned request requires a Content-Type header, but no content was supplied.");
            }

            request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(header.Value);
            return;
        }

        if (request.Headers.TryAddWithoutValidation(header.Name, header.Value)) {
            return;
        }

        if (request.Content is null) {
            throw new InvalidOperationException($"The presigned request requires the '{header.Name}' header, but no content was supplied.");
        }

        request.Content.Headers.TryAddWithoutValidation(header.Name, header.Value);
    }
}

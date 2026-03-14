using System.Net.Http.Headers;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

/// <summary>
/// Helpers for turning <see cref="StoragePresignedRequest"/> values into executable HTTP requests.
/// </summary>
public static class StoragePresignedRequestExtensions
{
    /// <summary>
    /// Creates an <see cref="HttpRequestMessage"/> and applies all signed headers required by the presigned request.
    /// </summary>
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

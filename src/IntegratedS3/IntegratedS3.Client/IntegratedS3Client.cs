using System.Net.Http.Json;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

/// <summary>
/// Default HTTP client for the IntegratedS3 presign endpoint.
/// </summary>
public sealed class IntegratedS3Client(HttpClient httpClient, string routePrefix = IntegratedS3ClientOptions.DefaultRoutePrefix) : IIntegratedS3Client
{
    private readonly HttpClient _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    private readonly string _routePrefix = IntegratedS3ClientPathUtilities.NormalizeRoutePrefix(routePrefix);

    /// <summary>
    /// Requests a presigned object operation from the configured route prefix.
    /// </summary>
    public async ValueTask<StoragePresignedRequest> PresignObjectAsync(
        StoragePresignRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        using var response = await _httpClient.PostAsJsonAsync(
            $"{_routePrefix}/presign/object",
            request,
            IntegratedS3ClientJsonSerializerContext.Default.StoragePresignRequest,
            cancellationToken);

        await EnsurePresignSuccessAsync(response, cancellationToken);

        var presignedRequest = await response.Content.ReadFromJsonAsync(
            IntegratedS3ClientJsonSerializerContext.Default.StoragePresignedRequest,
            cancellationToken);

        return presignedRequest ?? throw new InvalidOperationException("The IntegratedS3 host returned an empty presign response.");
    }

    /// <summary>
    /// Throws an <see cref="HttpRequestException"/> that includes the response body when the
    /// presign endpoint returns a non-success status, giving callers more actionable error detail.
    /// </summary>
    private static async Task EnsurePresignSuccessAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode) {
            return;
        }

        var body = response.Content is null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken);

        var statusCode = (int)response.StatusCode;
        var message = string.IsNullOrWhiteSpace(body)
            ? $"The IntegratedS3 presign request failed with HTTP {statusCode} ({response.ReasonPhrase})."
            : $"The IntegratedS3 presign request failed with HTTP {statusCode} ({response.ReasonPhrase}): {body}";

        throw new HttpRequestException(message, inner: null, statusCode: response.StatusCode);
    }

}

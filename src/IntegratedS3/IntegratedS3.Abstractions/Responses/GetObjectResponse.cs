using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Responses;

/// <summary>Contains the object data stream and metadata for a successful GetObject operation. Implements <see cref="IAsyncDisposable"/> to release the content stream.</summary>
public sealed class GetObjectResponse : IAsyncDisposable
{
    /// <summary>The object metadata.</summary>
    public required ObjectInfo Object { get; init; }

    /// <summary>The object data stream. Must be disposed after use.</summary>
    public required Stream Content { get; init; }

    /// <summary>Total size of the complete object in bytes.</summary>
    public long TotalContentLength { get; init; }

    /// <summary>The byte range returned, if a range request was made.</summary>
    public ObjectRange? Range { get; init; }

    /// <summary><see langword="true"/> if a conditional request determined the object has not been modified (HTTP 304 equivalent).</summary>
    public bool IsNotModified { get; init; }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        return Content.DisposeAsync();
    }
}

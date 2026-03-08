using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Responses;

public sealed class GetObjectResponse : IAsyncDisposable
{
    public required ObjectInfo Object { get; init; }

    public required Stream Content { get; init; }

    public long TotalContentLength { get; init; }

    public ObjectRange? Range { get; init; }

    public bool IsNotModified { get; init; }

    public ValueTask DisposeAsync()
    {
        return Content.DisposeAsync();
    }
}

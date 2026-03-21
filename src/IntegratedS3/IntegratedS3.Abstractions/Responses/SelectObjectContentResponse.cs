namespace IntegratedS3.Abstractions.Responses;

/// <summary>Response for an S3 Select query, containing the event stream with query results.</summary>
public sealed class SelectObjectContentResponse : IAsyncDisposable
{
    /// <summary>
    /// The raw S3 Select event stream body. Callers must stream this directly
    /// to the HTTP response and dispose when finished.
    /// </summary>
    public required Stream EventStream { get; init; }

    /// <summary>The MIME type of the event stream.</summary>
    public string? ContentType { get; init; }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        return EventStream.DisposeAsync();
    }
}

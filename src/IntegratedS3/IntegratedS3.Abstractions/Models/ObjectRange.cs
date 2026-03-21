namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Byte range for partial content (HTTP range) requests.
/// </summary>
public sealed class ObjectRange
{
    /// <summary>
    /// The inclusive start byte offset, or <see langword="null"/> to start from the beginning of the object.
    /// </summary>
    public long? Start { get; init; }

    /// <summary>
    /// The inclusive end byte offset, or <see langword="null"/> to read to the end of the object.
    /// </summary>
    public long? End { get; init; }
}

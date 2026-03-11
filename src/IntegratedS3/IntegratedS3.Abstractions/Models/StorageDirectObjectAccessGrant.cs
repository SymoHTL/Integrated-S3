namespace IntegratedS3.Abstractions.Models;

public sealed class StorageDirectObjectAccessGrant
{
    public required Uri Url { get; init; }

    public required DateTimeOffset ExpiresAtUtc { get; init; }

    public Dictionary<string, string> Headers { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}

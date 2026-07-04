namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Helpers for the S3 <c>x-amz-storage-class</c> value: the canonical set of storage classes, the
/// default (<c>STANDARD</c>), validation, and normalization for persistence and echo.
/// </summary>
public static class StorageClass
{
    /// <summary>The default storage class applied when a request does not specify one.</summary>
    public const string Standard = "STANDARD";

    /// <summary>
    /// The set of storage-class values accepted on write. Mirrors the values AWS S3 accepts for the
    /// <c>x-amz-storage-class</c> header.
    /// </summary>
    private static readonly HashSet<string> Known = new(StringComparer.Ordinal)
    {
        "STANDARD",
        "REDUCED_REDUNDANCY",
        "STANDARD_IA",
        "ONEZONE_IA",
        "INTELLIGENT_TIERING",
        "GLACIER",
        "DEEP_ARCHIVE",
        "GLACIER_IR",
        "SNOW",
        "EXPRESS_ONEZONE",
    };

    /// <summary>
    /// Determines whether <paramref name="value"/> is a recognized storage class. A null or
    /// whitespace value is treated as valid because it means "unspecified" (defaults to STANDARD).
    /// </summary>
    public static bool IsKnown(string? value)
        => string.IsNullOrWhiteSpace(value) || Known.Contains(value);

    /// <summary>
    /// Normalizes a stored/read storage-class value for echo: a null, whitespace, or unrecognized
    /// value collapses to <see cref="Standard"/> so read paths always report a concrete class.
    /// </summary>
    public static string NormalizeForEcho(string? value)
        => string.IsNullOrWhiteSpace(value) ? Standard : value;

    /// <summary>
    /// Returns <see langword="true"/> when the resolved storage class differs from <see cref="Standard"/>.
    /// AWS omits the <c>x-amz-storage-class</c> response header on GET/HEAD for STANDARD objects and
    /// emits it only for non-STANDARD classes.
    /// </summary>
    public static bool IsNonStandard(string? value)
        => !string.Equals(NormalizeForEcho(value), Standard, StringComparison.Ordinal);
}

using System.Text;

namespace IntegratedS3.Abstractions.Comparers;

/// <summary>
/// An ordinal string comparer that orders strings by their UTF-8 byte sequence, matching the
/// lexicographic ordering AWS S3 applies to object keys, common prefixes, and version markers.
/// </summary>
/// <remarks>
/// <para>
/// .NET's <see cref="StringComparer.Ordinal"/> compares UTF-16 code units. For ASCII and the
/// Basic Multilingual Plane the result matches UTF-8 byte order, but astral-plane characters
/// (code points U+10000 and above, encoded in UTF-16 as surrogate pairs whose code units lie in
/// U+D800..U+DFFF) sort <em>before</em> BMP characters such as U+E000..U+FFFF under code-unit
/// ordering, whereas AWS &#8212; sorting raw UTF-8 bytes &#8212; sorts them <em>after</em>. This
/// comparer reproduces the AWS ordering so listing pagination, marker filtering, and
/// <c>CommonPrefixes</c> grouping stay consistent for keys containing emoji and other astral
/// characters.
/// </para>
/// <para>
/// UTF-8 byte order is equivalent to Unicode scalar-value (code point) order, so the comparison is
/// performed by decoding each string into <see cref="Rune"/> values and comparing their scalar
/// values. This avoids materializing intermediate byte buffers while producing exactly the same
/// ordering as comparing <c>Encoding.UTF8.GetBytes(x)</c> against <c>Encoding.UTF8.GetBytes(y)</c>.
/// </para>
/// </remarks>
public sealed class Utf8OrdinalComparer : IComparer<string?>, IEqualityComparer<string?>
{
    /// <summary>
    /// Gets the shared, thread-safe <see cref="Utf8OrdinalComparer"/> instance.
    /// </summary>
    public static Utf8OrdinalComparer Instance { get; } = new();

    private Utf8OrdinalComparer()
    {
    }

    /// <summary>
    /// Compares two strings by their UTF-8 byte sequence (equivalently, Unicode scalar-value order).
    /// </summary>
    /// <param name="x">The first string, or <c>null</c>.</param>
    /// <param name="y">The second string, or <c>null</c>.</param>
    /// <returns>
    /// A negative value if <paramref name="x"/> sorts before <paramref name="y"/>, zero if they are
    /// equal, and a positive value if <paramref name="x"/> sorts after <paramref name="y"/>.
    /// <c>null</c> sorts before any non-<c>null</c> value.
    /// </returns>
    public int Compare(string? x, string? y)
    {
        if (ReferenceEquals(x, y)) {
            return 0;
        }

        if (x is null) {
            return -1;
        }

        if (y is null) {
            return 1;
        }

        return Compare(x.AsSpan(), y.AsSpan());
    }

    /// <summary>
    /// Compares two character spans by their UTF-8 byte sequence (equivalently, Unicode scalar-value order).
    /// </summary>
    /// <param name="x">The first span.</param>
    /// <param name="y">The second span.</param>
    /// <returns>
    /// A negative value if <paramref name="x"/> sorts before <paramref name="y"/>, zero if they are
    /// equal, and a positive value if <paramref name="x"/> sorts after <paramref name="y"/>.
    /// </returns>
    public static int Compare(ReadOnlySpan<char> x, ReadOnlySpan<char> y)
    {
        var xIndex = 0;
        var yIndex = 0;

        while (xIndex < x.Length && yIndex < y.Length) {
            var xScalar = DecodeScalar(x, ref xIndex);
            var yScalar = DecodeScalar(y, ref yIndex);

            if (xScalar != yScalar) {
                return xScalar < yScalar ? -1 : 1;
            }
        }

        // Shared prefix consumed identically; the shorter (fully consumed) span sorts first.
        return (x.Length - xIndex).CompareTo(y.Length - yIndex);
    }

    /// <summary>
    /// Determines whether two strings are ordinally equal (identical UTF-16 content).
    /// </summary>
    /// <param name="x">The first string, or <c>null</c>.</param>
    /// <param name="y">The second string, or <c>null</c>.</param>
    /// <returns><c>true</c> if the strings are equal; otherwise <c>false</c>.</returns>
    public bool Equals(string? x, string? y) => string.Equals(x, y, StringComparison.Ordinal);

    /// <summary>
    /// Returns an ordinal hash code for the supplied string.
    /// </summary>
    /// <param name="obj">The string to hash.</param>
    /// <returns>An ordinal hash code.</returns>
    public int GetHashCode(string? obj) => obj is null ? 0 : string.GetHashCode(obj, StringComparison.Ordinal);

    /// <summary>
    /// Decodes the Unicode scalar value at <paramref name="index"/>, advancing <paramref name="index"/>
    /// past the consumed UTF-16 code unit(s). A lone or malformed surrogate is treated as the Unicode
    /// replacement character (U+FFFD), matching how UTF-8 encoding would round-trip invalid input.
    /// </summary>
    private static int DecodeScalar(ReadOnlySpan<char> value, ref int index)
    {
        var current = value[index];

        if (char.IsHighSurrogate(current)
            && index + 1 < value.Length
            && char.IsLowSurrogate(value[index + 1])) {
            var scalar = char.ConvertToUtf32(current, value[index + 1]);
            index += 2;
            return scalar;
        }

        index += 1;

        // Unpaired surrogate: substitute the replacement character so ordering stays deterministic.
        return char.IsSurrogate(current) ? 0xFFFD : current;
    }
}

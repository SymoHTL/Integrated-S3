using System.Text;

namespace IntegratedS3.Engine.Metadata;

/// <summary>
/// Object keys as the metadata store holds them: UTF-8 bytes, compared bytewise, which is S3's key order.
/// </summary>
internal static class KeyRange
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>
    /// Encodes a key. Throws <see cref="ArgumentException"/> for a string that is not valid UTF-16, which no
    /// decoded request produces.
    /// </summary>
    public static byte[] Encode(string key) => StrictUtf8.GetBytes(key);

    public static string Decode(byte[] key) => StrictUtf8.GetString(key);

    /// <summary>
    /// Returns the smallest byte string that sorts after every string starting with <paramref name="prefix"/>, or
    /// <see langword="null"/> when there is none (an empty prefix, or one of 0xFF bytes only).
    /// </summary>
    public static byte[]? PrefixUpperBound(ReadOnlySpan<byte> prefix)
    {
        for (var i = prefix.Length - 1; i >= 0; i--) {
            if (prefix[i] != 0xFF) {
                var bound = prefix[..(i + 1)].ToArray();
                bound[i]++;
                return bound;
            }
        }

        return null;
    }
}

internal static class EngineClock
{
    public static long ToMicroseconds(DateTimeOffset value) => (value.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / TimeSpan.TicksPerMicrosecond;

    public static DateTimeOffset FromMicroseconds(long microseconds) => DateTimeOffset.UnixEpoch.AddTicks(microseconds * TimeSpan.TicksPerMicrosecond);
}

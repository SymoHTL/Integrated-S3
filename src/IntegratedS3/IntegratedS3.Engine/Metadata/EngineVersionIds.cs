using System.Globalization;
using System.Security.Cryptography;

namespace IntegratedS3.Engine.Metadata;

/// <summary>
/// Version ids the engine assigns: 12 hex digits of the key's sequence number, then 20 random hex digits. The
/// random part keeps an id from ever being handed out twice, even to a key deleted and written again; the
/// sequence part gives a listing its place within the key when the marker's version has since been deleted.
/// </summary>
internal static class EngineVersionIds
{
    private const int SeqDigits = 12;
    private const int Length = 32;

    public static string Create(long seq)
    {
        if (seq is < 1 or >= 1L << (4 * SeqDigits)) {
            throw new ArgumentOutOfRangeException(nameof(seq), seq, "A key's sequence number must fit in 12 hex digits.");
        }

        return string.Create(Length, seq, static (span, value) => {
            value.TryFormat(span[..SeqDigits], out _, "x12", CultureInfo.InvariantCulture);
            RandomNumberGenerator.GetHexString(span[SeqDigits..], lowercase: true);
        });
    }

    public static bool TryGetSeq(string versionId, out long seq)
    {
        seq = 0;
        return versionId.Length == Length
            && !versionId.AsSpan().ContainsAnyExcept("0123456789abcdef")
            && long.TryParse(versionId.AsSpan(0, SeqDigits), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out seq);
    }
}

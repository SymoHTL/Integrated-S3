using System.Text;
using IntegratedS3.Abstractions.Comparers;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Regression tests for issue #167: object-key sorting must match AWS S3, which orders keys by their
/// raw UTF-8 byte sequence rather than by .NET's UTF-16 code-unit (<see cref="StringComparer.Ordinal"/>)
/// ordering. The two orderings diverge for astral-plane (surrogate-pair) characters.
/// </summary>
public sealed class Utf8OrdinalComparerTests
{
    // "k" + U+1F600 (astral, encoded in UTF-16 as the surrogate pair D83D DE00).
    private const string AstralKey = "k\U0001F600";

    // "k" + U+F900 (BMP, single UTF-16 code unit F900).
    private const string BmpKey = "k豈";

    [Fact]
    public void Ordinal_And_Utf8_DisagreeForAstralKeys_ProvingTheBugScenario()
    {
        // UTF-16 code-unit ordering (the previous behaviour) puts the astral key first because the
        // high-surrogate code unit 0xD83D sorts before the BMP code unit 0xF900.
        Assert.True(StringComparer.Ordinal.Compare(AstralKey, BmpKey) < 0);

        // Raw UTF-8 byte ordering (what AWS does) puts the BMP key first because its three-byte
        // sequence (EF A4 80) sorts before the astral key's four-byte sequence (F0 9F 98 80).
        var astralBytes = Encoding.UTF8.GetBytes(AstralKey);
        var bmpBytes = Encoding.UTF8.GetBytes(BmpKey);
        Assert.True(astralBytes.AsSpan().SequenceCompareTo(bmpBytes) > 0);
    }

    [Fact]
    public void Compare_OrdersAstralKeys_ByUtf8ByteOrder()
    {
        // The BMP key must sort BEFORE the astral key (opposite of StringComparer.Ordinal).
        Assert.True(Utf8OrdinalComparer.Instance.Compare(BmpKey, AstralKey) < 0);
        Assert.True(Utf8OrdinalComparer.Instance.Compare(AstralKey, BmpKey) > 0);
    }

    [Fact]
    public void Compare_MatchesRawUtf8ByteComparison_AcrossMixedKeys()
    {
        string[] keys =
        [
            string.Empty,
            "a",
            "a/b",
            "a豈",
            "a\U0001F600",
            "b",
            "k豈",
            "k\U0001F600",
            "\U0001F600",
            "豈",
            "z",
        ];

        for (var i = 0; i < keys.Length; i++) {
            for (var j = 0; j < keys.Length; j++) {
                var expected = Math.Sign(Encoding.UTF8.GetBytes(keys[i]).AsSpan()
                    .SequenceCompareTo(Encoding.UTF8.GetBytes(keys[j])));
                var actual = Math.Sign(Utf8OrdinalComparer.Instance.Compare(keys[i], keys[j]));
                Assert.Equal(expected, actual);
            }
        }
    }

    [Fact]
    public void Sort_ProducesUtf8ByteOrder_ForAstralAndBmpKeys()
    {
        var keys = new List<string> { AstralKey, BmpKey, "k", "kz" };
        keys.Sort(Utf8OrdinalComparer.Instance);

        // UTF-8 byte order: "k" (6b), "kz" (6b 7a), "k豈" (6b ef a4 80), "k🙂" (6b f0 9f 98 80).
        Assert.Equal(["k", "kz", BmpKey, AstralKey], keys);
    }

    [Fact]
    public void Compare_TreatsNullAsSmallest_AndEqualReferencesAsEqual()
    {
        Assert.Equal(0, Utf8OrdinalComparer.Instance.Compare(null, null));
        Assert.True(Utf8OrdinalComparer.Instance.Compare(null, "a") < 0);
        Assert.True(Utf8OrdinalComparer.Instance.Compare("a", null) > 0);
        Assert.Equal(0, Utf8OrdinalComparer.Instance.Compare("same", "same"));
    }

    [Fact]
    public void Compare_ShorterPrefixSortsFirst()
    {
        Assert.True(Utf8OrdinalComparer.Instance.Compare("a", "ab") < 0);
        Assert.True(Utf8OrdinalComparer.Instance.Compare("ab", "a") > 0);
    }
}

using System.Globalization;
using System.Text;

namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Validates tag keys and values against S3 tagging constraints.
/// </summary>
public static class ObjectTagValidation
{
    /// <summary>
    /// Maximum number of tags allowed per object (10).
    /// </summary>
    public const int MaxTagCount = 10;

    /// <summary>
    /// Maximum length of a tag key in Unicode characters (128).
    /// </summary>
    public const int MaxKeyLength = 128;

    /// <summary>
    /// Maximum length of a tag value in Unicode characters (256).
    /// </summary>
    public const int MaxValueLength = 256;
    private const string ReservedPrefix = "aws:";

    /// <summary>
    /// Validates a dictionary of tags against S3 tagging constraints.
    /// </summary>
    /// <param name="tags">The tags to validate, or <see langword="null"/>.</param>
    /// <returns><see langword="null"/> if valid; otherwise an error message describing the first violation.</returns>
    public static string? Validate(IReadOnlyDictionary<string, string>? tags)
    {
        if (tags is null || tags.Count == 0) {
            return null;
        }

        if (tags.Count > MaxTagCount) {
            return $"Object tag sets cannot contain more than {MaxTagCount} tags.";
        }

        foreach (var tag in tags) {
            var validationMessage = ValidateTag(tag.Key, tag.Value);
            if (validationMessage is not null) {
                return validationMessage;
            }
        }

        return null;
    }

    /// <summary>
    /// Validates a list of tag key-value pairs against S3 tagging constraints, including duplicate key detection.
    /// </summary>
    /// <param name="tags">The tags to validate.</param>
    /// <returns><see langword="null"/> if valid; otherwise an error message describing the first violation.</returns>
    public static string? Validate(IReadOnlyList<KeyValuePair<string, string>> tags)
    {
        ArgumentNullException.ThrowIfNull(tags);

        if (tags.Count == 0) {
            return null;
        }

        if (tags.Count > MaxTagCount) {
            return $"Object tag sets cannot contain more than {MaxTagCount} tags.";
        }

        var seenKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var tag in tags) {
            if (!seenKeys.Add(tag.Key)) {
                return $"Duplicate tag key '{tag.Key}' is not allowed.";
            }

            var validationMessage = ValidateTag(tag.Key, tag.Value);
            if (validationMessage is not null) {
                return validationMessage;
            }
        }

        return null;
    }

    private static string? ValidateTag(string key, string value)
    {
        if (CountUnicodeCharacters(key) > MaxKeyLength) {
            return $"Tag key '{key}' exceeds the maximum length of {MaxKeyLength} characters.";
        }

        if (CountUnicodeCharacters(value) > MaxValueLength) {
            return $"Tag value for key '{key}' exceeds the maximum length of {MaxValueLength} characters.";
        }

        if (key.StartsWith(ReservedPrefix, StringComparison.OrdinalIgnoreCase)) {
            return $"Tag key '{key}' uses the reserved '{ReservedPrefix}' prefix.";
        }

        if (!ContainsOnlySupportedCharacters(key)) {
            return $"Tag key '{key}' contains unsupported characters.";
        }

        if (!ContainsOnlySupportedCharacters(value)) {
            return $"Tag value for key '{key}' contains unsupported characters.";
        }

        return null;
    }

    private static int CountUnicodeCharacters(string value)
    {
        var count = 0;
        foreach (var _ in value.EnumerateRunes()) {
            count++;
        }

        return count;
    }

    private static bool ContainsOnlySupportedCharacters(string value)
    {
        foreach (var rune in value.EnumerateRunes()) {
            if (!IsSupportedTagCharacter(rune)) {
                return false;
            }
        }

        return true;
    }

    private static bool IsSupportedTagCharacter(Rune rune)
    {
        var category = Rune.GetUnicodeCategory(rune);
        if (category is UnicodeCategory.UppercaseLetter
            or UnicodeCategory.LowercaseLetter
            or UnicodeCategory.TitlecaseLetter
            or UnicodeCategory.ModifierLetter
            or UnicodeCategory.OtherLetter
            or UnicodeCategory.DecimalDigitNumber
            or UnicodeCategory.LetterNumber
            or UnicodeCategory.OtherNumber) {
            return true;
        }

        return rune.Value is ' ' or '+' or '-' or '=' or '.' or '_' or ':' or '/' or '@';
    }
}

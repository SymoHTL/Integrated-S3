namespace IntegratedS3.Abstractions.Models;

public static class ObjectTagValidation
{
    public const int MaxTagCount = 10;
    public const int MaxKeyLength = 128;
    public const int MaxValueLength = 256;
    private const string ReservedPrefix = "aws:";

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
        if (key.Length > MaxKeyLength) {
            return $"Tag key '{key}' exceeds the maximum length of {MaxKeyLength} characters.";
        }

        if (value.Length > MaxValueLength) {
            return $"Tag value for key '{key}' exceeds the maximum length of {MaxValueLength} characters.";
        }

        if (key.StartsWith(ReservedPrefix, StringComparison.OrdinalIgnoreCase)) {
            return $"Tag key '{key}' uses the reserved '{ReservedPrefix}' prefix.";
        }

        return null;
    }
}

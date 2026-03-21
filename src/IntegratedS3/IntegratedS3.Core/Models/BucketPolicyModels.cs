using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace IntegratedS3.Core.Models;

/// <summary>
/// Effect for a bucket policy statement: Allow or Deny.
/// </summary>
public enum PolicyEffect
{
    Allow,
    Deny
}

/// <summary>
/// A parsed bucket policy document containing one or more statements.
/// </summary>
public sealed class ParsedBucketPolicy
{
    public string? Version { get; init; }

    public string? Id { get; init; }

    public required IReadOnlyList<PolicyStatement> Statements { get; init; }
}

/// <summary>
/// A single policy statement within a bucket policy.
/// </summary>
public sealed class PolicyStatement
{
    public string? Sid { get; init; }

    public required PolicyEffect Effect { get; init; }

    /// <summary>
    /// Principal values. "*" means all principals.
    /// </summary>
    public required IReadOnlyList<string> Principals { get; init; }

    /// <summary>
    /// S3 action strings, e.g. "s3:GetObject", "s3:PutObject", "s3:*".
    /// </summary>
    public required IReadOnlyList<string> Actions { get; init; }

    /// <summary>
    /// Resource ARNs with optional wildcards, e.g. "arn:aws:s3:::bucket/*".
    /// </summary>
    public required IReadOnlyList<string> Resources { get; init; }

    /// <summary>
    /// Optional conditions keyed by operator, then condition key, then values.
    /// Example: { "StringEquals": { "s3:prefix": ["logs/"] } }
    /// </summary>
    public IReadOnlyDictionary<string, IReadOnlyDictionary<string, IReadOnlyList<string>>>? Conditions { get; init; }
}

/// <summary>
/// Context passed during policy evaluation so conditions can be checked.
/// </summary>
public sealed class PolicyEvaluationContext
{
    public string? SourceIp { get; init; }

    public bool IsSecureTransport { get; init; }

    public string? Prefix { get; init; }
}

/// <summary>
/// Result of a bucket policy evaluation.
/// </summary>
public enum PolicyEvaluationResult
{
    /// <summary>No matching statement found — implicit deny.</summary>
    ImplicitDeny,

    /// <summary>An explicit Allow statement matched.</summary>
    Allow,

    /// <summary>An explicit Deny statement matched.</summary>
    ExplicitDeny
}

/// <summary>
/// Maps <see cref="StorageOperationType"/> to S3 action strings and resource scopes.
/// </summary>
public static class S3ActionMapping
{
    /// <summary>
    /// Returns the S3 action string(s) corresponding to the given operation type.
    /// </summary>
    public static string? GetS3Action(StorageOperationType operation) => operation switch
    {
        StorageOperationType.ListObjects => "s3:ListBucket",
        StorageOperationType.ListObjectVersions => "s3:ListBucketVersions",
        StorageOperationType.ListMultipartUploads => "s3:ListBucketMultipartUploads",
        StorageOperationType.HeadBucket => "s3:ListBucket",
        StorageOperationType.GetObject => "s3:GetObject",
        StorageOperationType.HeadObject => "s3:GetObject",
        StorageOperationType.PutObject => "s3:PutObject",
        StorageOperationType.CopyObject => "s3:PutObject",
        StorageOperationType.DeleteObject => "s3:DeleteObject",
        StorageOperationType.GetBucketLocation => "s3:GetBucketLocation",
        StorageOperationType.GetBucketPolicy => "s3:GetBucketPolicy",
        StorageOperationType.PutBucketPolicy => "s3:PutBucketPolicy",
        StorageOperationType.DeleteBucketPolicy => "s3:DeleteBucketPolicy",
        StorageOperationType.GetBucketCors => "s3:GetBucketCors",
        StorageOperationType.PutBucketCors => "s3:PutBucketCors",
        StorageOperationType.DeleteBucketCors => "s3:PutBucketCors",
        StorageOperationType.InitiateMultipartUpload => "s3:PutObject",
        StorageOperationType.UploadMultipartPart => "s3:PutObject",
        StorageOperationType.UploadPartCopy => "s3:PutObject",
        StorageOperationType.CompleteMultipartUpload => "s3:PutObject",
        StorageOperationType.AbortMultipartUpload => "s3:PutObject",
        StorageOperationType.GetBucketTagging => "s3:GetBucketTagging",
        StorageOperationType.PutBucketTagging => "s3:PutBucketTagging",
        StorageOperationType.DeleteBucketTagging => "s3:PutBucketTagging",
        StorageOperationType.GetBucketLogging => "s3:GetBucketLogging",
        StorageOperationType.PutBucketLogging => "s3:PutBucketLogging",
        StorageOperationType.GetBucketWebsite => "s3:GetBucketWebsite",
        StorageOperationType.PutBucketWebsite => "s3:PutBucketWebsite",
        StorageOperationType.DeleteBucketWebsite => "s3:DeleteBucketWebsite",
        StorageOperationType.GetBucketRequestPayment => "s3:GetBucketRequestPayment",
        StorageOperationType.PutBucketRequestPayment => "s3:PutBucketRequestPayment",
        StorageOperationType.GetBucketAccelerate => "s3:GetAccelerateConfiguration",
        StorageOperationType.PutBucketAccelerate => "s3:PutAccelerateConfiguration",
        StorageOperationType.GetBucketLifecycle => "s3:GetLifecycleConfiguration",
        StorageOperationType.PutBucketLifecycle => "s3:PutLifecycleConfiguration",
        StorageOperationType.DeleteBucketLifecycle => "s3:PutLifecycleConfiguration",
        StorageOperationType.GetBucketReplication => "s3:GetReplicationConfiguration",
        StorageOperationType.PutBucketReplication => "s3:PutReplicationConfiguration",
        StorageOperationType.DeleteBucketReplication => "s3:PutReplicationConfiguration",
        StorageOperationType.GetBucketNotificationConfiguration => "s3:GetBucketNotification",
        StorageOperationType.PutBucketNotificationConfiguration => "s3:PutBucketNotification",
        StorageOperationType.GetObjectLockConfiguration => "s3:GetBucketObjectLockConfiguration",
        StorageOperationType.PutObjectLockConfiguration => "s3:PutBucketObjectLockConfiguration",
        StorageOperationType.GetBucketAnalyticsConfiguration => "s3:GetAnalyticsConfiguration",
        StorageOperationType.PutBucketAnalyticsConfiguration => "s3:PutAnalyticsConfiguration",
        StorageOperationType.DeleteBucketAnalyticsConfiguration => "s3:PutAnalyticsConfiguration",
        StorageOperationType.GetBucketMetricsConfiguration => "s3:GetMetricsConfiguration",
        StorageOperationType.PutBucketMetricsConfiguration => "s3:PutMetricsConfiguration",
        StorageOperationType.DeleteBucketMetricsConfiguration => "s3:PutMetricsConfiguration",
        StorageOperationType.GetBucketInventoryConfiguration => "s3:GetInventoryConfiguration",
        StorageOperationType.PutBucketInventoryConfiguration => "s3:PutInventoryConfiguration",
        StorageOperationType.DeleteBucketInventoryConfiguration => "s3:PutInventoryConfiguration",
        StorageOperationType.GetBucketIntelligentTieringConfiguration => "s3:GetIntelligentTieringConfiguration",
        StorageOperationType.PutBucketIntelligentTieringConfiguration => "s3:PutIntelligentTieringConfiguration",
        StorageOperationType.DeleteBucketIntelligentTieringConfiguration => "s3:PutIntelligentTieringConfiguration",
        StorageOperationType.PutObjectRetention => "s3:PutObjectRetention",
        StorageOperationType.PutObjectLegalHold => "s3:PutObjectLegalHold",
        StorageOperationType.SelectObjectContent => "s3:GetObject",
        StorageOperationType.RestoreObject => "s3:RestoreObject",
        StorageOperationType.GetObjectAttributes => "s3:GetObjectAttributes",
        _ => null
    };

    /// <summary>
    /// Determines whether the operation targets an object-level resource (as opposed to bucket-level).
    /// </summary>
    public static bool IsObjectLevelOperation(StorageOperationType operation) => operation switch
    {
        StorageOperationType.GetObject => true,
        StorageOperationType.HeadObject => true,
        StorageOperationType.PutObject => true,
        StorageOperationType.CopyObject => true,
        StorageOperationType.DeleteObject => true,
        StorageOperationType.InitiateMultipartUpload => true,
        StorageOperationType.UploadMultipartPart => true,
        StorageOperationType.UploadPartCopy => true,
        StorageOperationType.CompleteMultipartUpload => true,
        StorageOperationType.AbortMultipartUpload => true,
        StorageOperationType.PutObjectRetention => true,
        StorageOperationType.PutObjectLegalHold => true,
        StorageOperationType.SelectObjectContent => true,
        StorageOperationType.RestoreObject => true,
        StorageOperationType.GetObjectAttributes => true,
        _ => false
    };
}

/// <summary>
/// Evaluates parsed bucket policies using standard IAM evaluation logic:
/// 1. If any Deny matches → explicit deny
/// 2. If any Allow matches → allow
/// 3. Otherwise → implicit deny
/// </summary>
public static class BucketPolicyEvaluator
{
    /// <summary>
    /// Evaluates whether a request is allowed by the given parsed policy.
    /// </summary>
    public static PolicyEvaluationResult Evaluate(
        ParsedBucketPolicy policy,
        string bucketName,
        StorageOperationType operation,
        string? objectKey,
        string? principal,
        PolicyEvaluationContext? context)
    {
        var s3Action = S3ActionMapping.GetS3Action(operation);
        if (s3Action is null) {
            return PolicyEvaluationResult.ImplicitDeny;
        }

        var resourceArn = BuildResourceArn(bucketName, objectKey, operation);
        var hasAllow = false;

        foreach (var statement in policy.Statements) {
            if (!MatchesPrincipal(statement, principal)) {
                continue;
            }

            if (!MatchesAction(statement, s3Action)) {
                continue;
            }

            if (!MatchesResource(statement, resourceArn)) {
                continue;
            }

            if (!MatchesConditions(statement, context)) {
                continue;
            }

            if (statement.Effect == PolicyEffect.Deny) {
                return PolicyEvaluationResult.ExplicitDeny;
            }

            hasAllow = true;
        }

        return hasAllow ? PolicyEvaluationResult.Allow : PolicyEvaluationResult.ImplicitDeny;
    }

    private static string BuildResourceArn(string bucketName, string? objectKey, StorageOperationType operation)
    {
        if (S3ActionMapping.IsObjectLevelOperation(operation) && !string.IsNullOrEmpty(objectKey)) {
            return $"arn:aws:s3:::{bucketName}/{objectKey}";
        }

        return $"arn:aws:s3:::{bucketName}";
    }

    private static bool MatchesPrincipal(PolicyStatement statement, string? principal)
    {
        foreach (var p in statement.Principals) {
            if (string.Equals(p, "*", StringComparison.Ordinal)) {
                return true;
            }

            if (principal is not null && string.Equals(p, principal, StringComparison.Ordinal)) {
                return true;
            }
        }

        return false;
    }

    private static bool MatchesAction(PolicyStatement statement, string s3Action)
    {
        foreach (var action in statement.Actions) {
            if (string.Equals(action, "s3:*", StringComparison.OrdinalIgnoreCase)) {
                return true;
            }

            if (WildcardMatch(action, s3Action)) {
                return true;
            }
        }

        return false;
    }

    private static bool MatchesResource(PolicyStatement statement, string resourceArn)
    {
        foreach (var resource in statement.Resources) {
            if (string.Equals(resource, "*", StringComparison.Ordinal)) {
                return true;
            }

            if (WildcardMatch(resource, resourceArn)) {
                return true;
            }
        }

        return false;
    }

    private static bool MatchesConditions(PolicyStatement statement, PolicyEvaluationContext? context)
    {
        if (statement.Conditions is null || statement.Conditions.Count == 0) {
            return true;
        }

        if (context is null) {
            return false;
        }

        foreach (var (conditionOperator, conditionKeys) in statement.Conditions) {
            foreach (var (conditionKey, conditionValues) in conditionKeys) {
                if (!EvaluateCondition(conditionOperator, conditionKey, conditionValues, context)) {
                    return false;
                }
            }
        }

        return true;
    }

    private static bool EvaluateCondition(
        string conditionOperator,
        string conditionKey,
        IReadOnlyList<string> conditionValues,
        PolicyEvaluationContext context)
    {
        var requestValue = GetConditionKeyValue(conditionKey, context);

        return conditionOperator switch
        {
            "StringEquals" => requestValue is not null && conditionValues.Any(v => string.Equals(v, requestValue, StringComparison.Ordinal)),
            "StringNotEquals" => requestValue is null || conditionValues.All(v => !string.Equals(v, requestValue, StringComparison.Ordinal)),
            "StringLike" => requestValue is not null && conditionValues.Any(v => WildcardMatch(v, requestValue)),
            "StringNotLike" => requestValue is null || conditionValues.All(v => !WildcardMatch(v, requestValue)),
            "Bool" => EvaluateBoolCondition(conditionKey, conditionValues, context),
            "IpAddress" => requestValue is not null && conditionValues.Any(v => IpMatchesCidr(requestValue, v)),
            "NotIpAddress" => requestValue is null || conditionValues.All(v => !IpMatchesCidr(requestValue, v)),
            _ => true // Unknown operators are treated as non-restrictive
        };
    }

    private static string? GetConditionKeyValue(string conditionKey, PolicyEvaluationContext context)
    {
        return conditionKey switch
        {
            "aws:SourceIp" => context.SourceIp,
            "aws:SecureTransport" => context.IsSecureTransport ? "true" : "false",
            "s3:prefix" => context.Prefix,
            _ => null
        };
    }

    private static bool EvaluateBoolCondition(string conditionKey, IReadOnlyList<string> conditionValues, PolicyEvaluationContext context)
    {
        var actualValue = GetConditionKeyValue(conditionKey, context);
        if (actualValue is null) {
            return false;
        }

        return conditionValues.Any(v => string.Equals(v, actualValue, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Matches a pattern with * (any chars) and ? (single char) wildcards against a value.
    /// Case-insensitive for action matching, case-sensitive for resource matching.
    /// </summary>
    public static bool WildcardMatch(string pattern, string value)
    {
        var patternIndex = 0;
        var valueIndex = 0;
        var starPatternIndex = -1;
        var starValueIndex = -1;

        while (valueIndex < value.Length) {
            if (patternIndex < pattern.Length && (pattern[patternIndex] == '?' || char.ToUpperInvariant(pattern[patternIndex]) == char.ToUpperInvariant(value[valueIndex]))) {
                patternIndex++;
                valueIndex++;
            }
            else if (patternIndex < pattern.Length && pattern[patternIndex] == '*') {
                starPatternIndex = patternIndex;
                starValueIndex = valueIndex;
                patternIndex++;
            }
            else if (starPatternIndex >= 0) {
                patternIndex = starPatternIndex + 1;
                starValueIndex++;
                valueIndex = starValueIndex;
            }
            else {
                return false;
            }
        }

        while (patternIndex < pattern.Length && pattern[patternIndex] == '*') {
            patternIndex++;
        }

        return patternIndex == pattern.Length;
    }

    /// <summary>
    /// Checks whether an IP address falls within a CIDR range (e.g. "192.168.1.0/24").
    /// </summary>
    public static bool IpMatchesCidr(string ipString, string cidrString)
    {
        if (!IPAddress.TryParse(ipString, out var ip)) {
            return false;
        }

        var slashIndex = cidrString.IndexOf('/');
        if (slashIndex < 0) {
            return IPAddress.TryParse(cidrString, out var exactIp) && ip.Equals(exactIp);
        }

        if (!IPAddress.TryParse(cidrString[..slashIndex], out var networkAddress)) {
            return false;
        }

        if (!int.TryParse(cidrString[(slashIndex + 1)..], out var prefixLength) || prefixLength < 0) {
            return false;
        }

        var networkBytes = networkAddress.GetAddressBytes();
        var ipBytes = ip.GetAddressBytes();

        if (networkBytes.Length != ipBytes.Length) {
            return false;
        }

        var maxBits = networkBytes.Length * 8;
        if (prefixLength > maxBits) {
            return false;
        }

        var fullBytes = prefixLength / 8;
        for (var i = 0; i < fullBytes; i++) {
            if (networkBytes[i] != ipBytes[i]) {
                return false;
            }
        }

        if (fullBytes < networkBytes.Length) {
            var remainingBits = prefixLength % 8;
            if (remainingBits > 0) {
                var mask = (byte)(0xFF << (8 - remainingBits));
                if ((networkBytes[fullBytes] & mask) != (ipBytes[fullBytes] & mask)) {
                    return false;
                }
            }
        }

        return true;
    }
}

/// <summary>
/// Parses JSON bucket policy documents into <see cref="ParsedBucketPolicy"/>.
/// </summary>
public static class BucketPolicyParser
{
    /// <summary>
    /// Attempts to parse a JSON bucket policy document.
    /// </summary>
    public static bool TryParse(string json, out ParsedBucketPolicy? policy, out string? error)
    {
        policy = null;
        error = null;

        if (string.IsNullOrWhiteSpace(json)) {
            error = "Policy document is empty.";
            return false;
        }

        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            root.TryGetProperty("Version", out var versionElement);
            root.TryGetProperty("Id", out var idElement);

            var version = versionElement.ValueKind == JsonValueKind.String ? versionElement.GetString() : null;
            var id = idElement.ValueKind == JsonValueKind.String ? idElement.GetString() : null;

            if (!root.TryGetProperty("Statement", out var statementsElement) || statementsElement.ValueKind != JsonValueKind.Array) {
                error = "Policy document must contain a Statement array.";
                return false;
            }

            var statements = new List<PolicyStatement>();
            foreach (var stmtElement in statementsElement.EnumerateArray()) {
                if (!TryParseStatement(stmtElement, out var statement, out error)) {
                    return false;
                }

                statements.Add(statement!);
            }

            policy = new ParsedBucketPolicy
            {
                Version = version,
                Id = id,
                Statements = statements
            };
            return true;
        }
        catch (JsonException ex) {
            error = $"Invalid JSON in policy document: {ex.Message}";
            return false;
        }
    }

    private static bool TryParseStatement(JsonElement element, out PolicyStatement? statement, out string? error)
    {
        statement = null;
        error = null;

        // Effect (required)
        if (!element.TryGetProperty("Effect", out var effectElement) || effectElement.ValueKind != JsonValueKind.String) {
            error = "Each statement must have an Effect (Allow or Deny).";
            return false;
        }

        if (!Enum.TryParse<PolicyEffect>(effectElement.GetString(), ignoreCase: true, out var effect)) {
            error = $"Invalid Effect value: {effectElement.GetString()}. Must be 'Allow' or 'Deny'.";
            return false;
        }

        // Principal (required)
        var principals = ParseStringOrArray(element, "Principal");
        if (principals.Count == 0) {
            // Check for nested AWS key
            if (element.TryGetProperty("Principal", out var principalElement) && principalElement.ValueKind == JsonValueKind.Object) {
                if (principalElement.TryGetProperty("AWS", out var awsElement)) {
                    principals = ParseStringOrArrayFromElement(awsElement);
                }
            }
        }

        if (principals.Count == 0) {
            principals = ["*"];
        }

        // Action (required)
        var actions = ParseStringOrArray(element, "Action");
        if (actions.Count == 0) {
            error = "Each statement must have at least one Action.";
            return false;
        }

        // Resource (required)
        var resources = ParseStringOrArray(element, "Resource");
        if (resources.Count == 0) {
            error = "Each statement must have at least one Resource.";
            return false;
        }

        // Sid (optional)
        element.TryGetProperty("Sid", out var sidElement);
        var sid = sidElement.ValueKind == JsonValueKind.String ? sidElement.GetString() : null;

        // Condition (optional)
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, IReadOnlyList<string>>>? conditions = null;
        if (element.TryGetProperty("Condition", out var conditionElement) && conditionElement.ValueKind == JsonValueKind.Object) {
            conditions = ParseConditions(conditionElement);
        }

        statement = new PolicyStatement
        {
            Sid = sid,
            Effect = effect,
            Principals = principals,
            Actions = actions,
            Resources = resources,
            Conditions = conditions
        };
        return true;
    }

    private static IReadOnlyList<string> ParseStringOrArray(JsonElement parent, string propertyName)
    {
        if (!parent.TryGetProperty(propertyName, out var element)) {
            return [];
        }

        return ParseStringOrArrayFromElement(element);
    }

    private static IReadOnlyList<string> ParseStringOrArrayFromElement(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.String) {
            var value = element.GetString();
            return value is not null ? [value] : [];
        }

        if (element.ValueKind == JsonValueKind.Array) {
            var list = new List<string>();
            foreach (var item in element.EnumerateArray()) {
                if (item.ValueKind == JsonValueKind.String) {
                    var value = item.GetString();
                    if (value is not null) {
                        list.Add(value);
                    }
                }
            }

            return list;
        }

        return [];
    }

    private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, IReadOnlyList<string>>> ParseConditions(JsonElement conditionElement)
    {
        var conditions = new Dictionary<string, IReadOnlyDictionary<string, IReadOnlyList<string>>>(StringComparer.Ordinal);

        foreach (var operatorProperty in conditionElement.EnumerateObject()) {
            var conditionKeys = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);

            if (operatorProperty.Value.ValueKind == JsonValueKind.Object) {
                foreach (var keyProperty in operatorProperty.Value.EnumerateObject()) {
                    conditionKeys[keyProperty.Name] = ParseStringOrArrayFromElement(keyProperty.Value);
                }
            }

            if (conditionKeys.Count > 0) {
                conditions[operatorProperty.Name] = conditionKeys;
            }
        }

        return conditions;
    }
}

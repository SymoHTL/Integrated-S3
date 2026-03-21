using IntegratedS3.Core.Models;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class BucketPolicyEvaluatorTests
{
    [Fact]
    public void AllowStatement_MatchesGetObject()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:GetObject"],
                    Resources = ["arn:aws:s3:::my-bucket/*"]
                }
            ]
        };

        var result = BucketPolicyEvaluator.Evaluate(
            policy, "my-bucket", StorageOperationType.GetObject, "test.txt", null, null);

        Assert.Equal(PolicyEvaluationResult.Allow, result);
    }

    [Fact]
    public void DenyStatement_TakesPrecedenceOverAllow()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:GetObject"],
                    Resources = ["arn:aws:s3:::my-bucket/*"]
                },
                new PolicyStatement
                {
                    Effect = PolicyEffect.Deny,
                    Principals = ["*"],
                    Actions = ["s3:GetObject"],
                    Resources = ["arn:aws:s3:::my-bucket/secret/*"]
                }
            ]
        };

        // Allowed path
        var allowedResult = BucketPolicyEvaluator.Evaluate(
            policy, "my-bucket", StorageOperationType.GetObject, "public/file.txt", null, null);
        Assert.Equal(PolicyEvaluationResult.Allow, allowedResult);

        // Denied path — deny takes precedence
        var deniedResult = BucketPolicyEvaluator.Evaluate(
            policy, "my-bucket", StorageOperationType.GetObject, "secret/file.txt", null, null);
        Assert.Equal(PolicyEvaluationResult.ExplicitDeny, deniedResult);
    }

    [Fact]
    public void ImplicitDeny_WhenNoStatementMatches()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:GetObject"],
                    Resources = ["arn:aws:s3:::other-bucket/*"]
                }
            ]
        };

        var result = BucketPolicyEvaluator.Evaluate(
            policy, "my-bucket", StorageOperationType.GetObject, "test.txt", null, null);

        Assert.Equal(PolicyEvaluationResult.ImplicitDeny, result);
    }

    [Fact]
    public void WildcardAction_MatchesAllS3Actions()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:*"],
                    Resources = ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"]
                }
            ]
        };

        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetObject, "test.txt", null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.PutObject, "test.txt", null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.ListObjects, null, null, null));
    }

    [Fact]
    public void PutObject_ActionIsSupported()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:PutObject"],
                    Resources = ["arn:aws:s3:::my-bucket/*"]
                }
            ]
        };

        var result = BucketPolicyEvaluator.Evaluate(
            policy, "my-bucket", StorageOperationType.PutObject, "upload.txt", null, null);

        Assert.Equal(PolicyEvaluationResult.Allow, result);
    }

    [Fact]
    public void DeleteObject_ActionIsSupported()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:DeleteObject"],
                    Resources = ["arn:aws:s3:::my-bucket/*"]
                }
            ]
        };

        var result = BucketPolicyEvaluator.Evaluate(
            policy, "my-bucket", StorageOperationType.DeleteObject, "file.txt", null, null);

        Assert.Equal(PolicyEvaluationResult.Allow, result);
    }

    [Fact]
    public void BucketLevelActions_AreSupported()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:GetBucketLocation", "s3:ListBucketVersions", "s3:ListBucketMultipartUploads",
                               "s3:PutBucketPolicy", "s3:GetBucketPolicy", "s3:DeleteBucketPolicy",
                               "s3:PutBucketCors", "s3:GetBucketCors"],
                    Resources = ["arn:aws:s3:::my-bucket"]
                }
            ]
        };

        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetBucketLocation, null, null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.ListObjectVersions, null, null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.ListMultipartUploads, null, null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.PutBucketPolicy, null, null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetBucketPolicy, null, null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.DeleteBucketPolicy, null, null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.PutBucketCors, null, null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetBucketCors, null, null, null));
    }

    [Fact]
    public void Condition_StringEquals_MatchesPrefix()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:ListBucket"],
                    Resources = ["arn:aws:s3:::my-bucket"],
                    Conditions = new Dictionary<string, IReadOnlyDictionary<string, IReadOnlyList<string>>>
                    {
                        ["StringEquals"] = new Dictionary<string, IReadOnlyList<string>>
                        {
                            ["s3:prefix"] = ["logs/"]
                        }
                    }
                }
            ]
        };

        var matchContext = new PolicyEvaluationContext { Prefix = "logs/" };
        var noMatchContext = new PolicyEvaluationContext { Prefix = "data/" };

        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.ListObjects, null, null, matchContext));
        Assert.Equal(PolicyEvaluationResult.ImplicitDeny,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.ListObjects, null, null, noMatchContext));
    }

    [Fact]
    public void Condition_StringLike_MatchesWildcard()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:ListBucket"],
                    Resources = ["arn:aws:s3:::my-bucket"],
                    Conditions = new Dictionary<string, IReadOnlyDictionary<string, IReadOnlyList<string>>>
                    {
                        ["StringLike"] = new Dictionary<string, IReadOnlyList<string>>
                        {
                            ["s3:prefix"] = ["logs/*"]
                        }
                    }
                }
            ]
        };

        var matchContext = new PolicyEvaluationContext { Prefix = "logs/2024/data" };
        var noMatchContext = new PolicyEvaluationContext { Prefix = "data/2024" };

        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.ListObjects, null, null, matchContext));
        Assert.Equal(PolicyEvaluationResult.ImplicitDeny,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.ListObjects, null, null, noMatchContext));
    }

    [Fact]
    public void Condition_IpAddress_MatchesCidr()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:GetObject"],
                    Resources = ["arn:aws:s3:::my-bucket/*"],
                    Conditions = new Dictionary<string, IReadOnlyDictionary<string, IReadOnlyList<string>>>
                    {
                        ["IpAddress"] = new Dictionary<string, IReadOnlyList<string>>
                        {
                            ["aws:SourceIp"] = ["192.168.1.0/24"]
                        }
                    }
                }
            ]
        };

        var matchContext = new PolicyEvaluationContext { SourceIp = "192.168.1.50" };
        var noMatchContext = new PolicyEvaluationContext { SourceIp = "10.0.0.1" };

        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetObject, "test.txt", null, matchContext));
        Assert.Equal(PolicyEvaluationResult.ImplicitDeny,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetObject, "test.txt", null, noMatchContext));
    }

    [Fact]
    public void Condition_Bool_SecureTransport()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Deny,
                    Principals = ["*"],
                    Actions = ["s3:*"],
                    Resources = ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"],
                    Conditions = new Dictionary<string, IReadOnlyDictionary<string, IReadOnlyList<string>>>
                    {
                        ["Bool"] = new Dictionary<string, IReadOnlyList<string>>
                        {
                            ["aws:SecureTransport"] = ["false"]
                        }
                    }
                }
            ]
        };

        var httpContext = new PolicyEvaluationContext { IsSecureTransport = false };
        var httpsContext = new PolicyEvaluationContext { IsSecureTransport = true };

        // HTTP should be denied
        Assert.Equal(PolicyEvaluationResult.ExplicitDeny,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetObject, "test.txt", null, httpContext));
        // HTTPS should not match the deny condition
        Assert.Equal(PolicyEvaluationResult.ImplicitDeny,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetObject, "test.txt", null, httpsContext));
    }

    [Fact]
    public void WildcardMatch_HandlesPatterns()
    {
        Assert.True(BucketPolicyEvaluator.WildcardMatch("arn:aws:s3:::bucket/*", "arn:aws:s3:::bucket/any/path/here"));
        Assert.True(BucketPolicyEvaluator.WildcardMatch("arn:aws:s3:::bucket/prefix*", "arn:aws:s3:::bucket/prefix-something"));
        Assert.True(BucketPolicyEvaluator.WildcardMatch("*", "anything"));
        Assert.True(BucketPolicyEvaluator.WildcardMatch("s3:Get*", "s3:GetObject"));
        Assert.True(BucketPolicyEvaluator.WildcardMatch("s3:?etObject", "s3:GetObject"));
        Assert.False(BucketPolicyEvaluator.WildcardMatch("arn:aws:s3:::other-bucket/*", "arn:aws:s3:::bucket/file"));
        Assert.False(BucketPolicyEvaluator.WildcardMatch("s3:PutObject", "s3:GetObject"));
    }

    [Fact]
    public void IpMatchesCidr_HandlesVariousRanges()
    {
        Assert.True(BucketPolicyEvaluator.IpMatchesCidr("192.168.1.1", "192.168.1.0/24"));
        Assert.True(BucketPolicyEvaluator.IpMatchesCidr("192.168.1.255", "192.168.1.0/24"));
        Assert.False(BucketPolicyEvaluator.IpMatchesCidr("192.168.2.1", "192.168.1.0/24"));
        Assert.True(BucketPolicyEvaluator.IpMatchesCidr("10.0.0.1", "10.0.0.0/8"));
        Assert.True(BucketPolicyEvaluator.IpMatchesCidr("10.255.255.255", "10.0.0.0/8"));
        Assert.False(BucketPolicyEvaluator.IpMatchesCidr("11.0.0.1", "10.0.0.0/8"));
        Assert.True(BucketPolicyEvaluator.IpMatchesCidr("1.2.3.4", "1.2.3.4"));
        Assert.False(BucketPolicyEvaluator.IpMatchesCidr("invalid", "192.168.1.0/24"));
    }

    [Fact]
    public void PolicyParser_ParsesValidDocument()
    {
        var json = """
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicRead",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                }
            ]
        }
        """;

        Assert.True(BucketPolicyParser.TryParse(json, out var policy, out _));
        Assert.NotNull(policy);
        Assert.Equal("2012-10-17", policy.Version);
        Assert.Single(policy.Statements);
        Assert.Equal(PolicyEffect.Allow, policy.Statements[0].Effect);
        Assert.Equal("s3:GetObject", policy.Statements[0].Actions[0]);
    }

    [Fact]
    public void PolicyParser_ParsesDenyWithConditions()
    {
        var json = """
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": ["s3:*"],
                    "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"],
                    "Condition": {
                        "Bool": {
                            "aws:SecureTransport": "false"
                        }
                    }
                }
            ]
        }
        """;

        Assert.True(BucketPolicyParser.TryParse(json, out var policy, out _));
        Assert.NotNull(policy);
        Assert.Equal(PolicyEffect.Deny, policy.Statements[0].Effect);
        Assert.NotNull(policy.Statements[0].Conditions);
        Assert.True(policy.Statements[0].Conditions.ContainsKey("Bool"));
    }

    [Fact]
    public void PolicyParser_ParsesNestedAwsPrincipal()
    {
        var json = """
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": { "AWS": "arn:aws:iam::123456789012:root" },
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                }
            ]
        }
        """;

        Assert.True(BucketPolicyParser.TryParse(json, out var policy, out _));
        Assert.NotNull(policy);
        Assert.Contains("arn:aws:iam::123456789012:root", policy.Statements[0].Principals);
    }

    [Fact]
    public void PolicyParser_RejectsInvalidJson()
    {
        Assert.False(BucketPolicyParser.TryParse("not json", out _, out var error));
        Assert.NotNull(error);
    }

    [Fact]
    public void PolicyParser_RejectsEmptyDocument()
    {
        Assert.False(BucketPolicyParser.TryParse("", out _, out var error));
        Assert.NotNull(error);
    }

    [Fact]
    public void MultipartUpload_MapsToS3PutObject()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Allow,
                    Principals = ["*"],
                    Actions = ["s3:PutObject"],
                    Resources = ["arn:aws:s3:::my-bucket/*"]
                }
            ]
        };

        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.InitiateMultipartUpload, "large.bin", null, null));
        Assert.Equal(PolicyEvaluationResult.Allow,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.CompleteMultipartUpload, "large.bin", null, null));
    }

    [Fact]
    public void Condition_NotIpAddress_DeniesMatchingIp()
    {
        var policy = new ParsedBucketPolicy
        {
            Statements =
            [
                new PolicyStatement
                {
                    Effect = PolicyEffect.Deny,
                    Principals = ["*"],
                    Actions = ["s3:*"],
                    Resources = ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"],
                    Conditions = new Dictionary<string, IReadOnlyDictionary<string, IReadOnlyList<string>>>
                    {
                        ["NotIpAddress"] = new Dictionary<string, IReadOnlyList<string>>
                        {
                            ["aws:SourceIp"] = ["10.0.0.0/8"]
                        }
                    }
                }
            ]
        };

        // IP outside 10.0.0.0/8 should be denied
        var outsideIp = new PolicyEvaluationContext { SourceIp = "192.168.1.1" };
        Assert.Equal(PolicyEvaluationResult.ExplicitDeny,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetObject, "test.txt", null, outsideIp));

        // IP inside 10.0.0.0/8 should not match the deny condition
        var insideIp = new PolicyEvaluationContext { SourceIp = "10.1.2.3" };
        Assert.Equal(PolicyEvaluationResult.ImplicitDeny,
            BucketPolicyEvaluator.Evaluate(policy, "my-bucket", StorageOperationType.GetObject, "test.txt", null, insideIp));
    }
}

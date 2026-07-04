using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using IntegratedS3.Protocol;

namespace IntegratedS3.Benchmarks;

/// <summary>
/// Benchmarks the SigV4 / SigV4a signing hot paths: canonical-request construction,
/// string-to-sign, HMAC signature compute, and ECDSA key derivation / sign / verify.
/// All targets are pure functions in <see cref="IntegratedS3.Protocol"/>.
/// </summary>
[MemoryDiagnoser]
public class SigV4Benchmarks
{
    private const string HttpMethod = "PUT";
    private const string Path = "/example-bucket/path/to/object-name.bin";
    private const string SecretAccessKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    private const string AccessKeyId = "AKIAIOSFODNN7EXAMPLE";
    private const string PayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private static readonly string[] SignedHeaders = ["host", "x-amz-content-sha256", "x-amz-date"];
    private static readonly KeyValuePair<string, string?>[] QueryParameters = [];
    private KeyValuePair<string, string?>[] _headers = [];
    private DateTimeOffset _timestamp;
    private S3SigV4CredentialScope _scope = null!;
    private string _canonicalHash = string.Empty;
    private string _stringToSign = string.Empty;
    private string _scopeStringV4a = string.Empty;
    private string _stringToSignV4a = string.Empty;
    private ECDsa _ecdsaKey = null!;
    private string _signatureV4a = string.Empty;

    [GlobalSetup]
    public void Setup()
    {
        _timestamp = new DateTimeOffset(2026, 7, 4, 12, 0, 0, TimeSpan.Zero);
        _headers =
        [
            new("host", "example-bucket.s3.us-east-1.amazonaws.com"),
            new("x-amz-content-sha256", PayloadHash),
            new("x-amz-date", "20260704T120000Z"),
            new("content-length", "1048576"),
            new("content-type", "application/octet-stream"),
        ];

        _scope = new S3SigV4CredentialScope
        {
            AccessKeyId = AccessKeyId,
            DateStamp = "20260704",
            Region = "us-east-1",
            Service = "s3",
            Terminator = "aws4_request",
        };

        var canonical = S3SigV4Signer.BuildCanonicalRequest(HttpMethod, Path, QueryParameters, _headers, SignedHeaders, PayloadHash);
        _canonicalHash = canonical.CanonicalRequestHashHex;
        _stringToSign = S3SigV4Signer.BuildStringToSign("AWS4-HMAC-SHA256", _timestamp, _scope, _canonicalHash);

        _scopeStringV4a = S3SigV4aSigner.BuildCredentialScopeString("20260704", "s3");
        _stringToSignV4a = S3SigV4aSigner.BuildStringToSign(_timestamp, _scopeStringV4a, _canonicalHash);
        _ecdsaKey = S3SigV4aSigner.DeriveEcdsaKey(SecretAccessKey, AccessKeyId);
        _signatureV4a = S3SigV4aSigner.ComputeSignature(_ecdsaKey, _stringToSignV4a);
    }

    [GlobalCleanup]
    public void Cleanup() => _ecdsaKey?.Dispose();

    // ---- SigV4 (HMAC-SHA256) ----

    [Benchmark(Baseline = true)]
    public string SigV4_BuildCanonicalRequest()
        => S3SigV4Signer.BuildCanonicalRequest(HttpMethod, Path, QueryParameters, _headers, SignedHeaders, PayloadHash).CanonicalRequestHashHex;

    [Benchmark]
    public string SigV4_BuildStringToSign()
        => S3SigV4Signer.BuildStringToSign("AWS4-HMAC-SHA256", _timestamp, _scope, _canonicalHash);

    [Benchmark]
    public string SigV4_ComputeSignature()
        => S3SigV4Signer.ComputeSignature(SecretAccessKey, _scope, _stringToSign);

    [Benchmark]
    public string SigV4_FullSign()
    {
        var canonical = S3SigV4Signer.BuildCanonicalRequest(HttpMethod, Path, QueryParameters, _headers, SignedHeaders, PayloadHash);
        var stringToSign = S3SigV4Signer.BuildStringToSign("AWS4-HMAC-SHA256", _timestamp, _scope, canonical.CanonicalRequestHashHex);
        return S3SigV4Signer.ComputeSignature(SecretAccessKey, _scope, stringToSign);
    }

    // ---- SigV4a (ECDSA P-256) ----

    [Benchmark]
    public ECDsa SigV4a_DeriveEcdsaKey()
    {
        var key = S3SigV4aSigner.DeriveEcdsaKey(SecretAccessKey, AccessKeyId);
        key.Dispose();
        return key;
    }

    [Benchmark]
    public string SigV4a_ComputeSignature()
        => S3SigV4aSigner.ComputeSignature(_ecdsaKey, _stringToSignV4a);

    [Benchmark]
    public bool SigV4a_VerifySignature()
        => S3SigV4aSigner.VerifySignature(_ecdsaKey, _stringToSignV4a, _signatureV4a);
}

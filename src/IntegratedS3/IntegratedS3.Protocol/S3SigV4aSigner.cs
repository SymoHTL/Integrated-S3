using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using IntegratedS3.Protocol.Internal;

namespace IntegratedS3.Protocol;

/// <summary>
/// Implements AWS Signature Version 4a (SigV4a) signing operations using ECDSA P-256.
/// Provides key derivation, signature computation/verification, and string-to-sign construction
/// for standard, streaming payload, and streaming trailer requests.
/// </summary>
public static class S3SigV4aSigner
{
    /// <summary>
    /// The algorithm identifier used in SigV4a Authorization headers and string-to-sign construction.
    /// </summary>
    public const string Algorithm = "AWS4-ECDSA-P256-SHA256";
    private const string StreamingPayloadAlgorithm = "AWS4-ECDSA-P256-SHA256-PAYLOAD";
    private const string StreamingTrailerAlgorithm = "AWS4-ECDSA-P256-SHA256-TRAILER";

    // P-256 curve order
    private static readonly byte[] CurveOrderBytes =
    [
        0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xBC, 0xE6, 0xFA, 0xAD, 0xA7, 0x17, 0x9E, 0x84,
        0xF3, 0xB9, 0xCA, 0xC2, 0xFC, 0x63, 0x25, 0x51
    ];

    /// <summary>
    /// Derives an ECDSA P-256 key pair from an AWS secret access key and access key ID
    /// using NIST SP 800-108 counter-mode KDF with HMAC-SHA256.
    /// </summary>
    public static ECDsa DeriveEcdsaKey(string secretAccessKey, string accessKeyId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(secretAccessKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(accessKeyId);

        var fixedInputKey = Encoding.UTF8.GetBytes($"AWS4A{secretAccessKey}");
        var label = Encoding.UTF8.GetBytes(Algorithm);
        var context = Encoding.UTF8.GetBytes(accessKeyId);

        // NIST SP 800-108 fixed input: counter(4) || label || 0x00 || context || L(4)
        // L = 256 bits = key length
        var fixedInput = new byte[4 + label.Length + 1 + context.Length + 4];
        Buffer.BlockCopy(label, 0, fixedInput, 4, label.Length);
        fixedInput[4 + label.Length] = 0x00;
        Buffer.BlockCopy(context, 0, fixedInput, 4 + label.Length + 1, context.Length);
        // L = 256 in big-endian
        fixedInput[fixedInput.Length - 4] = 0;
        fixedInput[fixedInput.Length - 3] = 0;
        fixedInput[fixedInput.Length - 2] = 1;
        fixedInput[fixedInput.Length - 1] = 0;

        var curveOrder = new BigInteger(CurveOrderBytes, isUnsigned: true, isBigEndian: true);

        for (var counter = 1; counter <= 255; counter++)
        {
            fixedInput[0] = (byte)(counter >> 24);
            fixedInput[1] = (byte)(counter >> 16);
            fixedInput[2] = (byte)(counter >> 8);
            fixedInput[3] = (byte)counter;

            byte[] candidateBytes;
            using (var hmac = new HMACSHA256(fixedInputKey))
            {
                candidateBytes = hmac.ComputeHash(fixedInput);
            }

            var candidate = new BigInteger(candidateBytes, isUnsigned: true, isBigEndian: true);
            if (candidate >= 1 && candidate < curveOrder)
            {
                var ecParams = new ECParameters
                {
                    Curve = ECCurve.NamedCurves.nistP256,
                    D = candidateBytes
                };
                return ECDsa.Create(ecParams);
            }
        }

        ProtocolTelemetry.RecordSignatureError("sigv4a-key-derivation", "Failed to derive valid P-256 private key within 255 iterations");
        throw new CryptographicException("Failed to derive a valid ECDSA P-256 private key within 255 counter iterations.");
    }

    /// <summary>
    /// Computes an ECDSA P-256 signature over the string-to-sign, returning a 128-char hex string.
    /// The signature is encoded as r(32 bytes) || s(32 bytes) in big-endian, then hex-encoded.
    /// </summary>
    public static string ComputeSignature(ECDsa ecdsaKey, string stringToSign)
    {
        ArgumentNullException.ThrowIfNull(ecdsaKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(stringToSign);

        var dataBytes = Encoding.UTF8.GetBytes(stringToSign);
        var signatureBytes = ecdsaKey.SignData(dataBytes, HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation);
        return Convert.ToHexStringLower(signatureBytes);
    }

    /// <summary>
    /// Verifies an ECDSA P-256 signature over the string-to-sign.
    /// The signature must be a 128-char hex string (r || s, each 32 bytes).
    /// </summary>
    public static bool VerifySignature(ECDsa ecdsaKey, string stringToSign, string signatureHex)
    {
        ArgumentNullException.ThrowIfNull(ecdsaKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(stringToSign);
        ArgumentException.ThrowIfNullOrWhiteSpace(signatureHex);

        var dataBytes = Encoding.UTF8.GetBytes(stringToSign);
        byte[] signatureBytes;
        try
        {
            signatureBytes = Convert.FromHexString(signatureHex);
        }
        catch (FormatException)
        {
            ProtocolTelemetry.RecordSignatureError("sigv4a-verify", "Malformed signature hex");
            return false;
        }
        return ecdsaKey.VerifyData(dataBytes, signatureBytes, HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation);
    }

    /// <summary>
    /// Builds the string-to-sign for SigV4a requests.
    /// Uses the SigV4a credential scope format: dateStamp/service/aws4_request (no region).
    /// </summary>
    public static string BuildStringToSign(DateTimeOffset requestTimestampUtc, string credentialScopeString, string canonicalRequestHashHex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(credentialScopeString);
        ArgumentException.ThrowIfNullOrWhiteSpace(canonicalRequestHashHex);

        return string.Join('\n',
        [
            Algorithm,
            requestTimestampUtc.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'"),
            credentialScopeString,
            canonicalRequestHashHex
        ]);
    }

    /// <summary>
    /// Builds the string-to-sign for SigV4a streaming payload chunks.
    /// </summary>
    public static string BuildStreamingPayloadStringToSign(
        DateTimeOffset requestTimestampUtc,
        string credentialScopeString,
        string previousSignature,
        string chunkContentHashHex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(credentialScopeString);
        ArgumentException.ThrowIfNullOrWhiteSpace(previousSignature);
        ArgumentException.ThrowIfNullOrWhiteSpace(chunkContentHashHex);

        return string.Join('\n',
        [
            StreamingPayloadAlgorithm,
            requestTimestampUtc.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'"),
            credentialScopeString,
            previousSignature,
            S3SigV4Signer.ComputeSha256Hex(string.Empty),
            chunkContentHashHex
        ]);
    }

    /// <summary>
    /// Builds the string-to-sign for SigV4a streaming trailer.
    /// </summary>
    public static string BuildStreamingTrailerStringToSign(
        DateTimeOffset requestTimestampUtc,
        string credentialScopeString,
        string previousSignature,
        string trailerHeadersHashHex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(credentialScopeString);
        ArgumentException.ThrowIfNullOrWhiteSpace(previousSignature);
        ArgumentException.ThrowIfNullOrWhiteSpace(trailerHeadersHashHex);

        return string.Join('\n',
        [
            StreamingTrailerAlgorithm,
            requestTimestampUtc.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'"),
            credentialScopeString,
            previousSignature,
            trailerHeadersHashHex
        ]);
    }

    /// <summary>
    /// Builds the SigV4a credential scope string (no region).
    /// Format: dateStamp/service/aws4_request
    /// </summary>
    public static string BuildCredentialScopeString(string dateStamp, string service)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dateStamp);
        ArgumentException.ThrowIfNullOrWhiteSpace(service);
        return $"{dateStamp}/{service}/aws4_request";
    }
}

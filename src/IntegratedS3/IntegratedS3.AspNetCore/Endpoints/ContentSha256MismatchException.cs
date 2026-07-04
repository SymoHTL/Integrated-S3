namespace IntegratedS3.AspNetCore.Endpoints;

/// <summary>
/// Thrown when a request carries a concrete (non-sentinel) <c>x-amz-content-sha256</c> payload hash
/// that does not match the SHA256 of the received request body. Maps to the S3
/// <c>XAmzContentSHA256Mismatch</c> error (HTTP 400).
/// </summary>
internal sealed class ContentSha256MismatchException()
    : Exception("The provided 'x-amz-content-sha256' header does not match what was computed.")
{
}

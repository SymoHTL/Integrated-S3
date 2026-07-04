namespace IntegratedS3.AspNetCore.Endpoints;

/// <summary>
/// Thrown while decoding a signed <c>aws-chunked</c> streaming body when a chunk's
/// per-chunk SigV4/SigV4a signature (the <c>;chunk-signature=</c> extension) does not match the
/// signature recomputed by the server over the chunk bytes and the rolling signature chain.
/// This is what cryptographically binds the uploaded body bytes to the authenticated request
/// signature. Maps to the S3 <c>SignatureDoesNotMatch</c> error (HTTP 403).
/// </summary>
internal sealed class ChunkSignatureMismatchException()
    : Exception("The request signature we calculated does not match the signature you provided.")
{
}

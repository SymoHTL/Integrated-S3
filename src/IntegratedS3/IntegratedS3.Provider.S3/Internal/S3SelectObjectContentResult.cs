namespace IntegratedS3.Provider.S3.Internal;

internal sealed record S3SelectObjectContentResult(
    Stream EventStream,
    string? ContentType);

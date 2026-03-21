namespace IntegratedS3.Provider.S3.Internal;

internal sealed record S3RestoreObjectResult(
    bool IsAlreadyRestored,
    string? RestoreOutputPath);

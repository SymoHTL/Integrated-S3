using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

/// <summary>Represents the versioning configuration for an S3 bucket.</summary>
internal sealed record S3VersioningEntry(BucketVersioningStatus Status);

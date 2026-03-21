namespace IntegratedS3.AspNetCore.Maintenance;

/// <summary>
/// Well-known maintenance job name constants for IntegratedS3.
/// Use these constants when registering or configuring built-in maintenance jobs.
/// </summary>
public static class IntegratedS3MaintenanceJobNames
{
    /// <summary>
    /// Job that replays queued mirror operations to synchronize data across storage backends.
    /// </summary>
    public const string MirrorReplay = "mirror-replay";

    /// <summary>
    /// Job that detects orphaned objects that are no longer referenced by the catalog.
    /// </summary>
    public const string OrphanDetection = "orphan-detection";

    /// <summary>
    /// Job that verifies stored object checksums against their expected values.
    /// </summary>
    public const string ChecksumVerification = "checksum-verification";

    /// <summary>
    /// Job that cleans up incomplete or abandoned multipart uploads.
    /// </summary>
    public const string MultipartCleanup = "multipart-cleanup";

    /// <summary>
    /// Job that compacts internal indexes to reclaim space and improve query performance.
    /// </summary>
    public const string IndexCompaction = "index-compaction";

    /// <summary>
    /// Job that removes artifacts whose retention period has expired.
    /// </summary>
    public const string ExpiredArtifactCleanup = "expired-artifact-cleanup";
}

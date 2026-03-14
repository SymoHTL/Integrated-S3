namespace IntegratedS3.AspNetCore.Maintenance;

public static class IntegratedS3MaintenanceJobNames
{
    public const string MirrorReplay = "mirror-replay";

    public const string OrphanDetection = "orphan-detection";

    public const string ChecksumVerification = "checksum-verification";

    public const string MultipartCleanup = "multipart-cleanup";

    public const string IndexCompaction = "index-compaction";

    public const string ExpiredArtifactCleanup = "expired-artifact-cleanup";
}

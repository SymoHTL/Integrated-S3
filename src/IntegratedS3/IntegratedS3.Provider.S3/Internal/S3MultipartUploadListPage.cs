using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

internal sealed record S3MultipartUploadListPage(
    IReadOnlyList<MultipartUploadInfo> Entries,
    string? NextKeyMarker,
    string? NextUploadIdMarker);

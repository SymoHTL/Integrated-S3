using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

internal sealed record S3MultipartUploadPartListPage(
    IReadOnlyList<MultipartUploadPart> Entries,
    int? NextPartNumberMarker);

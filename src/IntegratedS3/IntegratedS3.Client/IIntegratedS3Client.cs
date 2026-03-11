using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

public interface IIntegratedS3Client
{
    ValueTask<StoragePresignedRequest> PresignObjectAsync(
        StoragePresignRequest request,
        CancellationToken cancellationToken = default);
}

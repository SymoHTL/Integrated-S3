namespace IntegratedS3.Core.Models;

public enum StorageAccessMode
{
    /// <summary>
    /// The presigned URL targets the IntegratedS3 host, which proxies the request to the underlying storage provider.
    /// </summary>
    Proxy,

    /// <summary>
    /// The presigned URL targets the underlying storage location directly without proxying through the
    /// IntegratedS3 host. The backend may supply a plain URL or provider-native signing data as needed.
    /// </summary>
    Direct,

    /// <summary>
    /// The access grant is delegated by an external or provider-managed flow and is passed through by the
    /// IntegratedS3 host rather than minted by the primary proxy/direct presign path.
    /// </summary>
    Delegated
}

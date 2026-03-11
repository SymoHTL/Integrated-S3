namespace IntegratedS3.Abstractions.Models;

public enum StorageObjectAccessMode
{
    ProxyStream,
    Redirect,
    Delegated,
    Passthrough
}

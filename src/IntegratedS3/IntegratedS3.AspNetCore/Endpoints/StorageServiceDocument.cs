using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.AspNetCore.Endpoints;

/// <summary>
/// JSON-serializable document representing the IntegratedS3 service and its providers in API responses.
/// </summary>
public sealed class StorageServiceDocument
{
    /// <summary>
    /// Gets the name of the IntegratedS3 service.
    /// </summary>
    public string ServiceName { get; init; } = string.Empty;

    /// <summary>
    /// Gets the array of storage provider documents available in this service.
    /// </summary>
    public StorageProviderDocument[] Providers { get; init; } = [];

    /// <summary>
    /// Gets the aggregate capabilities of the service across all providers.
    /// </summary>
    public StorageCapabilities Capabilities { get; init; } = new();

    /// <summary>
    /// Creates a <see cref="StorageServiceDocument"/> from a <see cref="StorageServiceDescriptor"/>.
    /// </summary>
    /// <param name="descriptor">The storage service descriptor to convert.</param>
    /// <returns>A new <see cref="StorageServiceDocument"/> populated from the descriptor.</returns>
    public static StorageServiceDocument FromDescriptor(StorageServiceDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);

        return new StorageServiceDocument
        {
            ServiceName = descriptor.ServiceName,
            Providers = descriptor.Providers.Select(StorageProviderDocument.FromDescriptor).ToArray(),
            Capabilities = descriptor.Capabilities
        };
    }
}

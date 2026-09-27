namespace IntegratedS3.Shared;

/// <summary>
/// Mints S3 object version ids.
/// </summary>
internal static class ObjectVersionIds
{
    internal static string Create()
    {
        return Guid.CreateVersion7().ToString("N");
    }
}

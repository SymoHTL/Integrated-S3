namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Specifies whether to preserve or replace metadata during a copy operation.
/// </summary>
public enum CopyObjectMetadataDirective
{
    /// <summary>
    /// Preserve the metadata from the source object.
    /// </summary>
    Copy,

    /// <summary>
    /// Use the metadata supplied in the copy request.
    /// </summary>
    Replace
}

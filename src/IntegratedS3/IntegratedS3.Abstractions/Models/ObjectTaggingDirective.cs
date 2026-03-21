namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Specifies whether to preserve or replace tags during a copy operation.
/// </summary>
public enum ObjectTaggingDirective
{
    /// <summary>
    /// Preserve the tags from the source object.
    /// </summary>
    Copy = 0,

    /// <summary>
    /// Use the tags supplied in the copy request.
    /// </summary>
    Replace = 1
}

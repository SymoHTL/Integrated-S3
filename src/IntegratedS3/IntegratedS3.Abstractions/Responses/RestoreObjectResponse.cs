namespace IntegratedS3.Abstractions.Responses;

/// <summary>Result of a Glacier object restore request.</summary>
public sealed class RestoreObjectResponse
{
    /// <summary>
    /// True when the object is already restored (HTTP 200); false when
    /// a restore job has been accepted (HTTP 202).
    /// </summary>
    public bool IsAlreadyRestored { get; init; }

    /// <summary>The output path for the restored object, if applicable.</summary>
    public string? RestoreOutputPath { get; init; }
}

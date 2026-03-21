namespace IntegratedS3.Core.Services;

/// <summary>
/// Provides access to the <see cref="IntegratedS3RequestContext"/> for the current operation.
/// </summary>
public interface IIntegratedS3RequestContextAccessor
{
    /// <summary>
    /// Gets or sets the <see cref="IntegratedS3RequestContext"/> associated with the current operation,
    /// or <see langword="null"/> if no context has been established.
    /// </summary>
    IntegratedS3RequestContext? Current { get; set; }
}
using System.Security.Claims;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Captures per-request context for an IntegratedS3 operation, including the caller identity
/// and optional correlation metadata.
/// </summary>
public sealed class IntegratedS3RequestContext
{
    /// <summary>
    /// Gets the <see cref="ClaimsPrincipal"/> representing the caller for the current request.
    /// Defaults to an anonymous principal when not explicitly set.
    /// </summary>
    public ClaimsPrincipal Principal { get; init; } = new(new ClaimsIdentity());

    /// <summary>
    /// Gets the correlation identifier used to group related operations across services,
    /// or <see langword="null"/> if not provided.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets the unique request identifier for the current operation,
    /// or <see langword="null"/> if not provided.
    /// </summary>
    public string? RequestId { get; init; }
}

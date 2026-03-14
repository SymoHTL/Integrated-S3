namespace IntegratedS3.AspNetCore;

/// <summary>
/// Configures a static access key that the ASP.NET host can use for Signature Version 4 authentication.
/// </summary>
public sealed class IntegratedS3AccessKeyCredential
{
    /// <summary>The configured access key identifier.</summary>
    public string AccessKeyId { get; set; } = string.Empty;

    /// <summary>The secret access key paired with <see cref="AccessKeyId"/>.</summary>
    public string SecretAccessKey { get; set; } = string.Empty;

    /// <summary>An optional display name for diagnostics or administrative views.</summary>
    public string? DisplayName { get; set; }

    /// <summary>The authorization scopes associated with this credential.</summary>
    public List<string> Scopes { get; set; } = [];
}

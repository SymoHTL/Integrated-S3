namespace IntegratedS3.AspNetCore;

public sealed class IntegratedS3AccessKeyCredential
{
    public string AccessKeyId { get; set; } = string.Empty;

    public string SecretAccessKey { get; set; } = string.Empty;

    public string? DisplayName { get; set; }

    public List<string> Scopes { get; set; } = [];
}

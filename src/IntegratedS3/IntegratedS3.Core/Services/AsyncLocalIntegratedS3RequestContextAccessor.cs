namespace IntegratedS3.Core.Services;

internal sealed class AsyncLocalIntegratedS3RequestContextAccessor : IIntegratedS3RequestContextAccessor
{
    private static readonly AsyncLocal<IntegratedS3RequestContext?> CurrentContext = new();

    public IntegratedS3RequestContext? Current
    {
        get => CurrentContext.Value;
        set => CurrentContext.Value = value;
    }
}
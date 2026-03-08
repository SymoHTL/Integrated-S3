namespace IntegratedS3.Core.Services;

public interface IIntegratedS3RequestContextAccessor
{
    IntegratedS3RequestContext? Current { get; set; }
}
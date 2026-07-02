var builder = WebApplication.CreateSlimBuilder(args);
// The reference host intentionally uses configuration binding that the trim/AOT analyzers flag;
// the publish-time posture is enforced by the eng/Invoke-AotPublishValidation.ps1 warning baseline.
#pragma warning disable IL2026, IL3050
WebUiApplication.ConfigureServices(builder);
#pragma warning restore IL2026, IL3050

var app = builder.Build();
WebUiApplication.ConfigurePipeline(app);

app.Run();

public partial class Program;

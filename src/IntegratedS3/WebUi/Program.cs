var builder = WebApplication.CreateSlimBuilder(args);
WebUiApplication.ConfigureServices(builder);

var app = builder.Build();
WebUiApplication.ConfigurePipeline(app);

app.Run();

public partial class Program;

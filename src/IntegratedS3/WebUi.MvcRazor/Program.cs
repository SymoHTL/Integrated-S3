var builder = WebApplication.CreateSlimBuilder(args);
WebUi.MvcRazor.MvcRazorApplication.ConfigureServices(builder);

var app = builder.Build();
WebUi.MvcRazor.MvcRazorApplication.ConfigurePipeline(app);

app.Run();

public partial class Program;

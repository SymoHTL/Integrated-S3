var builder = WebApplication.CreateSlimBuilder(args);
WebUi.BlazorWasm.BlazorWasmApplication.ConfigureServices(builder);

var app = builder.Build();
WebUi.BlazorWasm.BlazorWasmApplication.ConfigurePipeline(app);

app.Run();

public partial class Program;

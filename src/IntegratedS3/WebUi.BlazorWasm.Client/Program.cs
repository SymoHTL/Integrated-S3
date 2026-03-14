using IntegratedS3.Client;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using WebUi.BlazorWasm.Client;
using WebUi.BlazorWasm.Client.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");

builder.Services.AddScoped(sp => new HttpClient
{
    BaseAddress = new Uri(builder.HostEnvironment.BaseAddress)
});
builder.Services.AddScoped<IIntegratedS3Client>(sp => new IntegratedS3Client(sp.GetRequiredService<HttpClient>()));
builder.Services.AddScoped<IntegratedS3BrowserSampleService>();

await builder.Build().RunAsync();

using System.Text.Json;
using System.Text.Json.Serialization;
using IntegratedS3.Abstractions.Models;
using WebUi.BlazorWasm.Client.Models;

namespace WebUi.BlazorWasm.Client.Serialization;

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(BrowserSampleServiceDocument))]
[JsonSerializable(typeof(BrowserSampleProviderDocument[]))]
[JsonSerializable(typeof(BucketInfo[]))]
[JsonSerializable(typeof(ObjectInfo[]))]
internal partial class WebUiBlazorWasmClientJsonSerializerContext : JsonSerializerContext
{
}

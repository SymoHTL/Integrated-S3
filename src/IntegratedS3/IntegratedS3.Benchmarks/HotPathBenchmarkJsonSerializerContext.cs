using System.Text.Json.Serialization;

namespace IntegratedS3.Benchmarks;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(HotPathBenchmarkReport))]
internal sealed partial class HotPathBenchmarkJsonSerializerContext : JsonSerializerContext
{
}

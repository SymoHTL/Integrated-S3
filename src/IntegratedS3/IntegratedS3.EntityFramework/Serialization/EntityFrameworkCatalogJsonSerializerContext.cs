using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace IntegratedS3.EntityFramework.Serialization;

/// <summary>
/// Source-generated <see cref="JsonSerializerContext"/> for the JSON columns persisted by the EntityFramework
/// catalog/multipart stores (the <c>MetadataJson</c>, <c>TagsJson</c> and <c>ChecksumsJson</c> string columns).
/// Routing those (de)serialize calls through the generated <see cref="Default"/> instance keeps the stores off the
/// reflection-based <see cref="System.Text.Json.JsonSerializer"/> resolver, matching the source-generated pattern
/// used everywhere else in the solution (e.g. <c>DiskStorageJsonSerializerContext</c>).
/// </summary>
/// <remarks>
/// The generator is configured with default options (no property-name policy, no custom converters) so the emitted
/// JSON shape is byte-for-byte identical to the previous reflection-based path — existing stored rows still
/// round-trip. The stored value type is <see cref="Dictionary{TKey,TValue}"/>: the write side materializes a
/// concrete dictionary from the <c>IReadOnlyDictionary&lt;string, string&gt;</c> source properties, and the read
/// side already deserializes into <see cref="Dictionary{TKey,TValue}"/>.
/// </remarks>
[JsonSourceGenerationOptions]
[JsonSerializable(typeof(Dictionary<string, string>))]
internal partial class EntityFrameworkCatalogJsonSerializerContext : JsonSerializerContext
{
}

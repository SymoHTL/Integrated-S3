namespace IntegratedS3.Engine;

/// <summary>
/// Options for the IntegratedS3 engine: where it keeps its metadata and blobs, and the timings of its background
/// upkeep.
/// </summary>
public sealed class IntegratedS3EngineOptions
{
    /// <summary>
    /// Gets or sets the provider name the engine registers under. Defaults to <c>engine-primary</c>.
    /// </summary>
    public string ProviderName { get; set; } = "engine-primary";

    /// <summary>
    /// Gets or sets whether the engine is the primary backend. Defaults to <see langword="true"/>.
    /// </summary>
    public bool IsPrimary { get; set; } = true;

    /// <summary>
    /// Gets or sets the path of the SQLite metadata database, created with its directory when missing. Defaults to
    /// <c>App_Data/IntegratedS3Engine/metadata.db</c>. The database must not be shared by two processes.
    /// </summary>
    public string SqliteDatabasePath { get; set; } = Path.Combine("App_Data", "IntegratedS3Engine", "metadata.db");

    /// <summary>
    /// Gets or sets the root directory of the local disk blob store, used when no
    /// <see cref="Abstractions.Blobs.IBlobStore"/> is registered. Defaults to <c>App_Data/IntegratedS3Engine/blobs</c>.
    /// </summary>
    public string BlobRootPath { get; set; } = Path.Combine("App_Data", "IntegratedS3Engine", "blobs");

    /// <summary>
    /// Gets or sets the largest object body stored inside its metadata row instead of in a blob. A PUT or GET of
    /// such an object touches no blob at all. Defaults to 8 KiB; 0 turns inline storage off.
    /// </summary>
    public int InlineThresholdBytes { get; set; } = 8 * 1024;

    /// <summary>
    /// Gets or sets how long a blob that no committed row references any more is kept before garbage collection
    /// deletes it, so a GET that resolved it before the change can finish streaming. Defaults to one hour.
    /// </summary>
    public TimeSpan GarbageCollectionDelay { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Gets or sets how old an unreferenced blob must be before the orphan sweep deletes it. It bounds the longest
    /// upload: a body that takes longer to arrive fails at its commit. Defaults to 24 hours.
    /// </summary>
    public TimeSpan OrphanGracePeriod { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Gets or sets the interval of the background upkeep (garbage collection and one step of the orphan sweep).
    /// Defaults to 30 seconds; <see cref="TimeSpan.Zero"/> turns it off, which only tests should do.
    /// </summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromSeconds(30);
}

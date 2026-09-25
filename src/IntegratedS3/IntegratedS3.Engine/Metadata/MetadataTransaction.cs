using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace IntegratedS3.Engine.Metadata;

/// <summary>
/// One metadata transaction: the primitives the engine combines into one S3 operation. Disposing it without
/// <see cref="CommitAsync"/> rolls it back.
/// </summary>
/// <remarks>
/// Every write takes <see cref="LockBucketAsync"/> first and <see cref="LockKeyAsync"/> for the key it changes, so
/// that a store which runs writers concurrently serializes the same things. SQLite runs one writer at a time, so
/// both locks only read or create their row here.
/// </remarks>
internal sealed class MetadataTransaction : IAsyncDisposable
{
    private const string VersionColumns =
        "v.bucket_id, v.key, v.seq, v.version_id, v.is_delete_marker, v.size, v.etag, v.last_modified_us, v.storage_class, v.retain_until_us, v.legal_hold, v.meta";

    private readonly SqliteConnection _connection;
    private readonly SqliteTransaction _transaction;
    private SemaphoreSlim? _writeGate;
    private bool _committed;

    internal MetadataTransaction(SqliteConnection connection, SqliteTransaction transaction, SemaphoreSlim? writeGate)
    {
        _connection = connection;
        _transaction = transaction;
        _writeGate = writeGate;
    }

    public async ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        await _transaction.CommitAsync(cancellationToken);
        _committed = true;
    }

    public async ValueTask DisposeAsync()
    {
        try {
            if (!_committed) {
                await _transaction.RollbackAsync();
            }
        }
        catch (SqliteException) {
            // SQLite already rolled back after the error that brought us here.
        }
        catch (InvalidOperationException) {
            // The transaction completed or its connection broke; nothing is left to roll back.
        }
        finally {
            await _transaction.DisposeAsync();
            await _connection.DisposeAsync();
            _writeGate?.Release();
            _writeGate = null;
        }
    }

    /// <summary>
    /// Gets the time the transaction acts at, in microseconds since the Unix epoch. A cluster store reads it from
    /// the database after its locks are taken; SQLite runs on the node that asks.
    /// </summary>
    public ValueTask<long> GetClockAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(EngineClock.ToMicroseconds(DateTimeOffset.UtcNow));
    }

    // ----- Buckets -----

    /// <summary>
    /// Serializes this transaction against changes to the bucket's existence and versioning state: shared for an
    /// object write, exclusive for DeleteBucket and versioning changes. Keyed by name, so a bucket deleted and
    /// created again under the same name is covered too.
    /// </summary>
    public ValueTask LockBucketAsync(string bucketName, bool exclusive, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<BucketRow?> GetBucketAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var command = Command("SELECT id, name, versioning, object_lock, created_us FROM buckets WHERE name = @name;");
        command.Parameters.AddWithValue("@name", name);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadBucket(reader) : null;
    }

    public async ValueTask<List<BucketRow>> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        await using var command = Command("SELECT id, name, versioning, object_lock, created_us FROM buckets ORDER BY name;");
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var buckets = new List<BucketRow>();
        while (await reader.ReadAsync(cancellationToken)) {
            buckets.Add(ReadBucket(reader));
        }

        return buckets;
    }

    /// <summary>
    /// Inserts a bucket. Returns <see langword="null"/> when a bucket with the name exists: the unique name decides
    /// which of two racing creates wins.
    /// </summary>
    public async ValueTask<BucketRow?> InsertBucketAsync(string name, StoredVersioning versioning, bool objectLockEnabled, long createdUs, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            INSERT INTO buckets (name, versioning, object_lock, created_us) VALUES (@name, @versioning, @lock, @created)
            ON CONFLICT (name) DO NOTHING
            RETURNING id;
            """);
        command.Parameters.AddWithValue("@name", name);
        command.Parameters.AddWithValue("@versioning", (int)versioning);
        command.Parameters.AddWithValue("@lock", objectLockEnabled);
        command.Parameters.AddWithValue("@created", createdUs);
        var id = await command.ExecuteScalarAsync(cancellationToken);
        return id is null ? null : new BucketRow((long)id, name, versioning, objectLockEnabled, createdUs);
    }

    public async ValueTask SetBucketVersioningAsync(long bucketId, StoredVersioning versioning, CancellationToken cancellationToken = default)
    {
        await using var command = Command("UPDATE buckets SET versioning = @versioning WHERE id = @id;");
        command.Parameters.AddWithValue("@versioning", (int)versioning);
        command.Parameters.AddWithValue("@id", bucketId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask SetBucketObjectLockAsync(long bucketId, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var command = Command("UPDATE buckets SET object_lock = @lock WHERE id = @id;");
        command.Parameters.AddWithValue("@lock", enabled);
        command.Parameters.AddWithValue("@id", bucketId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Gets whether the bucket holds any version or delete marker. In-progress multipart uploads do not count.
    /// </summary>
    public async ValueTask<bool> BucketHasVersionsAsync(long bucketId, CancellationToken cancellationToken = default)
    {
        await using var command = Command("SELECT EXISTS (SELECT 1 FROM object_versions WHERE bucket_id = @bucket);");
        command.Parameters.AddWithValue("@bucket", bucketId);
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture) != 0;
    }

    /// <summary>
    /// Deletes a bucket that holds no versions, with its configuration, its empty key rows and its uploads.
    /// Returns the blobs of the uploads' parts, which the caller queues for garbage collection.
    /// </summary>
    public async ValueTask<List<string>> DeleteBucketAsync(long bucketId, CancellationToken cancellationToken = default)
    {
        var locators = new List<string>();
        await using (var parts = Command("""
            SELECT p.manifest FROM upload_parts p JOIN uploads u ON u.id = p.upload_row WHERE u.bucket_id = @bucket;
            """)) {
            parts.Parameters.AddWithValue("@bucket", bucketId);
            await using var reader = await parts.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken)) {
                locators.AddRange(ReadManifest(reader.GetString(0)).Select(static extent => extent.Locator));
            }
        }

        await using var command = Command("DELETE FROM buckets WHERE id = @bucket;");
        command.Parameters.AddWithValue("@bucket", bucketId);
        await command.ExecuteNonQueryAsync(cancellationToken);
        return locators;
    }

    // ----- Bucket configuration -----

    public async ValueTask<string?> GetConfigAsync(long bucketId, string kind, CancellationToken cancellationToken = default)
    {
        await using var command = Command("SELECT doc FROM bucket_configs WHERE bucket_id = @bucket AND kind = @kind;");
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@kind", kind);
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    /// <summary>
    /// Lists the documents whose kind starts with <paramref name="kindPrefix"/>, ordered by kind.
    /// </summary>
    public async ValueTask<List<(string Kind, string Doc)>> ListConfigsAsync(long bucketId, string kindPrefix, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            SELECT kind, doc FROM bucket_configs
            WHERE bucket_id = @bucket AND substr(kind, 1, length(@prefix)) = @prefix
            ORDER BY kind;
            """);
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@prefix", kindPrefix);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var configs = new List<(string, string)>();
        while (await reader.ReadAsync(cancellationToken)) {
            configs.Add((reader.GetString(0), reader.GetString(1)));
        }

        return configs;
    }

    public async ValueTask PutConfigAsync(long bucketId, string kind, string doc, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            INSERT INTO bucket_configs (bucket_id, kind, doc) VALUES (@bucket, @kind, @doc)
            ON CONFLICT (bucket_id, kind) DO UPDATE SET doc = excluded.doc;
            """);
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@kind", kind);
        command.Parameters.AddWithValue("@doc", doc);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask<bool> DeleteConfigAsync(long bucketId, string kind, CancellationToken cancellationToken = default)
    {
        await using var command = Command("DELETE FROM bucket_configs WHERE bucket_id = @bucket AND kind = @kind;");
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@kind", kind);
        return await command.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    // ----- Keys and versions -----

    /// <summary>
    /// Locks the key for the rest of the transaction, creating its row when missing, and returns it. From here on
    /// the key's current version cannot change under this transaction.
    /// </summary>
    public async ValueTask<HeadRow> LockKeyAsync(long bucketId, byte[] key, CancellationToken cancellationToken = default)
    {
        await using (var insert = Command("""
            INSERT INTO object_heads (bucket_id, key, seq, current_seq, live) VALUES (@bucket, @key, 0, NULL, 0)
            ON CONFLICT (bucket_id, key) DO NOTHING;
            """)) {
            insert.Parameters.AddWithValue("@bucket", bucketId);
            insert.Parameters.AddWithValue("@key", key);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        return await GetHeadAsync(bucketId, key, cancellationToken)
            ?? throw new InvalidOperationException("The key's row vanished inside its own transaction.");
    }

    public async ValueTask<HeadRow?> GetHeadAsync(long bucketId, byte[] key, CancellationToken cancellationToken = default)
    {
        await using var command = Command("SELECT seq, current_seq FROM object_heads WHERE bucket_id = @bucket AND key = @key;");
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) {
            return null;
        }

        return new HeadRow(reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetInt64(1));
    }

    /// <summary>
    /// Points the key at its current version (<see langword="null"/> for none) and records the last sequence
    /// number handed out. A key is listed when its current version exists and is not a delete marker.
    /// </summary>
    public async ValueTask SetHeadAsync(long bucketId, byte[] key, long seq, long? currentSeq, bool live, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            UPDATE object_heads SET seq = @seq, current_seq = @current, live = @live
            WHERE bucket_id = @bucket AND key = @key;
            """);
        command.Parameters.AddWithValue("@seq", seq);
        command.Parameters.AddWithValue("@current", (object?)currentSeq ?? DBNull.Value);
        command.Parameters.AddWithValue("@live", live);
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@key", key);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Removes the key's row. Only valid once the key has no versions; the schema refuses otherwise.
    /// </summary>
    public async ValueTask DeleteHeadAsync(long bucketId, byte[] key, CancellationToken cancellationToken = default)
    {
        await using var command = Command("DELETE FROM object_heads WHERE bucket_id = @bucket AND key = @key;");
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@key", key);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Reads the key's current version, with its data. <see langword="null"/> when the key has none.
    /// </summary>
    public async ValueTask<VersionRow?> GetCurrentVersionAsync(long bucketId, byte[] key, CancellationToken cancellationToken = default)
    {
        await using var command = Command($"""
            SELECT {VersionColumns}, 1, v.inline_data, v.manifest
            FROM object_heads h JOIN object_versions v ON v.bucket_id = h.bucket_id AND v.key = h.key AND v.seq = h.current_seq
            WHERE h.bucket_id = @bucket AND h.key = @key;
            """);
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadVersion(reader, withData: true) : null;
    }

    /// <summary>
    /// Reads one version by its id (<see cref="VersionRow.NullVersionId"/> for the null version), with its data.
    /// </summary>
    public async ValueTask<VersionRow?> GetVersionAsync(long bucketId, byte[] key, string versionId, CancellationToken cancellationToken = default)
    {
        await using var command = Command($"""
            SELECT {VersionColumns}, COALESCE(v.seq = h.current_seq, 0), v.inline_data, v.manifest
            FROM object_versions v JOIN object_heads h ON h.bucket_id = v.bucket_id AND h.key = v.key
            WHERE v.bucket_id = @bucket AND v.key = @key AND v.version_id = @version;
            """);
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@version", versionId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadVersion(reader, withData: true) : null;
    }

    /// <summary>
    /// Reads the key's newest remaining version, with its data; the one that becomes current when the current
    /// version is removed.
    /// </summary>
    public async ValueTask<VersionRow?> GetNewestVersionAsync(long bucketId, byte[] key, CancellationToken cancellationToken = default)
    {
        await using var command = Command($"""
            SELECT {VersionColumns}, 0, v.inline_data, v.manifest
            FROM object_versions v
            WHERE v.bucket_id = @bucket AND v.key = @key
            ORDER BY v.seq DESC LIMIT 1;
            """);
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadVersion(reader, withData: true) : null;
    }

    public async ValueTask InsertVersionAsync(VersionRow version, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            INSERT INTO object_versions (bucket_id, key, seq, version_id, is_delete_marker, size, etag, last_modified_us,
                storage_class, retain_until_us, legal_hold, meta, inline_data, manifest)
            VALUES (@bucket, @key, @seq, @version, @marker, @size, @etag, @modified,
                @class, @retain, @hold, @meta, @inline, @manifest);
            """);
        command.Parameters.AddWithValue("@bucket", version.BucketId);
        command.Parameters.AddWithValue("@key", version.Key);
        command.Parameters.AddWithValue("@seq", version.Seq);
        command.Parameters.AddWithValue("@version", version.VersionId);
        command.Parameters.AddWithValue("@marker", version.IsDeleteMarker);
        command.Parameters.AddWithValue("@size", version.Size);
        command.Parameters.AddWithValue("@etag", (object?)version.ETag ?? DBNull.Value);
        command.Parameters.AddWithValue("@modified", version.LastModifiedUs);
        command.Parameters.AddWithValue("@class", (object?)version.StorageClass ?? DBNull.Value);
        command.Parameters.AddWithValue("@retain", (object?)version.RetainUntilUs ?? DBNull.Value);
        command.Parameters.AddWithValue("@hold", version.LegalHold);
        command.Parameters.AddWithValue("@meta", JsonSerializer.Serialize(version.Meta, MetadataJsonContext.Default.VersionMeta));
        command.Parameters.AddWithValue("@inline", (object?)version.InlineData ?? DBNull.Value);
        command.Parameters.AddWithValue("@manifest", version.Manifest is null ? DBNull.Value : WriteManifest(version.Manifest));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask UpdateVersionMetaAsync(long bucketId, byte[] key, long seq, VersionMeta meta, CancellationToken cancellationToken = default)
    {
        await using var command = Command("UPDATE object_versions SET meta = @meta WHERE bucket_id = @bucket AND key = @key AND seq = @seq;");
        command.Parameters.AddWithValue("@meta", JsonSerializer.Serialize(meta, MetadataJsonContext.Default.VersionMeta));
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@seq", seq);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask DeleteVersionAsync(long bucketId, byte[] key, long seq, CancellationToken cancellationToken = default)
    {
        await using var command = Command("DELETE FROM object_versions WHERE bucket_id = @bucket AND key = @key AND seq = @seq;");
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@seq", seq);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Lists the current versions of listed keys (see <see cref="SetHeadAsync"/>) in key order: keys starting with
    /// <paramref name="prefix"/> and, when given, after <paramref name="afterKey"/>. Rows carry no data.
    /// </summary>
    public async ValueTask<List<VersionRow>> ListCurrentAsync(long bucketId, byte[] prefix, byte[]? afterKey, int limit, CancellationToken cancellationToken = default)
    {
        var sql = new StringBuilder($"""
            SELECT {VersionColumns}, 1
            FROM object_heads h JOIN object_versions v ON v.bucket_id = h.bucket_id AND v.key = h.key AND v.seq = h.current_seq
            WHERE h.bucket_id = @bucket AND h.live AND h.key >= @prefix
            """);
        await using var command = Command(string.Empty);
        AppendKeyRange(sql, command, "h.key", prefix, afterKey);
        sql.Append(" ORDER BY h.key LIMIT @limit;");
        command.CommandText = sql.ToString();
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@limit", limit);
        return await ReadVersionsAsync(command, cancellationToken);
    }

    /// <summary>
    /// Lists versions and delete markers in S3's order, key ascending and newest first within a key, after the
    /// position (<paramref name="afterKey"/>, <paramref name="afterSeq"/>): later keys, and older versions of that
    /// key. <paramref name="afterSeq"/> <see langword="null"/> means after every version of the key. Rows carry no data.
    /// </summary>
    public async ValueTask<List<VersionRow>> ListVersionsAsync(long bucketId, byte[] prefix, byte[]? afterKey, long? afterSeq, int limit, CancellationToken cancellationToken = default)
    {
        var sql = new StringBuilder($"""
            SELECT {VersionColumns}, COALESCE(v.seq = h.current_seq, 0)
            FROM object_versions v JOIN object_heads h ON h.bucket_id = v.bucket_id AND h.key = v.key
            WHERE v.bucket_id = @bucket AND v.key >= @prefix
            """);
        await using var command = Command(string.Empty);
        if (afterKey is not null && afterSeq is not null) {
            AppendKeyRange(sql, command, "v.key", prefix, afterKey: null);
            sql.Append(" AND (v.key > @afterKey OR (v.key = @afterKey AND v.seq < @afterSeq))");
            command.Parameters.AddWithValue("@afterKey", afterKey);
            command.Parameters.AddWithValue("@afterSeq", afterSeq.Value);
        }
        else {
            AppendKeyRange(sql, command, "v.key", prefix, afterKey);
        }

        sql.Append(" ORDER BY v.key, v.seq DESC LIMIT @limit;");
        command.CommandText = sql.ToString();
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@limit", limit);
        return await ReadVersionsAsync(command, cancellationToken);
    }

    // ----- Multipart uploads -----

    public async ValueTask<long> InsertUploadAsync(UploadRow upload, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            INSERT INTO uploads (bucket_id, upload_id, key, initiated_us, meta) VALUES (@bucket, @upload, @key, @initiated, @meta)
            RETURNING id;
            """);
        command.Parameters.AddWithValue("@bucket", upload.BucketId);
        command.Parameters.AddWithValue("@upload", upload.UploadId);
        command.Parameters.AddWithValue("@key", upload.Key);
        command.Parameters.AddWithValue("@initiated", upload.InitiatedUs);
        command.Parameters.AddWithValue("@meta", JsonSerializer.Serialize(upload.Meta, MetadataJsonContext.Default.UploadMeta));
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>
    /// Reads an upload of the bucket. A write transaction that changes parts locks the row shared; Complete and
    /// Abort lock it exclusively, so no part lands after Complete has read the parts.
    /// </summary>
    public async ValueTask<UploadRow?> GetUploadAsync(long bucketId, string uploadId, bool exclusive, CancellationToken cancellationToken = default)
    {
        _ = exclusive; // SQLite has no row locks; its one writer at a time gives the same order.
        await using var command = Command("SELECT id, bucket_id, upload_id, key, initiated_us, meta FROM uploads WHERE bucket_id = @bucket AND upload_id = @upload;");
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@upload", uploadId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadUpload(reader) : null;
    }

    /// <summary>
    /// Lists uploads in key order and, within a key, by upload id, which is time-ordered, so oldest first. Starts
    /// after the position (<paramref name="afterKey"/>, <paramref name="afterUploadId"/>);
    /// <paramref name="afterUploadId"/> <see langword="null"/> means after every upload of the key.
    /// </summary>
    public async ValueTask<List<UploadRow>> ListUploadsAsync(long bucketId, byte[] prefix, byte[]? afterKey, string? afterUploadId, int limit, CancellationToken cancellationToken = default)
    {
        var sql = new StringBuilder("""
            SELECT id, bucket_id, upload_id, key, initiated_us, meta FROM uploads
            WHERE bucket_id = @bucket AND key >= @prefix
            """);
        await using var command = Command(string.Empty);
        if (afterKey is not null && afterUploadId is not null) {
            AppendKeyRange(sql, command, "key", prefix, afterKey: null);
            sql.Append(" AND (key > @afterKey OR (key = @afterKey AND upload_id > @afterUpload))");
            command.Parameters.AddWithValue("@afterKey", afterKey);
            command.Parameters.AddWithValue("@afterUpload", afterUploadId);
        }
        else {
            AppendKeyRange(sql, command, "key", prefix, afterKey);
        }

        sql.Append(" ORDER BY key, upload_id LIMIT @limit;");
        command.CommandText = sql.ToString();
        command.Parameters.AddWithValue("@bucket", bucketId);
        command.Parameters.AddWithValue("@limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var uploads = new List<UploadRow>();
        while (await reader.ReadAsync(cancellationToken)) {
            uploads.Add(ReadUpload(reader));
        }

        return uploads;
    }

    /// <summary>
    /// Deletes an upload and its parts. The caller queues the blobs of parts that no committed version lists.
    /// </summary>
    public async ValueTask DeleteUploadAsync(long uploadRowId, CancellationToken cancellationToken = default)
    {
        await using var command = Command("DELETE FROM uploads WHERE id = @id;");
        command.Parameters.AddWithValue("@id", uploadRowId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Stores a part, replacing one with the same number. Returns the replaced part, whose blobs the caller queues
    /// for garbage collection.
    /// </summary>
    public async ValueTask<PartRow?> UpsertPartAsync(PartRow part, CancellationToken cancellationToken = default)
    {
        var replaced = await GetPartAsync(part.UploadRowId, part.PartNumber, cancellationToken);
        await using var command = Command("""
            INSERT INTO upload_parts (upload_row, part_number, size, etag, last_modified_us, meta, manifest)
            VALUES (@upload, @part, @size, @etag, @modified, @meta, @manifest)
            ON CONFLICT (upload_row, part_number) DO UPDATE SET
                size = excluded.size, etag = excluded.etag, last_modified_us = excluded.last_modified_us,
                meta = excluded.meta, manifest = excluded.manifest;
            """);
        command.Parameters.AddWithValue("@upload", part.UploadRowId);
        command.Parameters.AddWithValue("@part", part.PartNumber);
        command.Parameters.AddWithValue("@size", part.Size);
        command.Parameters.AddWithValue("@etag", part.ETag);
        command.Parameters.AddWithValue("@modified", part.LastModifiedUs);
        command.Parameters.AddWithValue("@meta", JsonSerializer.Serialize(part.Meta, MetadataJsonContext.Default.PartMeta));
        command.Parameters.AddWithValue("@manifest", WriteManifest(part.Manifest));
        await command.ExecuteNonQueryAsync(cancellationToken);
        return replaced;
    }

    public async ValueTask<PartRow?> GetPartAsync(long uploadRowId, int partNumber, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            SELECT upload_row, part_number, size, etag, last_modified_us, meta, manifest FROM upload_parts
            WHERE upload_row = @upload AND part_number = @part;
            """);
        command.Parameters.AddWithValue("@upload", uploadRowId);
        command.Parameters.AddWithValue("@part", partNumber);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadPart(reader) : null;
    }

    /// <summary>
    /// Lists an upload's parts in part-number order, after <paramref name="afterPartNumber"/>.
    /// </summary>
    public async ValueTask<List<PartRow>> ListPartsAsync(long uploadRowId, int afterPartNumber, int limit, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            SELECT upload_row, part_number, size, etag, last_modified_us, meta, manifest FROM upload_parts
            WHERE upload_row = @upload AND part_number > @after
            ORDER BY part_number LIMIT @limit;
            """);
        command.Parameters.AddWithValue("@upload", uploadRowId);
        command.Parameters.AddWithValue("@after", afterPartNumber);
        command.Parameters.AddWithValue("@limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var parts = new List<PartRow>();
        while (await reader.ReadAsync(cancellationToken)) {
            parts.Add(ReadPart(reader));
        }

        return parts;
    }

    // ----- Blobs and garbage collection -----

    /// <summary>
    /// Records that committed rows reference these blobs. Returns <see langword="false"/> when the orphan sweep has
    /// claimed one of them: its blob is gone or going, and the transaction must not commit.
    /// </summary>
    public async ValueTask<bool> ReferenceBlobsAsync(IEnumerable<string> locators, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            INSERT INTO blob_refs (locator, swept) VALUES (@locator, 0)
            ON CONFLICT (locator) DO NOTHING
            RETURNING locator;
            """);
        var parameter = command.Parameters.Add("@locator", SqliteType.Text);
        foreach (var locator in locators) {
            parameter.Value = locator;
            if (await command.ExecuteScalarAsync(cancellationToken) is not null) {
                continue;
            }

            // The row existed. Referenced twice by this engine is a bug we can live with; swept is not.
            await using var check = Command("SELECT swept FROM blob_refs WHERE locator = @locator;");
            check.Parameters.AddWithValue("@locator", locator);
            if (Convert.ToInt64(await check.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture) != 0) {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Queues blobs that no committed row references any more; garbage collection deletes them after
    /// <paramref name="notBeforeUs"/>, so a read that resolved them before this commit can finish.
    /// </summary>
    public async ValueTask EnqueueGarbageAsync(IEnumerable<string> locators, long notBeforeUs, CancellationToken cancellationToken = default)
    {
        await using var command = Command("""
            INSERT INTO gc_queue (locator, not_before_us) VALUES (@locator, @notBefore)
            ON CONFLICT (locator) DO NOTHING;
            """);
        var parameter = command.Parameters.Add("@locator", SqliteType.Text);
        command.Parameters.AddWithValue("@notBefore", notBeforeUs);
        foreach (var locator in locators) {
            parameter.Value = locator;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async ValueTask<List<string>> ListDueGarbageAsync(long nowUs, int limit, CancellationToken cancellationToken = default)
    {
        await using var command = Command("SELECT locator FROM gc_queue WHERE not_before_us <= @now ORDER BY not_before_us LIMIT @limit;");
        command.Parameters.AddWithValue("@now", nowUs);
        command.Parameters.AddWithValue("@limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var locators = new List<string>();
        while (await reader.ReadAsync(cancellationToken)) {
            locators.Add(reader.GetString(0));
        }

        return locators;
    }

    /// <summary>
    /// Forgets a blob that garbage collection deleted.
    /// </summary>
    public async ValueTask ForgetCollectedBlobAsync(string locator, CancellationToken cancellationToken = default)
    {
        foreach (var sql in (string[])["DELETE FROM gc_queue WHERE locator = @locator;", "DELETE FROM blob_refs WHERE locator = @locator;"]) {
            await using var command = Command(sql);
            command.Parameters.AddWithValue("@locator", locator);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Returns the <paramref name="locators"/> that have a <c>blob_refs</c> row, each with whether the orphan sweep
    /// claimed it (<see langword="true"/>) or a commit referenced it (<see langword="false"/>).
    /// </summary>
    public async ValueTask<Dictionary<string, bool>> ClassifyBlobsAsync(IEnumerable<string> locators, CancellationToken cancellationToken = default)
    {
        var known = new Dictionary<string, bool>(StringComparer.Ordinal);
        await using var command = Command("SELECT swept FROM blob_refs WHERE locator = @locator;");
        var parameter = command.Parameters.Add("@locator", SqliteType.Text);
        foreach (var locator in locators) {
            parameter.Value = locator;
            if (await command.ExecuteScalarAsync(cancellationToken) is { } swept) {
                known[locator] = Convert.ToInt64(swept, System.Globalization.CultureInfo.InvariantCulture) != 0;
            }
        }

        return known;
    }

    /// <summary>
    /// Records a blob the orphan sweep found without a reference, keeping the first time it was seen, and returns
    /// that time.
    /// </summary>
    public async ValueTask<long> NoteOrphanCandidateAsync(string locator, long nowUs, CancellationToken cancellationToken = default)
    {
        await using (var insert = Command("""
            INSERT INTO orphan_candidates (locator, first_seen_us) VALUES (@locator, @now)
            ON CONFLICT (locator) DO NOTHING;
            """)) {
            insert.Parameters.AddWithValue("@locator", locator);
            insert.Parameters.AddWithValue("@now", nowUs);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var command = Command("SELECT first_seen_us FROM orphan_candidates WHERE locator = @locator;");
        command.Parameters.AddWithValue("@locator", locator);
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>
    /// Claims an orphan for deletion. Returns <see langword="false"/> when a commit referenced it first; either
    /// way the candidate row is gone afterwards.
    /// </summary>
    public async ValueTask<bool> ClaimOrphanAsync(string locator, CancellationToken cancellationToken = default)
    {
        await using (var forget = Command("DELETE FROM orphan_candidates WHERE locator = @locator;")) {
            forget.Parameters.AddWithValue("@locator", locator);
            await forget.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var command = Command("""
            INSERT INTO blob_refs (locator, swept) VALUES (@locator, 1)
            ON CONFLICT (locator) DO NOTHING
            RETURNING locator;
            """);
        command.Parameters.AddWithValue("@locator", locator);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    public async ValueTask<string?> GetStateAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var command = Command("SELECT value FROM engine_state WHERE name = @name;");
        command.Parameters.AddWithValue("@name", name);
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    public async ValueTask SetStateAsync(string name, string? value, CancellationToken cancellationToken = default)
    {
        await using var command = Command(value is null
            ? "DELETE FROM engine_state WHERE name = @name;"
            : "INSERT INTO engine_state (name, value) VALUES (@name, @value) ON CONFLICT (name) DO UPDATE SET value = excluded.value;");
        command.Parameters.AddWithValue("@name", name);
        if (value is not null) {
            command.Parameters.AddWithValue("@value", value);
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    // ----- Helpers -----

    private SqliteCommand Command(string sql)
    {
        var command = _connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = sql;
        return command;
    }

    // Restricts `column` to keys that start with the prefix and, when given, sort after `afterKey`. Key order is
    // bytewise over UTF-8, which is S3's order; BLOB comparison in SQLite and bytea in PostgreSQL both give it.
    private static void AppendKeyRange(StringBuilder sql, SqliteCommand command, string column, byte[] prefix, byte[]? afterKey)
    {
        command.Parameters.AddWithValue("@prefix", prefix);
        if (KeyRange.PrefixUpperBound(prefix) is { } upperBound) {
            sql.Append($" AND {column} < @prefixEnd");
            command.Parameters.AddWithValue("@prefixEnd", upperBound);
        }

        if (afterKey is not null) {
            sql.Append($" AND {column} > @after");
            command.Parameters.AddWithValue("@after", afterKey);
        }
    }

    private static async Task<List<VersionRow>> ReadVersionsAsync(SqliteCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var versions = new List<VersionRow>();
        while (await reader.ReadAsync(cancellationToken)) {
            versions.Add(ReadVersion(reader, withData: false));
        }

        return versions;
    }

    private static BucketRow ReadBucket(SqliteDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        (StoredVersioning)reader.GetInt32(2),
        reader.GetBoolean(3),
        reader.GetInt64(4));

    // Columns: VersionColumns (0-11), is_latest (12), then inline_data (13) and manifest (14) when withData.
    private static VersionRow ReadVersion(SqliteDataReader reader, bool withData) => new()
    {
        BucketId = reader.GetInt64(0),
        Key = (byte[])reader.GetValue(1),
        Seq = reader.GetInt64(2),
        VersionId = reader.GetString(3),
        IsDeleteMarker = reader.GetBoolean(4),
        Size = reader.GetInt64(5),
        ETag = reader.IsDBNull(6) ? null : reader.GetString(6),
        LastModifiedUs = reader.GetInt64(7),
        StorageClass = reader.IsDBNull(8) ? null : reader.GetString(8),
        RetainUntilUs = reader.IsDBNull(9) ? null : reader.GetInt64(9),
        LegalHold = reader.GetBoolean(10),
        Meta = JsonSerializer.Deserialize(reader.GetString(11), MetadataJsonContext.Default.VersionMeta)!,
        IsLatest = reader.GetBoolean(12),
        InlineData = withData && !reader.IsDBNull(13) ? (byte[])reader.GetValue(13) : null,
        Manifest = withData && !reader.IsDBNull(14) ? ReadManifest(reader.GetString(14)) : null
    };

    private static UploadRow ReadUpload(SqliteDataReader reader) => new()
    {
        Id = reader.GetInt64(0),
        BucketId = reader.GetInt64(1),
        UploadId = reader.GetString(2),
        Key = (byte[])reader.GetValue(3),
        InitiatedUs = reader.GetInt64(4),
        Meta = JsonSerializer.Deserialize(reader.GetString(5), MetadataJsonContext.Default.UploadMeta)!
    };

    private static PartRow ReadPart(SqliteDataReader reader) => new()
    {
        UploadRowId = reader.GetInt64(0),
        PartNumber = reader.GetInt32(1),
        Size = reader.GetInt64(2),
        ETag = reader.GetString(3),
        LastModifiedUs = reader.GetInt64(4),
        Meta = JsonSerializer.Deserialize(reader.GetString(5), MetadataJsonContext.Default.PartMeta)!,
        Manifest = ReadManifest(reader.GetString(6))
    };

    private static string WriteManifest(IReadOnlyList<Extent> manifest)
        => JsonSerializer.Serialize(manifest as Extent[] ?? [.. manifest], MetadataJsonContext.Default.ExtentArray);

    internal static Extent[] ReadManifest(string json)
        => JsonSerializer.Deserialize(json, MetadataJsonContext.Default.ExtentArray) ?? [];
}

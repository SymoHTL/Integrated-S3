using Microsoft.Data.Sqlite;

namespace IntegratedS3.Engine.Metadata;

/// <summary>
/// The single-node metadata store: one SQLite database in WAL mode. SQLite runs one write transaction at a time,
/// so write transactions queue on an async gate in this process, and the bucket and key locks of
/// <see cref="MetadataTransaction"/> have nothing left to do. Readers run beside the writer on their own
/// snapshot. The database must not be shared by two processes.
/// </summary>
internal sealed class SqliteMetadataStore : IAsyncDisposable
{
    // Each entry migrates the schema from version (index) to version (index + 1). Only ever append:
    // an entry that has run on a database is never edited.
    private static readonly string[] Migrations =
    [
        """
        CREATE TABLE buckets (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            versioning INTEGER NOT NULL,
            object_lock INTEGER NOT NULL,
            created_us INTEGER NOT NULL
        );

        CREATE TABLE bucket_configs (
            bucket_id INTEGER NOT NULL REFERENCES buckets (id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            doc TEXT NOT NULL,
            PRIMARY KEY (bucket_id, kind)
        ) WITHOUT ROWID;

        CREATE TABLE object_heads (
            bucket_id INTEGER NOT NULL REFERENCES buckets (id) ON DELETE CASCADE,
            key BLOB NOT NULL,
            seq INTEGER NOT NULL,
            current_seq INTEGER,
            live INTEGER NOT NULL,
            PRIMARY KEY (bucket_id, key)
        ) WITHOUT ROWID;

        CREATE INDEX object_heads_live ON object_heads (bucket_id, key) WHERE live;

        CREATE TABLE object_versions (
            bucket_id INTEGER NOT NULL,
            key BLOB NOT NULL,
            seq INTEGER NOT NULL,
            version_id TEXT NOT NULL,
            is_delete_marker INTEGER NOT NULL,
            size INTEGER NOT NULL,
            etag TEXT,
            last_modified_us INTEGER NOT NULL,
            storage_class TEXT,
            retain_until_us INTEGER,
            legal_hold INTEGER NOT NULL,
            meta TEXT NOT NULL,
            inline_data BLOB,
            manifest TEXT,
            PRIMARY KEY (bucket_id, key, seq),
            FOREIGN KEY (bucket_id, key) REFERENCES object_heads (bucket_id, key)
        ) WITHOUT ROWID;

        CREATE UNIQUE INDEX object_versions_by_id ON object_versions (bucket_id, key, version_id);

        CREATE TABLE uploads (
            id INTEGER PRIMARY KEY,
            bucket_id INTEGER NOT NULL REFERENCES buckets (id) ON DELETE CASCADE,
            upload_id TEXT NOT NULL UNIQUE,
            key BLOB NOT NULL,
            initiated_us INTEGER NOT NULL,
            meta TEXT NOT NULL
        );

        CREATE INDEX uploads_by_key ON uploads (bucket_id, key, upload_id);

        CREATE TABLE upload_parts (
            upload_row INTEGER NOT NULL REFERENCES uploads (id) ON DELETE CASCADE,
            part_number INTEGER NOT NULL,
            size INTEGER NOT NULL,
            etag TEXT NOT NULL,
            last_modified_us INTEGER NOT NULL,
            meta TEXT NOT NULL,
            manifest TEXT NOT NULL,
            PRIMARY KEY (upload_row, part_number)
        ) WITHOUT ROWID;

        CREATE TABLE blob_refs (
            locator TEXT PRIMARY KEY,
            swept INTEGER NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE gc_queue (
            locator TEXT PRIMARY KEY,
            not_before_us INTEGER NOT NULL
        ) WITHOUT ROWID;

        CREATE INDEX gc_queue_due ON gc_queue (not_before_us);

        CREATE TABLE orphan_candidates (
            locator TEXT PRIMARY KEY,
            first_seen_us INTEGER NOT NULL,
            walk INTEGER NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE engine_state (
            name TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID;
        """
    ];

    private readonly string _connectionString;
    private readonly TimeProvider _timeProvider;
    private readonly SemaphoreSlim _writeGate = new(1, 1);

    private SqliteMetadataStore(string connectionString, TimeProvider timeProvider)
    {
        _connectionString = connectionString;
        _timeProvider = timeProvider;
    }

    /// <summary>
    /// Gets the schema version this code writes.
    /// </summary>
    public static int SchemaVersion => Migrations.Length;

    /// <summary>
    /// Opens the database at <paramref name="path"/>, creating it and its directory when missing, and migrates
    /// its schema to <see cref="SchemaVersion"/>. Transactions read the time from <paramref name="timeProvider"/>.
    /// </summary>
    public static async Task<SqliteMetadataStore> OpenAsync(string path, TimeProvider timeProvider, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(timeProvider);
        var fullPath = Path.GetFullPath(path);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = fullPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            ForeignKeys = true,
            Pooling = true,
            DefaultTimeout = 30
        }.ToString();

        var store = new SqliteMetadataStore(connectionString, timeProvider);
        try {
            await store.MigrateAsync(cancellationToken);
        }
        catch {
            // A refused or failed migration must not leave a pooled connection holding the file open.
            await store.DisposeAsync();
            throw;
        }

        return store;
    }

    /// <summary>
    /// Begins a transaction. A write transaction takes the database's write lock at once (<c>BEGIN IMMEDIATE</c>)
    /// and holds this store's write gate until it is committed or disposed; a read transaction sees one snapshot.
    /// </summary>
    public async ValueTask<MetadataTransaction> BeginAsync(bool write, CancellationToken cancellationToken = default)
    {
        if (write) {
            await _writeGate.WaitAsync(cancellationToken);
        }

        SqliteConnection? connection = null;
        try {
            connection = new SqliteConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
            var transaction = connection.BeginTransaction(deferred: !write);
            return new MetadataTransaction(connection, transaction, write ? _writeGate : null, _timeProvider);
        }
        catch {
            if (connection is not null) {
                await connection.DisposeAsync();
            }

            if (write) {
                _writeGate.Release();
            }

            throw;
        }
    }

    public ValueTask DisposeAsync()
    {
        SqliteConnection.ClearPool(new SqliteConnection(_connectionString));
        _writeGate.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task MigrateAsync(CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // WAL lets readers run beside the writer; the setting is stored in the database file.
        await ExecuteAsync(connection, null, "PRAGMA journal_mode = WAL;", cancellationToken);

        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        await ExecuteAsync(connection, transaction, "CREATE TABLE IF NOT EXISTS engine_schema (version INTEGER NOT NULL);", cancellationToken);

        await using var read = connection.CreateCommand();
        read.Transaction = transaction;
        read.CommandText = "SELECT version FROM engine_schema;";
        var stored = await read.ExecuteScalarAsync(cancellationToken);
        var version = stored is null ? 0 : Convert.ToInt32(stored, System.Globalization.CultureInfo.InvariantCulture);

        if (version > Migrations.Length) {
            throw new InvalidOperationException(
                $"The metadata database has schema version {version}, newer than the {Migrations.Length} this version of the engine knows. Upgrade the engine.");
        }

        for (var next = version; next < Migrations.Length; next++) {
            await ExecuteAsync(connection, transaction, Migrations[next], cancellationToken);
        }

        if (version != Migrations.Length) {
            await ExecuteAsync(connection, transaction, "DELETE FROM engine_schema;", cancellationToken);
            await using var write = connection.CreateCommand();
            write.Transaction = transaction;
            write.CommandText = "INSERT INTO engine_schema (version) VALUES (@version);";
            write.Parameters.AddWithValue("@version", Migrations.Length);
            await write.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync();
    }

    private static async Task ExecuteAsync(SqliteConnection connection, SqliteTransaction? transaction, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}

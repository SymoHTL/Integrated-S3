using IntegratedS3.Abstractions.Blobs;

namespace IntegratedS3.Engine.Blobs;

/// <summary>
/// An <see cref="IBlobStore"/> on a local or shared filesystem. Each blob is one file, named by a random
/// locator (a version 4 GUID: 122 random bits) and spread over two levels of directories (<c>ab/cd/abcd…</c>). Files are created once and
/// never renamed or modified, so several nodes can share one directory without any locking. A root directory that is
/// missing after construction, or replaced by a file, is an I/O error, never an empty store: every call throws an
/// <see cref="IOException"/> that names it, and a write does not create it. A shared root must be mounted before the
/// engine starts, since an empty mount point looks like an empty store. HAZARD (#289): so does a share unmounted
/// while the engine runs, whose mount point stays behind: live locators answer not found, listings come back empty,
/// and writes land under the mount point, hidden once the share is back.
/// </summary>
public sealed class LocalDiskBlobStore : IBlobStore
{
    private const int LocatorLength = 32;
    private const int CopyBufferSize = 81920;

    private readonly string _rootPath;
    private readonly bool _flushToDisk;

    /// <summary>
    /// Initializes a store under <paramref name="rootPath"/>, creating the directory when it is missing.
    /// </summary>
    /// <param name="rootPath">The directory that holds the blobs.</param>
    /// <param name="flushToDisk">
    /// Whether each write is flushed to stable storage before it returns. Turn it off only where a lost blob after
    /// a power failure is acceptable, such as tests. Defaults to <see langword="true"/>.
    /// </param>
    public LocalDiskBlobStore(string rootPath, bool flushToDisk = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootPath);

        _rootPath = Path.GetFullPath(rootPath);
        _flushToDisk = flushToDisk;
        Directory.CreateDirectory(_rootPath);
    }

    /// <inheritdoc />
    public BlobStoreCapabilities Capabilities { get; } = new() { SupportsRangeReads = true };

    /// <inheritdoc />
    public ValueTask<BlobWriteResult> WriteAsync(Stream content, long? length = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        return WriteToAsync(Guid.NewGuid().ToString("N"), content, cancellationToken);
    }

    // Writes a new blob at the given locator. The tests pass a live blob's locator, as a locator collision would.
    internal async ValueTask<BlobWriteResult> WriteToAsync(string locator, Stream content, CancellationToken cancellationToken)
    {
        if (!IsLocator(locator)) {
            throw new ArgumentException("The locator is not one this store issues.", nameof(locator));
        }

        var path = GetPath(locator);
        var directory = Path.GetDirectoryName(path)!;
        if (!Directory.Exists(directory)) {
            // Only ab/ and ab/cd/ are created here, under a root that exists.
            // ponytail: a root removed between the check and CreateDirectory is created again, since .NET has no mkdir
            // that refuses a missing parent; the window is one system call wide.
            ThrowIfRootMissing();
            Directory.CreateDirectory(directory);
        }

        // A file at its final name that no metadata row references is an orphan for the sweep, never a blob a
        // client can read, so the write needs no temporary name and no rename.
        // ponytail: no fsync of the directory entries, neither the file's nor the ab/ and ab/cd/ directories this
        // write may create; ext4 and xfs journal them with the file's fsync, other filesystems may lose a
        // just-written blob on power failure until directory fsyncs are added.
        var written = 0L;
        var created = false;
        try {
            await using (var file = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 0, FileOptions.Asynchronous)) {
                created = true;
                var buffer = new byte[CopyBufferSize];
                int read;
                while ((read = await content.ReadAsync(buffer, cancellationToken)) > 0) {
                    await file.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
                    written += read;
                }

                if (_flushToDisk) {
                    file.Flush(flushToDisk: true);
                }
            }
        }
        catch {
            // Only a file this call created: CreateNew fails on an existing file, which is another write's blob.
            if (created) {
                TryDelete(path);
            }

            throw;
        }

        return new BlobWriteResult { Locator = locator, Length = written };
    }

    /// <inheritdoc />
    public ValueTask<Stream> OpenReadAsync(string locator, long offset = 0, long? length = null, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        if (length is < 0) {
            throw new ArgumentOutOfRangeException(nameof(length), length, "The length must not be negative.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        if (!IsLocator(locator)) {
            ThrowIfRootMissing();
            throw new BlobNotFoundException(locator);
        }

        Microsoft.Win32.SafeHandles.SafeFileHandle handle;
        try {
            // FileShare.Delete lets garbage collection remove a blob that a slow reader still streams (Windows).
            handle = File.OpenHandle(GetPath(locator), FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, FileOptions.Asynchronous);
        }
        catch (Exception exception) when (exception is FileNotFoundException or DirectoryNotFoundException) {
            ThrowIfRootMissing(exception);
            throw new BlobNotFoundException(locator, exception);
        }

        var fileLength = RandomAccess.GetLength(handle);
        var start = Math.Min(offset, fileLength);
        var end = length is { } requested ? start + Math.Min(requested, fileLength - start) : fileLength;
        return ValueTask.FromResult<Stream>(new FileRangeReadStream(handle, start, end));
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(string locator, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (IsLocator(locator)) {
            try {
                File.Delete(GetPath(locator));
            }
            catch (DirectoryNotFoundException) {
                // The blob never existed, which is a successful delete if the root is there.
            }
        }

        // Checked on every delete, not only after a DirectoryNotFoundException, so that a delete under a missing root
        // fails whatever File.Delete reports on the platform.
        ThrowIfRootMissing();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<BlobListPage> ListAsync(string? cursor, int maxEntries, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEntries);
        if (cursor is not null && !IsLocator(cursor)) {
            throw new ArgumentException("The cursor is not one this store returned.", nameof(cursor));
        }

        // Locators are fixed-length lowercase hex, and the directories are their first four characters, so walking
        // the directories in ordinal order walks the locators in ordinal order, and a cursor is the last locator seen.
        var entries = new List<BlobListEntry>(Math.Min(maxEntries, 1024));
        foreach (var first in SortedChildren(_rootPath, cursor?[..2], directories: true)) {
            var firstName = Path.GetFileName(first);
            var secondFloor = cursor is not null && string.CompareOrdinal(firstName, cursor[..2]) == 0 ? cursor[2..4] : null;
            foreach (var second in SortedChildren(first, secondFloor, directories: true)) {
                cancellationToken.ThrowIfCancellationRequested();
                foreach (var file in SortedChildren(second, floor: null, directories: false)) {
                    var locator = Path.GetFileName(file);
                    if (!IsLocator(locator)
                        || string.CompareOrdinal(locator[..4], firstName + Path.GetFileName(second)) != 0
                        || cursor is not null && string.CompareOrdinal(locator, cursor) <= 0) {
                        continue;
                    }

                    FileInfo info;
                    try {
                        info = new FileInfo(file);
                        if (!info.Exists) {
                            continue;
                        }
                    }
                    catch (IOException) {
                        continue;
                    }

                    entries.Add(new BlobListEntry { Locator = locator, Length = info.Length });

                    if (entries.Count == maxEntries) {
                        return ValueTask.FromResult(new BlobListPage { Entries = entries, NextCursor = locator });
                    }
                }
            }
        }

        // A missing root lists as nothing at all, which would read as an empty store.
        ThrowIfRootMissing();
        return ValueTask.FromResult(new BlobListPage { Entries = entries, NextCursor = null });
    }

    private string GetPath(string locator)
        => Path.Combine(_rootPath, locator[..2], locator[2..4], locator);

    // A deleted root, or a file in its place: the store cannot say what it holds, so no call answers "not found" or an
    // empty page. An unmounted share whose mount point stays behind passes this check (the HAZARD on the class).
    private void ThrowIfRootMissing(Exception? innerException = null)
    {
        if (!Directory.Exists(_rootPath)) {
            throw new IOException($"The blob store's root directory '{_rootPath}' does not exist or cannot be reached.", innerException);
        }
    }

    // The canonical form WriteAsync issues: exactly 32 lowercase hex digits. Every locator and cursor is checked
    // against it before it becomes a path, so no other string names a file, an uppercase copy of a locator on a
    // case-insensitive filesystem included.
    private static bool IsLocator(string? value)
    {
        if (value is not { Length: LocatorLength }) {
            return false;
        }

        foreach (var character in value) {
            if (!char.IsAsciiHexDigitLower(character)) {
                return false;
            }
        }

        return true;
    }

    // The children of a directory in ordinal name order, skipping names below floor.
    private static IEnumerable<string> SortedChildren(string directory, string? floor, bool directories)
    {
        string[] children;
        try {
            children = directories ? Directory.GetDirectories(directory) : Directory.GetFiles(directory);
        }
        catch (DirectoryNotFoundException) {
            yield break;
        }

        Array.Sort(children, StringComparer.Ordinal);
        foreach (var child in children) {
            if (floor is null || string.CompareOrdinal(Path.GetFileName(child), floor) >= 0) {
                yield return child;
            }
        }
    }

    private static void TryDelete(string path)
    {
        try {
            File.Delete(path);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException) {
            // The orphan sweep removes what is left.
        }
    }
}

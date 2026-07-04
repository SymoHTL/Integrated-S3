using System.Text.RegularExpressions;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Regression guard for issue #121: the disk provider must force every write-path temp file to
/// stable storage (<c>Flush(flushToDisk: true)</c>) before it publishes the file with an atomic
/// <c>File.Move(..., overwrite: true)</c> rename. Without the flush, a crash or power loss between the
/// rename and the OS flushing the page cache can publish a zero-length/torn object or an unparseable
/// JSON sidecar.
/// </summary>
/// <remarks>
/// The write paths construct their <see cref="System.IO.FileStream"/>s inline with <c>new FileStream(...)</c>,
/// so there is no injectable stream factory to spy on. This test therefore pins the durability contract
/// at the source level: it asserts (1) the shared flush helper actually forces the OS buffer to disk and
/// (2) every write-path atomic rename is preceded by a flush-to-stable-storage call in the same method.
/// If a future edit adds a new temp-file + <c>File.Move</c> publish path without flushing first, this test
/// fails.
/// </remarks>
public sealed class DiskStorageDurableFlushTests
{
    private static readonly Regex TempRenameRegex = new(
        @"File\.Move\(\s*(?<temp>\w*[Tt]emp\w*)\s*,[^;]*overwrite:\s*true",
        RegexOptions.Compiled);

    [Fact]
    public void FlushHelper_ForcesBuffersToDisk()
    {
        var source = ReadDiskStorageServiceSource();

        // The helper must both drain user-space buffers and force the OS page cache to physical media.
        Assert.Contains("FlushAsync(cancellationToken)", source, StringComparison.Ordinal);
        Assert.Contains("Flush(flushToDisk: true)", source, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryWritePathAtomicRename_IsPrecededByFlushToStableStorage()
    {
        var lines = File.ReadAllLines(GetDiskStorageServiceSourcePath());

        var renameLines = new List<int>();
        for (var i = 0; i < lines.Length; i++) {
            if (TempRenameRegex.IsMatch(lines[i])) {
                renameLines.Add(i);
            }
        }

        // Sanity: the write paths (put, copy, multipart part/complete, and the JSON sidecars) mean there
        // are several temp-file publish sites. If this drops to zero the detection regex has drifted.
        Assert.True(
            renameLines.Count >= 8,
            $"Expected to find the disk provider's temp-file atomic renames, but found {renameLines.Count}. " +
            "The detection regex may be stale.");

        foreach (var renameLine in renameLines) {
            // Look back within the enclosing write block for the durable-flush call. A generous window
            // covers the CompleteMultipartUpload assembly loop, which flushes after concatenating parts.
            var windowStart = Math.Max(0, renameLine - 60);
            var flushed = false;
            for (var i = renameLine - 1; i >= windowStart; i--) {
                if (lines[i].Contains("FlushToStableStorageAsync(", StringComparison.Ordinal)) {
                    flushed = true;
                    break;
                }
            }

            Assert.True(
                flushed,
                $"The atomic rename on line {renameLine + 1} of DiskStorageService.cs " +
                $"('{lines[renameLine].Trim()}') is not preceded by a FlushToStableStorageAsync call. " +
                "Write-path temp files must be flushed to stable storage before the rename (issue #121).");
        }
    }

    private static string ReadDiskStorageServiceSource()
        => File.ReadAllText(GetDiskStorageServiceSourcePath());

    private static string GetDiskStorageServiceSourcePath()
        => Path.Combine(
            GetRepositoryRoot(),
            "src", "IntegratedS3", "IntegratedS3.Provider.Disk", "DiskStorageService.cs");

    private static string GetRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);

        while (directory is not null) {
            if (File.Exists(Path.Combine(directory.FullName, "LICENSE"))
                && Directory.Exists(Path.Combine(directory.FullName, "docs"))
                && Directory.Exists(Path.Combine(directory.FullName, "src"))) {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the repository root for durable-flush validation.");
    }
}

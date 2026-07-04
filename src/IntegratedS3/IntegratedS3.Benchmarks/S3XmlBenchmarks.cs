using System.Text;
using BenchmarkDotNet.Attributes;
using IntegratedS3.Protocol;

namespace IntegratedS3.Benchmarks;

/// <summary>
/// Benchmarks S3 XML response writing for ListObjects results at 1 / 100 / 1000 keys.
/// </summary>
[MemoryDiagnoser]
public class S3XmlWriteBenchmarks
{
    [Params(1, 100, 1000)]
    public int KeyCount { get; set; }

    private S3ListBucketResult _result = null!;

    [GlobalSetup]
    public void Setup()
    {
        var contents = new S3ListBucketObject[KeyCount];
        var lastModified = new DateTimeOffset(2026, 7, 4, 12, 0, 0, TimeSpan.Zero);
        for (var i = 0; i < KeyCount; i++)
        {
            contents[i] = new S3ListBucketObject
            {
                Key = $"prefix/segment-{i / 100}/object-{i:D6}.bin",
                ETag = "\"5d41402abc4b2a76b9719d911017c592\"",
                Size = 1024L + i,
                LastModifiedUtc = lastModified,
                StorageClass = "STANDARD",
            };
        }

        _result = new S3ListBucketResult
        {
            Name = "benchmark-bucket",
            IsV2 = true,
            Prefix = "prefix/",
            MaxKeys = 1000,
            KeyCount = KeyCount,
            IsTruncated = KeyCount >= 1000,
            NextContinuationToken = KeyCount >= 1000 ? "next-token" : null,
            Contents = contents,
        };
    }

    [Benchmark]
    public int WriteListBucketResult() => S3XmlResponseWriter.WriteListBucketResult(_result).Length;
}

/// <summary>
/// Benchmarks S3 XML request parsing (CompleteMultipartUpload, DeleteObjects) at increasing entry counts.
/// </summary>
[MemoryDiagnoser]
public class S3XmlReadBenchmarks
{
    private const string Namespace = "http://s3.amazonaws.com/doc/2006-03-01/";

    [Params(1, 100, 1000)]
    public int EntryCount { get; set; }

    private byte[] _completeMultipartXml = [];
    private byte[] _deleteObjectsXml = [];

    [GlobalSetup]
    public void Setup()
    {
        var complete = new StringBuilder();
        complete.Append($"<CompleteMultipartUpload xmlns=\"{Namespace}\">");
        for (var i = 1; i <= EntryCount; i++)
        {
            complete.Append("<Part><PartNumber>").Append(i).Append("</PartNumber><ETag>&quot;")
                .Append("5d41402abc4b2a76b9719d911017c592").Append("&quot;</ETag></Part>");
        }

        complete.Append("</CompleteMultipartUpload>");
        _completeMultipartXml = Encoding.UTF8.GetBytes(complete.ToString());

        var delete = new StringBuilder();
        delete.Append($"<Delete xmlns=\"{Namespace}\">");
        for (var i = 0; i < EntryCount; i++)
        {
            delete.Append("<Object><Key>prefix/object-").Append(i).Append(".bin</Key></Object>");
        }

        delete.Append("<Quiet>false</Quiet></Delete>");
        _deleteObjectsXml = Encoding.UTF8.GetBytes(delete.ToString());
    }

    [Benchmark]
    public async Task<int> ReadCompleteMultipartUpload()
    {
        using var stream = new MemoryStream(_completeMultipartXml, writable: false);
        var parsed = await S3XmlRequestReader.ReadCompleteMultipartUploadRequestAsync(stream);
        return parsed.Parts.Count;
    }

    [Benchmark]
    public async Task<int> ReadDeleteObjects()
    {
        using var stream = new MemoryStream(_deleteObjectsXml, writable: false);
        var parsed = await S3XmlRequestReader.ReadDeleteObjectsRequestAsync(stream);
        return parsed.Objects.Count;
    }
}

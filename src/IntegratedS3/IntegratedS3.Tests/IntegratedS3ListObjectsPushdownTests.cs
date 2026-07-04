using System.Collections.Concurrent;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Xml.Linq;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Regression tests for issue #102: <c>ListObjects</c> (V1) and <c>ListObjectsV2</c> must push
/// <c>max-keys</c> (a page-size bound) and the client cursor (marker / continuation-token /
/// start-after) into the storage layer instead of materializing the entire bucket in memory.
///
/// The tests wrap <see cref="IStorageService"/> with a recording proxy that captures the last
/// <see cref="ListObjectsRequest"/> the endpoint issues and counts how many objects were actually
/// pulled from the returned stream, proving the whole bucket is no longer read for a small page.
/// </summary>
public sealed class IntegratedS3ListObjectsPushdownTests
{
    private static readonly XNamespace S3Ns = "http://s3.amazonaws.com/doc/2006-03-01/";

    private static async Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateRecordingClientAsync(
        WebUiApplicationFactory factory,
        ListObjectsRecorder recorder)
    {
        return await factory.CreateIsolatedClientAsync(configureBuilder: builder =>
        {
            var descriptor = builder.Services.Single(static service => service.ServiceType == typeof(IStorageService));
            builder.Services.Remove(descriptor);
            builder.Services.AddSingleton<IStorageService>(provider =>
            {
                IStorageService inner;
                if (descriptor.ImplementationFactory is not null)
                {
                    inner = (IStorageService)descriptor.ImplementationFactory(provider);
                }
                else if (descriptor.ImplementationInstance is not null)
                {
                    inner = (IStorageService)descriptor.ImplementationInstance;
                }
                else
                {
                    inner = (IStorageService)ActivatorUtilities.CreateInstance(provider, descriptor.ImplementationType!);
                }

                return RecordingStorageService.Create(inner, recorder);
            });
        });
    }

    private static async Task SeedObjectsAsync(HttpClient client, string bucketName, IEnumerable<string> keys)
    {
        var createBucket = await client.PutAsync($"/integrated-s3/buckets/{bucketName}", content: null);
        Assert.True(createBucket.IsSuccessStatusCode, $"Bucket create failed: {createBucket.StatusCode}");
        foreach (var key in keys)
        {
            var response = await client.PutAsync(
                $"/integrated-s3/buckets/{bucketName}/objects/{key}",
                new StringContent(key, Encoding.UTF8, "text/plain"));
            Assert.True(response.IsSuccessStatusCode, $"Object PUT failed for '{key}': {response.StatusCode}");
        }
    }

    private static string GetValue(XDocument document, string name)
        => document.Root!.Element(S3Ns + name)?.Value
           ?? throw new InvalidOperationException($"Missing element '{name}'.");

    private static string[] Contents(XDocument document)
        => document.Root!.Elements(S3Ns + "Contents")
            .Select(static element => element.Element(S3Ns + "Key")!.Value)
            .ToArray();

    private static string[] CommonPrefixes(XDocument document)
        => document.Root!.Elements(S3Ns + "CommonPrefixes")
            .Select(static element => element.Element(S3Ns + "Prefix")!.Value)
            .ToArray();

    [Fact]
    public async Task ListObjectsV2_SmallPage_DoesNotMaterializeWholeBucket()
    {
        await using var factory = new WebUiApplicationFactory();
        var recorder = new ListObjectsRecorder();
        await using var isolated = await CreateRecordingClientAsync(factory, recorder);
        var client = isolated.Client;

        // 20 objects, request only 2.
        var keys = Enumerable.Range(0, 20).Select(static index => $"key-{index:D2}.txt").ToArray();
        await SeedObjectsAsync(client, "pushdown-v2", keys);
        recorder.Reset();

        var response = await client.GetAsync("/integrated-s3/pushdown-v2?list-type=2&max-keys=2");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("2", GetValue(document, "KeyCount"));
        Assert.Equal("true", GetValue(document, "IsTruncated"));
        Assert.Equal("key-01.txt", GetValue(document, "NextContinuationToken"));
        Assert.Equal(new[] { "key-00.txt", "key-01.txt" }, Contents(document));

        var call = Assert.Single(recorder.Calls);
        // No delimiter -> a numeric page-size bound of maxKeys + 1 must be pushed down.
        Assert.Equal(3, call.Request.PageSize);
        Assert.Null(call.Request.ContinuationToken);
        // The endpoint must read at most maxKeys + 1 objects, never all 20.
        Assert.True(call.ObjectsRead <= 3, $"Expected <= 3 objects read, but read {call.ObjectsRead}.");
    }

    [Fact]
    public async Task ListObjectsV1_SmallPage_DoesNotMaterializeWholeBucket()
    {
        await using var factory = new WebUiApplicationFactory();
        var recorder = new ListObjectsRecorder();
        await using var isolated = await CreateRecordingClientAsync(factory, recorder);
        var client = isolated.Client;

        var keys = Enumerable.Range(0, 20).Select(static index => $"key-{index:D2}.txt").ToArray();
        await SeedObjectsAsync(client, "pushdown-v1", keys);
        recorder.Reset();

        var response = await client.GetAsync("/integrated-s3/pushdown-v1?max-keys=2");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("2", GetValue(document, "MaxKeys"));
        Assert.Equal("true", GetValue(document, "IsTruncated"));
        Assert.Equal(new[] { "key-00.txt", "key-01.txt" }, Contents(document));

        var call = Assert.Single(recorder.Calls);
        Assert.Equal(3, call.Request.PageSize);
        Assert.Null(call.Request.ContinuationToken);
        Assert.True(call.ObjectsRead <= 3, $"Expected <= 3 objects read, but read {call.ObjectsRead}.");
    }

    [Fact]
    public async Task ListObjectsV2_ContinuationToken_PushedIntoStorageAndResumesCorrectly()
    {
        await using var factory = new WebUiApplicationFactory();
        var recorder = new ListObjectsRecorder();
        await using var isolated = await CreateRecordingClientAsync(factory, recorder);
        var client = isolated.Client;

        var keys = new[] { "a.txt", "b.txt", "c.txt", "d.txt", "e.txt" };
        await SeedObjectsAsync(client, "pushdown-continue", keys);
        recorder.Reset();

        var firstResponse = await client.GetAsync("/integrated-s3/pushdown-continue?list-type=2&max-keys=2");
        var firstDocument = XDocument.Parse(await firstResponse.Content.ReadAsStringAsync());
        Assert.Equal("true", GetValue(firstDocument, "IsTruncated"));
        Assert.Equal(new[] { "a.txt", "b.txt" }, Contents(firstDocument));
        var token = GetValue(firstDocument, "NextContinuationToken");
        Assert.Equal("b.txt", token);

        recorder.Reset();
        var secondResponse = await client.GetAsync(
            $"/integrated-s3/pushdown-continue?list-type=2&max-keys=2&continuation-token={Uri.EscapeDataString(token)}");
        var secondDocument = XDocument.Parse(await secondResponse.Content.ReadAsStringAsync());
        Assert.Equal(new[] { "c.txt", "d.txt" }, Contents(secondDocument));
        Assert.Equal("true", GetValue(secondDocument, "IsTruncated"));
        Assert.Equal("d.txt", GetValue(secondDocument, "NextContinuationToken"));

        // The continuation token (a key cursor) must have been pushed into the storage request.
        var secondCall = Assert.Single(recorder.Calls);
        Assert.Equal("b.txt", secondCall.Request.ContinuationToken);
        Assert.Equal(3, secondCall.Request.PageSize);

        recorder.Reset();
        var thirdResponse = await client.GetAsync(
            $"/integrated-s3/pushdown-continue?list-type=2&max-keys=2&continuation-token={Uri.EscapeDataString("d.txt")}");
        var thirdDocument = XDocument.Parse(await thirdResponse.Content.ReadAsStringAsync());
        Assert.Equal(new[] { "e.txt" }, Contents(thirdDocument));
        Assert.Equal("false", GetValue(thirdDocument, "IsTruncated"));
        Assert.Empty(thirdDocument.Root!.Elements(S3Ns + "NextContinuationToken"));
    }

    [Fact]
    public async Task ListObjectsV2_StartAfter_PushedIntoStorage()
    {
        await using var factory = new WebUiApplicationFactory();
        var recorder = new ListObjectsRecorder();
        await using var isolated = await CreateRecordingClientAsync(factory, recorder);
        var client = isolated.Client;

        var keys = new[] { "a.txt", "b.txt", "c.txt", "d.txt" };
        await SeedObjectsAsync(client, "pushdown-startafter", keys);
        recorder.Reset();

        var response = await client.GetAsync(
            $"/integrated-s3/pushdown-startafter?list-type=2&max-keys=2&start-after={Uri.EscapeDataString("b.txt")}");
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(new[] { "c.txt", "d.txt" }, Contents(document));
        Assert.Equal("false", GetValue(document, "IsTruncated"));

        var call = Assert.Single(recorder.Calls);
        Assert.Equal("b.txt", call.Request.ContinuationToken);
    }

    [Fact]
    public async Task ListObjectsV1_Marker_PushedIntoStorage()
    {
        await using var factory = new WebUiApplicationFactory();
        var recorder = new ListObjectsRecorder();
        await using var isolated = await CreateRecordingClientAsync(factory, recorder);
        var client = isolated.Client;

        var keys = new[] { "a.txt", "b.txt", "c.txt", "d.txt" };
        await SeedObjectsAsync(client, "pushdown-marker", keys);
        recorder.Reset();

        var response = await client.GetAsync(
            $"/integrated-s3/pushdown-marker?marker={Uri.EscapeDataString("b.txt")}&max-keys=1");
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(new[] { "c.txt" }, Contents(document));
        Assert.Equal("true", GetValue(document, "IsTruncated"));

        var call = Assert.Single(recorder.Calls);
        Assert.Equal("b.txt", call.Request.ContinuationToken);
    }

    [Fact]
    public async Task ListObjectsV2_Delimiter_GroupsCommonPrefixesAndPaginatesAcrossPages()
    {
        await using var factory = new WebUiApplicationFactory();
        var recorder = new ListObjectsRecorder();
        await using var isolated = await CreateRecordingClientAsync(factory, recorder);
        var client = isolated.Client;

        // Two large common-prefix runs plus a top-level object. A common-prefix run spans more
        // objects than maxKeys, so the collector must consume a full run before finalizing it.
        var keys = new List<string> { "root.txt" };
        keys.AddRange(Enumerable.Range(0, 5).Select(static index => $"docs/a{index}.txt"));
        keys.AddRange(Enumerable.Range(0, 5).Select(static index => $"images/b{index}.png"));
        await SeedObjectsAsync(client, "pushdown-delim", keys);
        recorder.Reset();

        // Page 1: expect first entry = "docs/" common prefix (keys sort before "images/" and "root.txt").
        var firstResponse = await client.GetAsync("/integrated-s3/pushdown-delim?list-type=2&delimiter=/&max-keys=1");
        var firstDocument = XDocument.Parse(await firstResponse.Content.ReadAsStringAsync());
        Assert.Equal("true", GetValue(firstDocument, "IsTruncated"));
        Assert.Equal(new[] { "docs/" }, CommonPrefixes(firstDocument));
        Assert.Empty(Contents(firstDocument));
        // Next token is the LAST object key consumed into the docs/ run, i.e. docs/a4.txt.
        Assert.Equal("docs/a4.txt", GetValue(firstDocument, "NextContinuationToken"));

        // Page 2: resume from docs/a4.txt -> images/ common prefix next.
        var secondResponse = await client.GetAsync(
            $"/integrated-s3/pushdown-delim?list-type=2&delimiter=/&max-keys=1&continuation-token={Uri.EscapeDataString("docs/a4.txt")}");
        var secondDocument = XDocument.Parse(await secondResponse.Content.ReadAsStringAsync());
        Assert.Equal("true", GetValue(secondDocument, "IsTruncated"));
        Assert.Equal(new[] { "images/" }, CommonPrefixes(secondDocument));
        Assert.Equal("images/b4.png", GetValue(secondDocument, "NextContinuationToken"));

        // Page 3: resume from images/b4.png -> the top-level object "root.txt".
        var thirdResponse = await client.GetAsync(
            $"/integrated-s3/pushdown-delim?list-type=2&delimiter=/&max-keys=1&continuation-token={Uri.EscapeDataString("images/b4.png")}");
        var thirdDocument = XDocument.Parse(await thirdResponse.Content.ReadAsStringAsync());
        Assert.Equal("false", GetValue(thirdDocument, "IsTruncated"));
        Assert.Empty(CommonPrefixes(thirdDocument));
        Assert.Equal(new[] { "root.txt" }, Contents(thirdDocument));
    }

    [Fact]
    public async Task ListObjectsV2_Delimiter_MatchesFullEnumerationForWholeBucket()
    {
        await using var factory = new WebUiApplicationFactory();
        var recorder = new ListObjectsRecorder();
        await using var isolated = await CreateRecordingClientAsync(factory, recorder);
        var client = isolated.Client;

        var keys = new List<string>();
        keys.AddRange(Enumerable.Range(0, 3).Select(static index => $"a/{index}.txt"));
        keys.AddRange(Enumerable.Range(0, 3).Select(static index => $"b/{index}.txt"));
        keys.Add("z.txt");
        await SeedObjectsAsync(client, "pushdown-delim-full", keys);

        var response = await client.GetAsync("/integrated-s3/pushdown-delim-full?list-type=2&delimiter=/&max-keys=1000");
        var document = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("false", GetValue(document, "IsTruncated"));
        Assert.Equal(new[] { "a/", "b/" }, CommonPrefixes(document));
        Assert.Equal(new[] { "z.txt" }, Contents(document));
    }

    [Fact]
    public async Task ListObjectsV2_MaxKeysBoundary_ExactPageIsNotTruncated()
    {
        await using var factory = new WebUiApplicationFactory();
        var recorder = new ListObjectsRecorder();
        await using var isolated = await CreateRecordingClientAsync(factory, recorder);
        var client = isolated.Client;

        var keys = new[] { "a.txt", "b.txt", "c.txt" };
        await SeedObjectsAsync(client, "pushdown-boundary", keys);
        recorder.Reset();

        // Exactly max-keys objects -> not truncated.
        var exactResponse = await client.GetAsync("/integrated-s3/pushdown-boundary?list-type=2&max-keys=3");
        var exactDocument = XDocument.Parse(await exactResponse.Content.ReadAsStringAsync());
        Assert.Equal("false", GetValue(exactDocument, "IsTruncated"));
        Assert.Equal(new[] { "a.txt", "b.txt", "c.txt" }, Contents(exactDocument));
        Assert.Empty(exactDocument.Root!.Elements(S3Ns + "NextContinuationToken"));

        // One fewer than available -> truncated with a next token.
        var truncatedResponse = await client.GetAsync("/integrated-s3/pushdown-boundary?list-type=2&max-keys=2");
        var truncatedDocument = XDocument.Parse(await truncatedResponse.Content.ReadAsStringAsync());
        Assert.Equal("true", GetValue(truncatedDocument, "IsTruncated"));
        Assert.Equal("b.txt", GetValue(truncatedDocument, "NextContinuationToken"));
    }

    private sealed record ListObjectsCall(ListObjectsRequest Request, int ObjectsRead)
    {
        public int ObjectsRead { get; set; } = ObjectsRead;
    }

    private sealed class ListObjectsRecorder
    {
        public ConcurrentQueue<ListObjectsCall> Calls { get; } = new();

        public void Reset() => Calls.Clear();

        public ListObjectsCall Record(ListObjectsRequest request)
        {
            var call = new ListObjectsCall(request, 0);
            Calls.Enqueue(call);
            return call;
        }
    }

    /// <summary>
    /// A <see cref="DispatchProxy"/> over <see cref="IStorageService"/> that records the
    /// <see cref="ListObjectsRequest"/> issued and counts how many objects are pulled from the
    /// returned stream, forwarding every other member to the real implementation.
    /// </summary>
    private class RecordingStorageService : DispatchProxy
    {
        private IStorageService _inner = null!;
        private ListObjectsRecorder _recorder = null!;

        internal static IStorageService Create(IStorageService inner, ListObjectsRecorder recorder)
        {
            var proxy = Create<IStorageService, RecordingStorageService>();
            var recording = (RecordingStorageService)(object)proxy;
            recording._inner = inner;
            recording._recorder = recorder;
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod is null)
            {
                throw new ArgumentNullException(nameof(targetMethod));
            }

            if (targetMethod.Name == nameof(IStorageService.ListObjectsAsync)
                && args is [ListObjectsRequest request, CancellationToken cancellationToken])
            {
                var call = _recorder.Record(request);
                var inner = _inner.ListObjectsAsync(request, cancellationToken);
                return CountingStream(inner, call, cancellationToken);
            }

            return targetMethod.Invoke(_inner, args);
        }

        private static async IAsyncEnumerable<ObjectInfo> CountingStream(
            IAsyncEnumerable<ObjectInfo> source,
            ListObjectsCall call,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var item in source.WithCancellation(cancellationToken))
            {
                call.ObjectsRead++;
                yield return item;
            }
        }
    }
}

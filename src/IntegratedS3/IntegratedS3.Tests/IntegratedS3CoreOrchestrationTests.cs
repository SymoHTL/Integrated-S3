using System.Text;
using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Persistence;
using IntegratedS3.Core.Services;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3CoreOrchestrationTests
{
    [Fact]
    public async Task OrchestratedStorageService_PersistsCatalogEntriesUsingEntityFramework()
    {
        await using var fixture = new CoreStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var catalogStore = fixture.Services.GetRequiredService<IStorageCatalogStore>();

        var createBucket = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "catalog-bucket"
        });
        Assert.True(createBucket.IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("catalog payload"));
        var putObject = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "catalog-bucket",
            Key = "docs/catalog.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "ef"
            }
        });
        Assert.True(putObject.IsSuccess);

        var buckets = await catalogStore.ListBucketsAsync("catalog-disk");
        var bucket = Assert.Single(buckets);
        Assert.Equal("catalog-bucket", bucket.BucketName);

        var objects = await catalogStore.ListObjectsAsync("catalog-disk", "catalog-bucket");
        var @object = Assert.Single(objects);
        Assert.Equal("docs/catalog.txt", @object.Key);
        Assert.Equal("text/plain", @object.ContentType);
        Assert.Equal("ef", @object.Metadata!["source"]);

        Assert.True(File.Exists(fixture.DatabasePath));
    }

    [Fact]
    public async Task ConsumerCanOverrideCatalogStoreImplementation()
    {
        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true);
        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var catalogStore = fixture.Services.GetRequiredService<IStorageCatalogStore>();

        Assert.IsType<FakeCatalogStore>(catalogStore);

        var createBucket = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "override-bucket"
        });
        Assert.True(createBucket.IsSuccess);

        var fakeCatalogStore = (FakeCatalogStore)catalogStore;
        var storedBucket = Assert.Single(fakeCatalogStore.Buckets);
        Assert.Equal("override-bucket", storedBucket.BucketName);
        Assert.Equal("catalog-disk", storedBucket.ProviderName);
    }

    [Fact]
    public async Task OrchestratedStorageService_CopyObject_UpdatesCatalog()
    {
        await using var fixture = new CoreStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var catalogStore = fixture.Services.GetRequiredService<IStorageCatalogStore>();

        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "source-bucket" });
        await storageService.CreateBucketAsync(new CreateBucketRequest { BucketName = "target-bucket" });

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("copy payload"));
        var putObject = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "source-bucket",
            Key = "docs/source.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });
        Assert.True(putObject.IsSuccess);

        var copyObject = await storageService.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "source-bucket",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "target-bucket",
            DestinationKey = "docs/copied.txt"
        });
        Assert.True(copyObject.IsSuccess);

        var objects = await catalogStore.ListObjectsAsync("catalog-disk", "target-bucket");
        var copiedObject = Assert.Single(objects);
        Assert.Equal("docs/copied.txt", copiedObject.Key);
        Assert.Equal("text/plain", copiedObject.ContentType);
    }

    [Fact]
    public async Task OrchestratedStorageService_UsesAmbientPrincipalForAuthorization()
    {
        await using var fixture = new CoreStorageFixture(configureServices: services => {
            services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var requestContextAccessor = fixture.Services.GetRequiredService<IIntegratedS3RequestContextAccessor>();

        requestContextAccessor.Current = new IntegratedS3RequestContext
        {
            Principal = CreatePrincipal("storage.write")
        };

        var createBucket = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "authorized-bucket"
        });
        Assert.True(createBucket.IsSuccess);

        requestContextAccessor.Current = new IntegratedS3RequestContext
        {
            Principal = CreatePrincipal("storage.read")
        };

        await using var deniedUploadStream = new MemoryStream(Encoding.UTF8.GetBytes("denied payload"));
        var deniedPut = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "authorized-bucket",
            Key = "docs/denied.txt",
            Content = deniedUploadStream,
            ContentType = "text/plain"
        });

        Assert.False(deniedPut.IsSuccess);
        Assert.Equal(StorageErrorCode.AccessDenied, deniedPut.Error!.Code);

        requestContextAccessor.Current = new IntegratedS3RequestContext
        {
            Principal = CreatePrincipal("storage.write", "storage.read")
        };

        await using var allowedUploadStream = new MemoryStream(Encoding.UTF8.GetBytes("allowed payload"));
        var allowedPut = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "authorized-bucket",
            Key = "docs/allowed.txt",
            Content = allowedUploadStream,
            ContentType = "text/plain"
        });

        Assert.True(allowedPut.IsSuccess);

        requestContextAccessor.Current = null;
    }

    [Fact]
    public async Task OrchestratedStorageService_PreferPrimary_FallsBackToHealthyReplica_WhenPrimaryIsMarkedUnhealthy()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        primaryBackend.AddObject("read-bucket", "docs/read.txt", "primary payload");

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("read-bucket", "docs/read.txt", "replica payload");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ReadRoutingMode = StorageReadRoutingMode.PreferPrimary;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
            services.AddSingleton<IStorageBackendHealthEvaluator>(new ConfigurableStorageBackendHealthEvaluator(new Dictionary<string, StorageBackendHealthStatus>(StringComparer.Ordinal)
            {
                [primaryBackend.Name] = StorageBackendHealthStatus.Unhealthy,
                [replicaBackend.Name] = StorageBackendHealthStatus.Healthy
            }));
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var getObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(getObject.IsSuccess);
        await using var response = getObject.Value!;
        Assert.Equal("replica payload", await ReadContentAsStringAsync(response.Content));
        Assert.Equal(0, primaryBackend.GetObjectCallCount);
        Assert.Equal(1, replicaBackend.GetObjectCallCount);
    }

    [Fact]
    public async Task OrchestratedStorageService_PreferHealthyReplica_ReadsFromReplicaBeforeHealthyPrimary()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        primaryBackend.AddObject("read-bucket", "docs/read.txt", "primary payload");

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("read-bucket", "docs/read.txt", "replica payload");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ReadRoutingMode = StorageReadRoutingMode.PreferHealthyReplica;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
            services.AddSingleton<IStorageBackendHealthEvaluator>(new ConfigurableStorageBackendHealthEvaluator(new Dictionary<string, StorageBackendHealthStatus>(StringComparer.Ordinal)
            {
                [primaryBackend.Name] = StorageBackendHealthStatus.Healthy,
                [replicaBackend.Name] = StorageBackendHealthStatus.Healthy
            }));
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var getObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(getObject.IsSuccess);
        await using var response = getObject.Value!;
        Assert.Equal("replica payload", await ReadContentAsStringAsync(response.Content));
        Assert.Equal(0, primaryBackend.GetObjectCallCount);
        Assert.Equal(1, replicaBackend.GetObjectCallCount);
    }

    [Fact]
    public async Task OrchestratedStorageService_PreferPrimary_FailsOverWhenPrimaryReadReturnsProviderUnavailable()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true)
        {
            FailGetObjectWithProviderUnavailable = true
        };
        primaryBackend.AddObject("read-bucket", "docs/read.txt", "primary payload");

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("read-bucket", "docs/read.txt", "replica payload");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ReadRoutingMode = StorageReadRoutingMode.PreferPrimary;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
            services.AddSingleton<IStorageBackendHealthEvaluator>(new ConfigurableStorageBackendHealthEvaluator(new Dictionary<string, StorageBackendHealthStatus>(StringComparer.Ordinal)
            {
                [primaryBackend.Name] = StorageBackendHealthStatus.Healthy,
                [replicaBackend.Name] = StorageBackendHealthStatus.Healthy
            }));
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var getObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(getObject.IsSuccess);
        await using var response = getObject.Value!;
        Assert.Equal("replica payload", await ReadContentAsStringAsync(response.Content));
        Assert.Equal(1, primaryBackend.GetObjectCallCount);
        Assert.Equal(1, replicaBackend.GetObjectCallCount);
    }

    [Fact]
    public async Task OrchestratedStorageService_WriteThroughAll_ReplicatesBucketsAndObjects()
    {
        await using var fixture = new CoreStorageFixture(configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteThroughAll;
            });
            services.AddDiskStorage(new DiskStorageOptions
            {
                ProviderName = "replica-disk",
                RootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Core.Tests", Guid.NewGuid().ToString("N")),
                CreateRootDirectory = true,
                IsPrimary = false
            });
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var backends = fixture.Services.GetServices<IStorageBackend>().OrderBy(static backend => backend.Name, StringComparer.Ordinal).ToArray();

        Assert.Equal(2, backends.Length);

        var createBucket = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "mirrored-bucket"
        });
        Assert.True(createBucket.IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("mirrored payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "mirrored-bucket",
            Key = "docs/mirrored.txt",
            Content = uploadStream,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["replicated"] = "true"
            }
        });
        Assert.True(putResult.IsSuccess);

        foreach (var backend in backends) {
            var bucketResult = await backend.HeadBucketAsync("mirrored-bucket");
            Assert.True(bucketResult.IsSuccess);

            var headObject = await backend.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = "mirrored-bucket",
                Key = "docs/mirrored.txt"
            });

            Assert.True(headObject.IsSuccess);
            Assert.Equal("true", headObject.Value!.Metadata!["replicated"]);
        }
    }

    [Fact]
    public async Task OrchestratedStorageService_WriteThroughAll_ReturnsFailureWhenReplicaWriteFails()
    {
        await using var fixture = new CoreStorageFixture(configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteThroughAll;
            });
            services.AddSingleton<IStorageBackend, FailingReplicaStorageBackend>();
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var primaryBackend = fixture.Services.GetServices<IStorageBackend>().Single(static backend => backend.Name == "catalog-disk");

        var createBucket = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "partial-bucket"
        });
        Assert.True(createBucket.IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("partial payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "partial-bucket",
            Key = "docs/partial.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.False(putResult.IsSuccess);
        Assert.Equal(StorageErrorCode.ProviderUnavailable, putResult.Error!.Code);
        Assert.Equal("failing-replica", putResult.Error.ProviderName);

        var primaryObject = await primaryBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "partial-bucket",
            Key = "docs/partial.txt"
        });

        Assert.True(primaryObject.IsSuccess);
    }

    [Fact]
    public async Task OrchestratedStorageService_WriteThroughAll_RejectsMultipartUploads()
    {
        await using var fixture = new CoreStorageFixture(configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteThroughAll;
            });
            services.AddDiskStorage(new DiskStorageOptions
            {
                ProviderName = "replica-disk",
                RootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Core.Tests", Guid.NewGuid().ToString("N")),
                CreateRootDirectory = true,
                IsPrimary = false
            });
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();

        var createBucket = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "multipart-rejected"
        });
        Assert.True(createBucket.IsSuccess);

        var initiateResult = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "multipart-rejected",
            Key = "docs/blocked.txt"
        });

        Assert.False(initiateResult.IsSuccess);
        Assert.Equal(StorageErrorCode.UnsupportedCapability, initiateResult.Error!.Code);
    }

    private sealed class CoreStorageFixture : IAsyncDisposable
    {
        public CoreStorageFixture(bool overrideCatalogStore = false, bool addDefaultDiskStorage = true, Action<IServiceCollection>? configureServices = null)
        {
            RootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Core.Tests", Guid.NewGuid().ToString("N"));
            DatabasePath = Path.Combine(RootPath, "catalog.db");

            var services = new ServiceCollection();
            if (overrideCatalogStore) {
                services.AddSingleton<IStorageCatalogStore, FakeCatalogStore>();
            }
            else {
                services.AddDbContext<TestCatalogDbContext>(options => options.UseSqlite($"Data Source={DatabasePath}"));
            }

            services.AddIntegratedS3Core();
            if (!overrideCatalogStore) {
                services.AddEntityFrameworkStorageCatalog<TestCatalogDbContext>(options => {
                    options.EnsureCreated = true;
                });
            }
            if (addDefaultDiskStorage) {
                services.AddDiskStorage(new DiskStorageOptions
                {
                    ProviderName = "catalog-disk",
                    RootPath = RootPath,
                    CreateRootDirectory = true
                });
            }
            configureServices?.Invoke(services);

            Services = services.BuildServiceProvider();
        }

        public string RootPath { get; }

        public string DatabasePath { get; }

        public ServiceProvider Services { get; }

        public async ValueTask DisposeAsync()
        {
            await Services.DisposeAsync();

            if (Directory.Exists(RootPath)) {
                for (var attempt = 0; attempt < 5; attempt++) {
                    try {
                        Directory.Delete(RootPath, recursive: true);
                        break;
                    }
                    catch (IOException) {
                        if (attempt < 4) {
                            await Task.Delay(100);
                        }
                    }
                    catch (UnauthorizedAccessException) {
                        if (attempt < 4) {
                            await Task.Delay(100);
                        }
                    }
                }

                try {
                    if (Directory.Exists(RootPath)) {
                        Directory.Delete(RootPath, recursive: true);
                    }
                }
                catch (IOException) {
                }
                catch (UnauthorizedAccessException) {
                }
            }
        }
    }

    private static ClaimsPrincipal CreatePrincipal(params string[] scopes)
    {
        var claims = scopes.Select(static scope => new Claim("scope", scope));
        return new ClaimsPrincipal(new ClaimsIdentity(claims, "Tests"));
    }

    private static async Task<string> ReadContentAsStringAsync(Stream content)
    {
        using var reader = new StreamReader(content, Encoding.UTF8, leaveOpen: true);
        return await reader.ReadToEndAsync();
    }

    private sealed class ScopeBasedIntegratedS3AuthorizationService : IIntegratedS3AuthorizationService
    {
        public ValueTask<StorageResult> AuthorizeAsync(ClaimsPrincipal principal, StorageAuthorizationRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var requiredScope = request.Operation switch
            {
                StorageOperationType.ListBuckets => "storage.read",
                StorageOperationType.HeadBucket => "storage.read",
                StorageOperationType.ListObjects => "storage.read",
                StorageOperationType.GetObject => "storage.read",
                StorageOperationType.HeadObject => "storage.read",
                _ => "storage.write"
            };

            if (principal.HasClaim("scope", requiredScope)) {
                return ValueTask.FromResult(StorageResult.Success());
            }

            return ValueTask.FromResult(StorageResult.Failure(new StorageError
            {
                Code = StorageErrorCode.AccessDenied,
                Message = $"Missing required scope '{requiredScope}'.",
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                SuggestedHttpStatusCode = 403
            }));
        }
    }

    private sealed class ConfigurableStorageBackendHealthEvaluator(IReadOnlyDictionary<string, StorageBackendHealthStatus> statuses) : IStorageBackendHealthEvaluator
    {
        public ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(statuses.TryGetValue(backend.Name, out var status)
                ? status
                : StorageBackendHealthStatus.Healthy);
        }
    }

    private sealed class FailingReplicaStorageBackend : IStorageBackend
    {
        public string Name => "failing-replica";

        public string Kind => "test";

        public bool IsPrimary => false;

        public string? Description => "Replica backend that fails object writes for orchestration tests.";

        public ValueTask<IntegratedS3.Abstractions.Capabilities.StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new IntegratedS3.Abstractions.Capabilities.StorageCapabilities
            {
                BucketOperations = IntegratedS3.Abstractions.Capabilities.StorageCapabilitySupport.Native,
                ObjectCrud = IntegratedS3.Abstractions.Capabilities.StorageCapabilitySupport.Native,
                CopyOperations = IntegratedS3.Abstractions.Capabilities.StorageCapabilitySupport.Native
            });
        }

        public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketInfo>.Success(new BucketInfo
            {
                Name = request.BucketName,
                CreatedAtUtc = DateTimeOffset.UtcNow,
                VersioningEnabled = false
            }));
        }

        public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.BucketNotFound,
                Message = $"Bucket '{bucketName}' was not found.",
                BucketName = bucketName,
                ProviderName = Name,
                SuggestedHttpStatusCode = 404
            }));
        }

        public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult.Success());
        }

        public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<GetObjectResponse>.Failure(new StorageError
            {
                Code = StorageErrorCode.ObjectNotFound,
                Message = $"Object '{request.Key}' was not found.",
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                ProviderName = Name,
                SuggestedHttpStatusCode = 404
            }));
        }

        public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<ObjectInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.ProviderUnavailable,
                Message = "Replica copy failed.",
                BucketName = request.DestinationBucketName,
                ObjectKey = request.DestinationKey,
                ProviderName = Name,
                SuggestedHttpStatusCode = 503
            }));
        }

        public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<ObjectInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.ProviderUnavailable,
                Message = "Replica write failed.",
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                ProviderName = Name,
                SuggestedHttpStatusCode = 503
            }));
        }

        public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<MultipartUploadInfo>.Failure(StorageError.Unsupported("Multipart uploads are not supported by the failing replica.", request.BucketName, request.Key)));
        }

        public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<MultipartUploadPart>.Failure(StorageError.Unsupported("Multipart uploads are not supported by the failing replica.", request.BucketName, request.Key)));
        }

        public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<ObjectInfo>.Failure(StorageError.Unsupported("Multipart uploads are not supported by the failing replica.", request.BucketName, request.Key)));
        }

        public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Multipart uploads are not supported by the failing replica.", request.BucketName, request.Key)));
        }

        public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<ObjectInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.ObjectNotFound,
                Message = $"Object '{request.Key}' was not found.",
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                ProviderName = Name,
                SuggestedHttpStatusCode = 404
            }));
        }

        public ValueTask<StorageResult> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult.Success());
        }
    }

    private sealed class InMemoryStorageBackend(string name, bool isPrimary = false) : IStorageBackend
    {
        private readonly Dictionary<string, BucketInfo> _buckets = new(StringComparer.Ordinal);
        private readonly Dictionary<(string BucketName, string Key), StoredObject> _objects = new();

        public string Name => name;

        public string Kind => "test-memory";

        public bool IsPrimary => isPrimary;

        public string? Description => $"In-memory backend '{name}'.";

        public int GetObjectCallCount { get; private set; }

        public bool FailGetObjectWithProviderUnavailable { get; set; }

        public void AddObject(string bucketName, string key, string content, string contentType = "text/plain", IReadOnlyDictionary<string, string>? metadata = null)
        {
            if (!_buckets.ContainsKey(bucketName)) {
                _buckets[bucketName] = new BucketInfo
                {
                    Name = bucketName,
                    CreatedAtUtc = DateTimeOffset.UtcNow,
                    VersioningEnabled = false
                };
            }

            var bytes = Encoding.UTF8.GetBytes(content);
            _objects[(bucketName, key)] = new StoredObject
            {
                Content = bytes,
                Info = new ObjectInfo
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentLength = bytes.Length,
                    ContentType = contentType,
                    ETag = $"{bucketName}:{key}:{bytes.Length}",
                    LastModifiedUtc = DateTimeOffset.UtcNow,
                    Metadata = metadata
                }
            };
        }

        public ValueTask<IntegratedS3.Abstractions.Capabilities.StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new IntegratedS3.Abstractions.Capabilities.StorageCapabilities
            {
                BucketOperations = IntegratedS3.Abstractions.Capabilities.StorageCapabilitySupport.Native,
                ObjectCrud = IntegratedS3.Abstractions.Capabilities.StorageCapabilitySupport.Native,
                CopyOperations = IntegratedS3.Abstractions.Capabilities.StorageCapabilitySupport.Native
            });
        }

        public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var bucket in _buckets.Values) {
                cancellationToken.ThrowIfCancellationRequested();
                yield return bucket;
                await Task.Yield();
            }
        }

        public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_buckets.ContainsKey(request.BucketName)) {
                return ValueTask.FromResult(StorageResult<BucketInfo>.Failure(new StorageError
                {
                    Code = StorageErrorCode.BucketAlreadyExists,
                    Message = $"Bucket '{request.BucketName}' already exists.",
                    BucketName = request.BucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 409
                }));
            }

            var bucket = new BucketInfo
            {
                Name = request.BucketName,
                CreatedAtUtc = DateTimeOffset.UtcNow,
                VersioningEnabled = false
            };
            _buckets[request.BucketName] = bucket;
            return ValueTask.FromResult(StorageResult<BucketInfo>.Success(bucket));
        }

        public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult(_buckets.TryGetValue(bucketName, out var bucket)
                ? StorageResult<BucketInfo>.Success(bucket)
                : StorageResult<BucketInfo>.Failure(new StorageError
                {
                    Code = StorageErrorCode.BucketNotFound,
                    Message = $"Bucket '{bucketName}' was not found.",
                    BucketName = bucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
        }

        public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_buckets.Remove(request.BucketName)) {
                return ValueTask.FromResult(StorageResult.Failure(new StorageError
                {
                    Code = StorageErrorCode.BucketNotFound,
                    Message = $"Bucket '{request.BucketName}' was not found.",
                    BucketName = request.BucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            foreach (var key in _objects.Keys.Where(entry => string.Equals(entry.BucketName, request.BucketName, StringComparison.Ordinal)).ToArray()) {
                _objects.Remove(key);
            }

            return ValueTask.FromResult(StorageResult.Success());
        }

        public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var entry in _objects.Where(entry => string.Equals(entry.Key.BucketName, request.BucketName, StringComparison.Ordinal)).OrderBy(entry => entry.Key.Key, StringComparer.Ordinal)) {
                cancellationToken.ThrowIfCancellationRequested();
                yield return entry.Value.Info;
                await Task.Yield();
            }
        }

        public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            GetObjectCallCount++;

            if (FailGetObjectWithProviderUnavailable) {
                return ValueTask.FromResult(StorageResult<GetObjectResponse>.Failure(new StorageError
                {
                    Code = StorageErrorCode.ProviderUnavailable,
                    Message = "Simulated provider outage.",
                    BucketName = request.BucketName,
                    ObjectKey = request.Key,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 503
                }));
            }

            if (!_objects.TryGetValue((request.BucketName, request.Key), out var storedObject)) {
                return ValueTask.FromResult(StorageResult<GetObjectResponse>.Failure(new StorageError
                {
                    Code = StorageErrorCode.ObjectNotFound,
                    Message = $"Object '{request.Key}' was not found in bucket '{request.BucketName}'.",
                    BucketName = request.BucketName,
                    ObjectKey = request.Key,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            return ValueTask.FromResult(StorageResult<GetObjectResponse>.Success(new GetObjectResponse
            {
                Object = storedObject.Info,
                Content = new MemoryStream(storedObject.Content, writable: false),
                TotalContentLength = storedObject.Info.ContentLength
            }));
        }

        public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_objects.TryGetValue((request.SourceBucketName, request.SourceKey), out var sourceObject)) {
                return ValueTask.FromResult(StorageResult<ObjectInfo>.Failure(new StorageError
                {
                    Code = StorageErrorCode.ObjectNotFound,
                    Message = $"Object '{request.SourceKey}' was not found in bucket '{request.SourceBucketName}'.",
                    BucketName = request.SourceBucketName,
                    ObjectKey = request.SourceKey,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            AddObject(request.DestinationBucketName, request.DestinationKey, Encoding.UTF8.GetString(sourceObject.Content), sourceObject.Info.ContentType ?? "application/octet-stream", sourceObject.Info.Metadata);
            return ValueTask.FromResult(StorageResult<ObjectInfo>.Success(_objects[(request.DestinationBucketName, request.DestinationKey)].Info));
        }

        public async ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var buffer = new MemoryStream();
            await request.Content.CopyToAsync(buffer, cancellationToken);

            if (!_buckets.ContainsKey(request.BucketName)) {
                _buckets[request.BucketName] = new BucketInfo
                {
                    Name = request.BucketName,
                    CreatedAtUtc = DateTimeOffset.UtcNow,
                    VersioningEnabled = false
                };
            }

            var bytes = buffer.ToArray();
            var objectInfo = new ObjectInfo
            {
                BucketName = request.BucketName,
                Key = request.Key,
                ContentLength = bytes.Length,
                ContentType = request.ContentType,
                ETag = $"{request.BucketName}:{request.Key}:{bytes.Length}",
                LastModifiedUtc = DateTimeOffset.UtcNow,
                Metadata = request.Metadata
            };
            _objects[(request.BucketName, request.Key)] = new StoredObject
            {
                Content = bytes,
                Info = objectInfo
            };

            return StorageResult<ObjectInfo>.Success(objectInfo);
        }

        public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<MultipartUploadInfo>.Failure(StorageError.Unsupported("Multipart uploads are not implemented by the in-memory test backend.", request.BucketName, request.Key)));
        }

        public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<MultipartUploadPart>.Failure(StorageError.Unsupported("Multipart uploads are not implemented by the in-memory test backend.", request.BucketName, request.Key)));
        }

        public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<ObjectInfo>.Failure(StorageError.Unsupported("Multipart uploads are not implemented by the in-memory test backend.", request.BucketName, request.Key)));
        }

        public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Multipart uploads are not implemented by the in-memory test backend.", request.BucketName, request.Key)));
        }

        public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult(_objects.TryGetValue((request.BucketName, request.Key), out var storedObject)
                ? StorageResult<ObjectInfo>.Success(storedObject.Info)
                : StorageResult<ObjectInfo>.Failure(new StorageError
                {
                    Code = StorageErrorCode.ObjectNotFound,
                    Message = $"Object '{request.Key}' was not found in bucket '{request.BucketName}'.",
                    BucketName = request.BucketName,
                    ObjectKey = request.Key,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
        }

        public ValueTask<StorageResult> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_objects.Remove((request.BucketName, request.Key))) {
                return ValueTask.FromResult(StorageResult.Failure(new StorageError
                {
                    Code = StorageErrorCode.ObjectNotFound,
                    Message = $"Object '{request.Key}' was not found in bucket '{request.BucketName}'.",
                    BucketName = request.BucketName,
                    ObjectKey = request.Key,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            return ValueTask.FromResult(StorageResult.Success());
        }

        private sealed class StoredObject
        {
            public required byte[] Content { get; init; }

            public required ObjectInfo Info { get; init; }
        }
    }

    private sealed class TestCatalogDbContext(DbContextOptions<TestCatalogDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.MapIntegratedS3Catalog();
        }
    }

    private sealed class FakeCatalogStore : IStorageCatalogStore
    {
        public List<StoredBucketEntry> Buckets { get; } = [];

        public List<StoredObjectEntry> Objects { get; } = [];

        public ValueTask UpsertBucketAsync(string providerName, Abstractions.Models.BucketInfo bucket, CancellationToken cancellationToken = default)
        {
            Buckets.RemoveAll(existing => existing.ProviderName == providerName && existing.BucketName == bucket.Name);
            Buckets.Add(new StoredBucketEntry
            {
                ProviderName = providerName,
                BucketName = bucket.Name,
                CreatedAtUtc = bucket.CreatedAtUtc,
                VersioningEnabled = bucket.VersioningEnabled,
                LastSyncedAtUtc = DateTimeOffset.UtcNow
            });
            return ValueTask.CompletedTask;
        }

        public ValueTask RemoveBucketAsync(string providerName, string bucketName, CancellationToken cancellationToken = default)
        {
            Buckets.RemoveAll(existing => existing.ProviderName == providerName && existing.BucketName == bucketName);
            Objects.RemoveAll(existing => existing.ProviderName == providerName && existing.BucketName == bucketName);
            return ValueTask.CompletedTask;
        }

        public ValueTask<IReadOnlyList<StoredBucketEntry>> ListBucketsAsync(string? providerName = null, CancellationToken cancellationToken = default)
        {
            IReadOnlyList<StoredBucketEntry> result = string.IsNullOrWhiteSpace(providerName)
                ? Buckets.ToArray()
                : Buckets.Where(existing => existing.ProviderName == providerName).ToArray();
            return ValueTask.FromResult(result);
        }

        public ValueTask UpsertObjectAsync(string providerName, Abstractions.Models.ObjectInfo @object, CancellationToken cancellationToken = default)
        {
            Objects.RemoveAll(existing => existing.ProviderName == providerName && existing.BucketName == @object.BucketName && existing.Key == @object.Key);
            Objects.Add(new StoredObjectEntry
            {
                ProviderName = providerName,
                BucketName = @object.BucketName,
                Key = @object.Key,
                ContentLength = @object.ContentLength,
                ContentType = @object.ContentType,
                ETag = @object.ETag,
                LastModifiedUtc = @object.LastModifiedUtc,
                Metadata = @object.Metadata,
                LastSyncedAtUtc = DateTimeOffset.UtcNow
            });
            return ValueTask.CompletedTask;
        }

        public ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, CancellationToken cancellationToken = default)
        {
            Objects.RemoveAll(existing => existing.ProviderName == providerName && existing.BucketName == bucketName && existing.Key == key);
            return ValueTask.CompletedTask;
        }

        public ValueTask<IReadOnlyList<StoredObjectEntry>> ListObjectsAsync(string? providerName = null, string? bucketName = null, CancellationToken cancellationToken = default)
        {
            IEnumerable<StoredObjectEntry> result = Objects;
            if (!string.IsNullOrWhiteSpace(providerName)) {
                result = result.Where(existing => existing.ProviderName == providerName);
            }

            if (!string.IsNullOrWhiteSpace(bucketName)) {
                result = result.Where(existing => existing.BucketName == bucketName);
            }

            return ValueTask.FromResult<IReadOnlyList<StoredObjectEntry>>(result.ToArray());
        }
    }
}

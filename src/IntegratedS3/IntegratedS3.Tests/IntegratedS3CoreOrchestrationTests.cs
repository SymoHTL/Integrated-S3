using System.Collections.Concurrent;
using System.Security.Claims;
using System.Text;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Persistence;
using IntegratedS3.Core.Services;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
    public async Task EntityFrameworkRegistration_ProvidesPlatformManagedMultipartStateStore()
    {
        await using var fixture = new CoreStorageFixture();
        var multipartStateStore = fixture.Services.GetRequiredService<IStorageMultipartStateStore>();

        var state = new MultipartUploadState
        {
            BucketName = "catalog-bucket",
            Key = "docs/multipart.txt",
            UploadId = "upload-123",
            InitiatedAtUtc = DateTimeOffset.UtcNow,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "ef"
            }
        };

        await multipartStateStore.UpsertMultipartUploadStateAsync("catalog-disk", state);

        var storedState = await multipartStateStore.GetMultipartUploadStateAsync("catalog-disk", "catalog-bucket", "docs/multipart.txt", "upload-123");
        Assert.NotNull(storedState);
        Assert.Equal("text/plain", storedState!.ContentType);
        Assert.Equal("ef", storedState.Metadata!["source"]);

        await multipartStateStore.RemoveMultipartUploadStateAsync("catalog-disk", "catalog-bucket", "docs/multipart.txt", "upload-123");
        Assert.Null(await multipartStateStore.GetMultipartUploadStateAsync("catalog-disk", "catalog-bucket", "docs/multipart.txt", "upload-123"));
    }

    [Fact]
    public async Task EntityFrameworkRegistration_ListsPlatformManagedMultipartStateStoreEntriesInStableOrder()
    {
        await using var fixture = new CoreStorageFixture();
        var multipartStateStore = fixture.Services.GetRequiredService<IStorageMultipartStateStore>();
        var baseInitiatedAtUtc = DateTimeOffset.UtcNow;

        await multipartStateStore.UpsertMultipartUploadStateAsync("catalog-disk", new MultipartUploadState
        {
            BucketName = "catalog-bucket",
            Key = "docs/alpha.txt",
            UploadId = "upload-002",
            InitiatedAtUtc = baseInitiatedAtUtc.AddSeconds(1)
        });
        await multipartStateStore.UpsertMultipartUploadStateAsync("catalog-disk", new MultipartUploadState
        {
            BucketName = "catalog-bucket",
            Key = "docs/alpha.txt",
            UploadId = "upload-001",
            InitiatedAtUtc = baseInitiatedAtUtc
        });
        await multipartStateStore.UpsertMultipartUploadStateAsync("catalog-disk", new MultipartUploadState
        {
            BucketName = "catalog-bucket",
            Key = "videos/clip.txt",
            UploadId = "upload-999",
            InitiatedAtUtc = baseInitiatedAtUtc.AddSeconds(2)
        });

        var uploads = await multipartStateStore.ListMultipartUploadStatesAsync("catalog-disk", "catalog-bucket", prefix: "docs/").ToArrayAsync();

        Assert.Collection(
            uploads,
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal("upload-001", upload.UploadId);
            },
            upload => {
                Assert.Equal("docs/alpha.txt", upload.Key);
                Assert.Equal("upload-002", upload.UploadId);
            });
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
    public async Task OrchestratedStorageService_PreferPrimary_CachesTransientReadFailures_ToAvoidRepeatedPrimaryAttempts()
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
                options.BackendHealth.UnhealthySnapshotTtl = TimeSpan.FromMinutes(5);
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var firstRead = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(firstRead.IsSuccess);
        await using (var response = firstRead.Value!) {
            Assert.Equal("replica payload", await ReadContentAsStringAsync(response.Content));
        }

        Assert.Equal(1, primaryBackend.GetObjectCallCount);
        Assert.Equal(1, replicaBackend.GetObjectCallCount);

        primaryBackend.FailGetObjectWithProviderUnavailable = false;

        var secondRead = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(secondRead.IsSuccess);
        await using (var response = secondRead.Value!) {
            Assert.Equal("replica payload", await ReadContentAsStringAsync(response.Content));
        }

        Assert.Equal(1, primaryBackend.GetObjectCallCount);
        Assert.Equal(2, replicaBackend.GetObjectCallCount);
    }

    [Fact]
    public async Task OrchestratedStorageService_ActiveProbes_CanDriveReadRoutingAndRecovery()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        primaryBackend.AddObject("read-bucket", "docs/read.txt", "primary payload");

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("read-bucket", "docs/read.txt", "replica payload");

        var healthProbe = new ConfigurableStorageBackendHealthProbe(new Dictionary<string, StorageBackendHealthStatus>(StringComparer.Ordinal)
        {
            [primaryBackend.Name] = StorageBackendHealthStatus.Unhealthy,
            [replicaBackend.Name] = StorageBackendHealthStatus.Healthy
        });

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ReadRoutingMode = StorageReadRoutingMode.PreferPrimary;
                options.BackendHealth.EnableActiveProbing = true;
                options.BackendHealth.HealthySnapshotTtl = TimeSpan.Zero;
                options.BackendHealth.UnhealthySnapshotTtl = TimeSpan.Zero;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
            services.AddSingleton<IStorageBackendHealthProbe>(healthProbe);
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var firstRead = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(firstRead.IsSuccess);
        await using (var response = firstRead.Value!) {
            Assert.Equal("replica payload", await ReadContentAsStringAsync(response.Content));
        }

        Assert.Equal(0, primaryBackend.GetObjectCallCount);
        Assert.Equal(1, replicaBackend.GetObjectCallCount);

        healthProbe.SetStatus(primaryBackend.Name, StorageBackendHealthStatus.Healthy);

        var secondRead = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(secondRead.IsSuccess);
        await using (var response = secondRead.Value!) {
            Assert.Equal("primary payload", await ReadContentAsStringAsync(response.Content));
        }

        Assert.Equal(1, primaryBackend.GetObjectCallCount);
        Assert.Equal(1, replicaBackend.GetObjectCallCount);
    }

    [Fact]
    public async Task OrchestratedStorageService_ActiveProbes_DoNotOverrideStaticHealthEvaluators()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        primaryBackend.AddObject("read-bucket", "docs/read.txt", "primary payload");

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("read-bucket", "docs/read.txt", "replica payload");

        var healthProbe = new ConfigurableStorageBackendHealthProbe(new Dictionary<string, StorageBackendHealthStatus>(StringComparer.Ordinal)
        {
            [primaryBackend.Name] = StorageBackendHealthStatus.Healthy,
            [replicaBackend.Name] = StorageBackendHealthStatus.Healthy
        });

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ReadRoutingMode = StorageReadRoutingMode.PreferPrimary;
                options.BackendHealth.EnableActiveProbing = true;
                options.BackendHealth.HealthySnapshotTtl = TimeSpan.Zero;
                options.BackendHealth.UnhealthySnapshotTtl = TimeSpan.Zero;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
            services.AddSingleton<IStorageBackendHealthProbe>(healthProbe);
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
    public async Task OrchestratedStorageService_PreferPrimary_FailsOverWhenPrimaryReadReturnsThrottled()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true)
        {
            GetObjectFailureCode = StorageErrorCode.Throttled
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
    public async Task OrchestratedStorageService_PreferPrimary_DoesNotFailOverWhenPrimaryReturnsObjectNotFound()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("read-bucket", "docs/read.txt", "replica payload");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ReadRoutingMode = StorageReadRoutingMode.PreferPrimary;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var getObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.False(getObject.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, getObject.Error!.Code);
        Assert.Equal(primaryBackend.Name, getObject.Error.ProviderName);
        Assert.Equal(1, primaryBackend.GetObjectCallCount);
        Assert.Equal(0, replicaBackend.GetObjectCallCount);
    }

    [Fact]
    public async Task OrchestratedStorageService_PreferPrimary_RetriesPrimaryAfterUnhealthySnapshotExpires()
    {
        var timeProvider = new ManualTimeProvider(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true)
        {
            GetObjectFailureCode = StorageErrorCode.ProviderUnavailable
        };
        primaryBackend.AddObject("read-bucket", "docs/read.txt", "primary payload");

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("read-bucket", "docs/read.txt", "replica payload");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ReadRoutingMode = StorageReadRoutingMode.PreferPrimary;
                options.BackendHealth.UnhealthySnapshotTtl = TimeSpan.FromMinutes(5);
            });
            services.AddSingleton<TimeProvider>(timeProvider);
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var firstRead = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(firstRead.IsSuccess);
        await using (var response = firstRead.Value!) {
            Assert.Equal("replica payload", await ReadContentAsStringAsync(response.Content));
        }

        primaryBackend.GetObjectFailureCode = null;
        timeProvider.Advance(TimeSpan.FromMinutes(5) + TimeSpan.FromMilliseconds(1));

        var secondRead = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(secondRead.IsSuccess);
        await using (var response = secondRead.Value!) {
            Assert.Equal("primary payload", await ReadContentAsStringAsync(response.Content));
        }

        Assert.Equal(2, primaryBackend.GetObjectCallCount);
        Assert.Equal(1, replicaBackend.GetObjectCallCount);
    }

    [Fact]
    public async Task OrchestratedStorageService_ActiveProbes_TreatProbeTimeoutsAsUnhealthy()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        primaryBackend.AddObject("read-bucket", "docs/read.txt", "primary payload");

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("read-bucket", "docs/read.txt", "replica payload");

        var healthProbe = new DelegatingStorageBackendHealthProbe(new Dictionary<string, Func<CancellationToken, ValueTask<StorageBackendHealthStatus>>>(StringComparer.Ordinal)
        {
            [primaryBackend.Name] = async cancellationToken => {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                return StorageBackendHealthStatus.Healthy;
            },
            [replicaBackend.Name] = static _ => ValueTask.FromResult(StorageBackendHealthStatus.Healthy)
        });

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ReadRoutingMode = StorageReadRoutingMode.PreferPrimary;
                options.BackendHealth.EnableActiveProbing = true;
                options.BackendHealth.HealthySnapshotTtl = TimeSpan.Zero;
                options.BackendHealth.UnhealthySnapshotTtl = TimeSpan.Zero;
                options.BackendHealth.ProbeTimeout = TimeSpan.FromMilliseconds(50);
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
            services.AddSingleton<IStorageBackendHealthProbe>(healthProbe);
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
    public async Task OrchestratedStorageService_WriteToPrimaryAsyncReplicas_RecordsPendingReplicaWorkUntilExplicitDispatch()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        var replicaBackend = new InMemoryStorageBackend("replica-memory");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteToPrimaryAsyncReplicas;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
            services.AddSingleton<RecordingReplicaRepairDispatcher>();
            services.AddSingleton<IStorageReplicaRepairDispatcher>(static serviceProvider => serviceProvider.GetRequiredService<RecordingReplicaRepairDispatcher>());
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var repairBacklog = fixture.Services.GetRequiredService<IStorageReplicaRepairBacklog>();
        var repairDispatcher = fixture.Services.GetRequiredService<RecordingReplicaRepairDispatcher>();
        var catalogStore = Assert.IsType<FakeCatalogStore>(fixture.Services.GetRequiredService<IStorageCatalogStore>());

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("async payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/async.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);
        Assert.Equal(1, primaryBackend.PutObjectCallCount);
        Assert.Equal(0, replicaBackend.PutObjectCallCount);
        Assert.True((await primaryBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/async.txt"
        })).IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, (await replicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/async.txt"
        })).Error!.Code);

        var outstandingRepair = Assert.Single(await repairBacklog.ListOutstandingAsync());
        Assert.Equal(StorageReplicaRepairOrigin.AsyncReplication, outstandingRepair.Origin);
        Assert.Equal(StorageReplicaRepairStatus.Pending, outstandingRepair.Status);
        Assert.Equal(StorageOperationType.PutObject, outstandingRepair.Operation);
        Assert.Equal(primaryBackend.Name, outstandingRepair.PrimaryBackendName);
        Assert.Equal(replicaBackend.Name, outstandingRepair.ReplicaBackendName);
        Assert.Equal("async-bucket", outstandingRepair.BucketName);
        Assert.Equal("docs/async.txt", outstandingRepair.ObjectKey);

        var catalogProvidersBeforeDispatch = catalogStore.Objects
            .Where(entry => entry.BucketName == "async-bucket" && entry.Key == "docs/async.txt")
            .Select(entry => entry.ProviderName)
            .OrderBy(static providerName => providerName, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal([primaryBackend.Name], catalogProvidersBeforeDispatch);

        var recordedDispatch = Assert.Single(repairDispatcher.Dispatches);
        Assert.Equal(outstandingRepair.Id, recordedDispatch.Entry.Id);

        var dispatchError = await recordedDispatch.ExecuteAsync();
        Assert.Null(dispatchError);

        Assert.Equal(1, replicaBackend.PutObjectCallCount);
        Assert.True((await replicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/async.txt"
        })).IsSuccess);
        Assert.Empty(await repairBacklog.ListOutstandingAsync());

        var catalogProvidersAfterDispatch = catalogStore.Objects
            .Where(entry => entry.BucketName == "async-bucket" && entry.Key == "docs/async.txt")
            .Select(entry => entry.ProviderName)
            .OrderBy(static providerName => providerName, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal([primaryBackend.Name, replicaBackend.Name], catalogProvidersAfterDispatch);
    }

    [Fact]
    public async Task OrchestratedStorageService_WriteToPrimaryAsyncReplicas_ContinuesTrackingHealthyReplicas_WhenAnotherDispatchCannotBeRecorded()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        var brokenReplicaBackend = new InMemoryStorageBackend("broken-replica");
        var healthyReplicaBackend = new InMemoryStorageBackend("healthy-replica");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteToPrimaryAsyncReplicas;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(brokenReplicaBackend);
            services.AddSingleton<IStorageBackend>(healthyReplicaBackend);
            services.AddSingleton<SelectiveThrowingReplicaRepairDispatcher>(serviceProvider =>
                new SelectiveThrowingReplicaRepairDispatcher(
                    serviceProvider.GetRequiredService<IStorageReplicaRepairBacklog>(),
                    [brokenReplicaBackend.Name]));
            services.AddSingleton<IStorageReplicaRepairDispatcher>(static serviceProvider => serviceProvider.GetRequiredService<SelectiveThrowingReplicaRepairDispatcher>());
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var repairBacklog = fixture.Services.GetRequiredService<IStorageReplicaRepairBacklog>();
        var repairDispatcher = fixture.Services.GetRequiredService<SelectiveThrowingReplicaRepairDispatcher>();

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("multi-provider payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/multi-provider.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.False(putResult.IsSuccess);
        Assert.Equal(StorageErrorCode.ProviderUnavailable, putResult.Error!.Code);
        Assert.Equal(brokenReplicaBackend.Name, putResult.Error.ProviderName);
        Assert.Contains("could not be recorded", putResult.Error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True((await primaryBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/multi-provider.txt"
        })).IsSuccess);

        Assert.Equal([brokenReplicaBackend.Name, healthyReplicaBackend.Name], repairDispatcher.AttemptedReplicaNames);

        var recordedReplica = Assert.Single(repairDispatcher.RecordedEntries);
        Assert.Equal(healthyReplicaBackend.Name, recordedReplica.ReplicaBackendName);

        var outstandingRepair = Assert.Single(await repairBacklog.ListOutstandingAsync());
        Assert.Equal(recordedReplica.Id, outstandingRepair.Id);
        Assert.Equal(healthyReplicaBackend.Name, outstandingRepair.ReplicaBackendName);
        Assert.Equal(StorageReplicaRepairStatus.Pending, outstandingRepair.Status);
        Assert.Equal(StorageReplicaRepairOrigin.AsyncReplication, outstandingRepair.Origin);
    }

    [Fact]
    public async Task OrchestratedStorageService_WriteToPrimaryAsyncReplicas_ReplayCanAdvanceCurrentReplicaWhileSiblingRepairFails()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        var healthyReplicaBackend = new InMemoryStorageBackend("healthy-replica");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteToPrimaryAsyncReplicas;
                options.ReadRoutingMode = StorageReadRoutingMode.PreferHealthyReplica;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend, FailingReplicaStorageBackend>();
            services.AddSingleton<IStorageBackend>(healthyReplicaBackend);
            services.AddSingleton<RecordingReplicaRepairDispatcher>();
            services.AddSingleton<IStorageReplicaRepairDispatcher>(static serviceProvider => serviceProvider.GetRequiredService<RecordingReplicaRepairDispatcher>());
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var repairBacklog = fixture.Services.GetRequiredService<IStorageReplicaRepairBacklog>();
        var repairDispatcher = fixture.Services.GetRequiredService<RecordingReplicaRepairDispatcher>();
        var catalogStore = Assert.IsType<FakeCatalogStore>(fixture.Services.GetRequiredService<IStorageCatalogStore>());

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("repairable payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "repair-bucket",
            Key = "docs/replay.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.True(putResult.IsSuccess);
        Assert.Equal(2, repairDispatcher.Dispatches.Count);
        Assert.Equal(2, (await repairBacklog.ListOutstandingAsync()).Count);

        var healthyDispatch = Assert.Single(repairDispatcher.Dispatches, dispatch => dispatch.Entry.ReplicaBackendName == healthyReplicaBackend.Name);
        var failingDispatch = Assert.Single(repairDispatcher.Dispatches, dispatch => dispatch.Entry.ReplicaBackendName == "failing-replica");

        var failingError = await failingDispatch.ExecuteAsync();
        Assert.NotNull(failingError);
        Assert.Equal(StorageErrorCode.ProviderUnavailable, failingError!.Code);

        var healthyError = await healthyDispatch.ExecuteAsync();
        Assert.Null(healthyError);

        Assert.True((await healthyReplicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "repair-bucket",
            Key = "docs/replay.txt"
        })).IsSuccess);

        var remainingRepair = Assert.Single(await repairBacklog.ListOutstandingAsync());
        Assert.Equal("failing-replica", remainingRepair.ReplicaBackendName);
        Assert.Equal(StorageReplicaRepairStatus.Failed, remainingRepair.Status);
        Assert.Equal(StorageReplicaRepairOrigin.AsyncReplication, remainingRepair.Origin);
        Assert.Equal(StorageOperationType.PutObject, remainingRepair.Operation);
        Assert.Equal(1, remainingRepair.AttemptCount);
        Assert.Equal(StorageErrorCode.ProviderUnavailable, remainingRepair.LastErrorCode);

        var catalogProviders = catalogStore.Objects
            .Where(entry => entry.BucketName == "repair-bucket" && entry.Key == "docs/replay.txt")
            .Select(entry => entry.ProviderName)
            .OrderBy(static providerName => providerName, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal([healthyReplicaBackend.Name, primaryBackend.Name], catalogProviders);

        var primaryReadCountBeforeRead = primaryBackend.GetObjectCallCount;
        var healthyReplicaReadCountBeforeRead = healthyReplicaBackend.GetObjectCallCount;

        var getObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "repair-bucket",
            Key = "docs/replay.txt"
        });

        Assert.True(getObject.IsSuccess);
        await using var response = getObject.Value!;
        Assert.Equal("repairable payload", await ReadContentAsStringAsync(response.Content));
        Assert.Equal(primaryReadCountBeforeRead, primaryBackend.GetObjectCallCount);
        Assert.Equal(healthyReplicaReadCountBeforeRead + 1, healthyReplicaBackend.GetObjectCallCount);
    }

    [Fact]
    public async Task OrchestratedStorageService_WriteToPrimaryAsyncReplicas_QueuesNewReplicaWorkOnlyForBackendsThatRemainStale()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        var laggingReplicaBackend = new InMemoryStorageBackend("lagging-replica");
        var currentReplicaBackend = new InMemoryStorageBackend("current-replica");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteToPrimaryAsyncReplicas;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(laggingReplicaBackend);
            services.AddSingleton<IStorageBackend>(currentReplicaBackend);
            services.AddSingleton<RecordingReplicaRepairDispatcher>();
            services.AddSingleton<IStorageReplicaRepairDispatcher>(static serviceProvider => serviceProvider.GetRequiredService<RecordingReplicaRepairDispatcher>());
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var repairBacklog = fixture.Services.GetRequiredService<IStorageReplicaRepairBacklog>();
        var repairDispatcher = fixture.Services.GetRequiredService<RecordingReplicaRepairDispatcher>();

        await using (var firstUploadStream = new MemoryStream(Encoding.UTF8.GetBytes("first payload"))) {
            var firstPutResult = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "async-bucket",
                Key = "docs/first.txt",
                Content = firstUploadStream,
                ContentType = "text/plain"
            });

            Assert.True(firstPutResult.IsSuccess);
        }

        Assert.Equal(2, repairDispatcher.Dispatches.Count);

        var currentReplicaFirstDispatch = Assert.Single(repairDispatcher.Dispatches, dispatch =>
            dispatch.Entry.ReplicaBackendName == currentReplicaBackend.Name
            && string.Equals(dispatch.Entry.ObjectKey, "docs/first.txt", StringComparison.Ordinal));
        Assert.Null(await currentReplicaFirstDispatch.ExecuteAsync());

        var remainingAfterFirstReplay = Assert.Single(await repairBacklog.ListOutstandingAsync());
        Assert.Equal(laggingReplicaBackend.Name, remainingAfterFirstReplay.ReplicaBackendName);
        Assert.Equal("docs/first.txt", remainingAfterFirstReplay.ObjectKey);

        await using (var secondUploadStream = new MemoryStream(Encoding.UTF8.GetBytes("second payload"))) {
            var secondPutResult = await storageService.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "async-bucket",
                Key = "docs/second.txt",
                Content = secondUploadStream,
                ContentType = "text/plain"
            });

            Assert.True(secondPutResult.IsSuccess);
        }

        var laggingReplicaDispatches = repairDispatcher.Dispatches
            .Where(dispatch => dispatch.Entry.ReplicaBackendName == laggingReplicaBackend.Name)
            .ToArray();
        Assert.Single(laggingReplicaDispatches);
        Assert.Equal("docs/first.txt", laggingReplicaDispatches[0].Entry.ObjectKey);

        var currentReplicaDispatches = repairDispatcher.Dispatches
            .Where(dispatch => dispatch.Entry.ReplicaBackendName == currentReplicaBackend.Name)
            .ToArray();
        Assert.Equal(2, currentReplicaDispatches.Length);

        var outstandingRepairs = await repairBacklog.ListOutstandingAsync();
        var laggingReplicaRepairs = outstandingRepairs
            .Where(entry => entry.ReplicaBackendName == laggingReplicaBackend.Name)
            .OrderBy(entry => entry.ObjectKey, StringComparer.Ordinal)
            .ToArray();
        Assert.Collection(
            laggingReplicaRepairs,
            entry => {
                Assert.Equal("docs/first.txt", entry.ObjectKey);
                Assert.Equal(StorageReplicaRepairStatus.Pending, entry.Status);
            },
            entry => {
                Assert.Equal("docs/second.txt", entry.ObjectKey);
                Assert.Equal(StorageReplicaRepairStatus.Pending, entry.Status);
            });

        var currentReplicaOutstandingRepair = Assert.Single(outstandingRepairs, entry => entry.ReplicaBackendName == currentReplicaBackend.Name);
        Assert.Equal("docs/second.txt", currentReplicaOutstandingRepair.ObjectKey);
        Assert.Equal(StorageReplicaRepairStatus.Pending, currentReplicaOutstandingRepair.Status);

        Assert.True((await currentReplicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/first.txt"
        })).IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, (await laggingReplicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/first.txt"
        })).Error!.Code);
        Assert.Equal(StorageErrorCode.ObjectNotFound, (await currentReplicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/second.txt"
        })).Error!.Code);

        var currentReplicaSecondDispatch = Assert.Single(currentReplicaDispatches, dispatch => string.Equals(dispatch.Entry.ObjectKey, "docs/second.txt", StringComparison.Ordinal));
        Assert.Null(await currentReplicaSecondDispatch.ExecuteAsync());

        Assert.True((await currentReplicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/second.txt"
        })).IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, (await laggingReplicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "async-bucket",
            Key = "docs/second.txt"
        })).Error!.Code);
    }

    [Fact]
    public async Task OrchestratedStorageService_WriteThroughAll_FailsBeforeMutatingPrimary_WhenReplicaIsKnownUnhealthy()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        var replicaBackend = new InMemoryStorageBackend("replica-memory");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteThroughAll;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
            services.AddSingleton<IStorageBackendHealthEvaluator>(new ConfigurableStorageBackendHealthEvaluator(new Dictionary<string, StorageBackendHealthStatus>(StringComparer.Ordinal)
            {
                [primaryBackend.Name] = StorageBackendHealthStatus.Healthy,
                [replicaBackend.Name] = StorageBackendHealthStatus.Unhealthy
            }));
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var repairBacklog = fixture.Services.GetRequiredService<IStorageReplicaRepairBacklog>();

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("blocked payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "strict-bucket",
            Key = "docs/blocked.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.False(putResult.IsSuccess);
        Assert.Equal(StorageErrorCode.ProviderUnavailable, putResult.Error!.Code);
        Assert.Equal(replicaBackend.Name, putResult.Error.ProviderName);
        Assert.Contains("unhealthy", putResult.Error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, primaryBackend.PutObjectCallCount);
        Assert.Equal(0, replicaBackend.PutObjectCallCount);
        Assert.Equal(StorageErrorCode.BucketNotFound, (await primaryBackend.HeadBucketAsync("strict-bucket")).Error!.Code);
        Assert.Empty(await repairBacklog.ListOutstandingAsync());
    }

    [Theory]
    [InlineData(false, "primary payload", 1, 0)]
    [InlineData(true, "replica payload", 0, 1)]
    public async Task OrchestratedStorageService_PreferHealthyReplica_HonorsOutstandingRepairReadPolicy(
        bool allowReadsFromReplicasWithOutstandingRepairs,
        string expectedPayload,
        int expectedPrimaryReadCount,
        int expectedReplicaReadCount)
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        primaryBackend.AddObject("read-bucket", "docs/read.txt", "primary payload");

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("read-bucket", "docs/read.txt", "replica payload");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ReadRoutingMode = StorageReadRoutingMode.PreferHealthyReplica;
                options.Replication.AllowReadsFromReplicasWithOutstandingRepairs = allowReadsFromReplicasWithOutstandingRepairs;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var repairBacklog = fixture.Services.GetRequiredService<IStorageReplicaRepairBacklog>();
        await repairBacklog.AddAsync(CreateRepairEntry(
            StorageReplicaRepairOrigin.AsyncReplication,
            StorageReplicaRepairStatus.Pending,
            StorageOperationType.PutObject,
            primaryBackend.Name,
            replicaBackend.Name,
            "read-bucket",
            "docs/read.txt"));

        var getObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "read-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(getObject.IsSuccess);
        await using var response = getObject.Value!;
        Assert.Equal(expectedPayload, await ReadContentAsStringAsync(response.Content));
        Assert.Equal(expectedPrimaryReadCount, primaryBackend.GetObjectCallCount);
        Assert.Equal(expectedReplicaReadCount, replicaBackend.GetObjectCallCount);
        Assert.True(await repairBacklog.HasOutstandingRepairsAsync(replicaBackend.Name));
    }

    [Fact]
    public async Task OrchestratedStorageService_WriteThroughAll_RecordsPartialWriteFailuresInReplicaBacklog()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        var successfulReplicaBackend = new InMemoryStorageBackend("replica-memory");
        var trailingReplicaBackend = new InMemoryStorageBackend("trailing-replica");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteThroughAll;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(successfulReplicaBackend);
            services.AddSingleton<IStorageBackend, FailingReplicaStorageBackend>();
            services.AddSingleton<IStorageBackend>(trailingReplicaBackend);
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var repairBacklog = fixture.Services.GetRequiredService<IStorageReplicaRepairBacklog>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "partial-bucket"
        })).IsSuccess);

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
        Assert.True((await primaryBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "partial-bucket",
            Key = "docs/partial.txt"
        })).IsSuccess);
        Assert.True((await successfulReplicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "partial-bucket",
            Key = "docs/partial.txt"
        })).IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, (await trailingReplicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "partial-bucket",
            Key = "docs/partial.txt"
        })).Error!.Code);

        var outstandingRepairs = (await repairBacklog.ListOutstandingAsync())
            .ToDictionary(entry => entry.ReplicaBackendName, StringComparer.Ordinal);
        Assert.Equal(2, outstandingRepairs.Count);
        Assert.DoesNotContain(successfulReplicaBackend.Name, outstandingRepairs.Keys);

        Assert.Contains("failing-replica", outstandingRepairs.Keys);
        var failedReplicaRepair = outstandingRepairs["failing-replica"];
        Assert.Equal(StorageReplicaRepairOrigin.PartialWriteFailure, failedReplicaRepair.Origin);
        Assert.Equal(StorageReplicaRepairStatus.Failed, failedReplicaRepair.Status);
        Assert.Equal(StorageOperationType.PutObject, failedReplicaRepair.Operation);
        Assert.Equal(1, failedReplicaRepair.AttemptCount);
        Assert.Equal(StorageErrorCode.ProviderUnavailable, failedReplicaRepair.LastErrorCode);
        Assert.NotNull(failedReplicaRepair.LastErrorMessage);
        Assert.Contains("Replica write failed.", failedReplicaRepair.LastErrorMessage!, StringComparison.Ordinal);

        Assert.Contains(trailingReplicaBackend.Name, outstandingRepairs.Keys);
        var pendingReplicaRepair = outstandingRepairs[trailingReplicaBackend.Name];
        Assert.Equal(StorageReplicaRepairOrigin.PartialWriteFailure, pendingReplicaRepair.Origin);
        Assert.Equal(StorageReplicaRepairStatus.Pending, pendingReplicaRepair.Status);
        Assert.Equal(StorageOperationType.PutObject, pendingReplicaRepair.Operation);
        Assert.Equal(0, pendingReplicaRepair.AttemptCount);
        Assert.Null(pendingReplicaRepair.LastErrorCode);
        Assert.Null(pendingReplicaRepair.LastErrorMessage);
    }

    [Fact]
    public async Task OrchestratedStorageService_FailedReplicaRepairs_RemainVisibleAndInfluenceReadsAndWrites()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        primaryBackend.AddObject("repair-bucket", "docs/read.txt", "primary payload");

        var replicaBackend = new InMemoryStorageBackend("replica-memory");
        replicaBackend.AddObject("repair-bucket", "docs/read.txt", "replica payload");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteThroughAll;
                options.ReadRoutingMode = StorageReadRoutingMode.PreferHealthyReplica;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var repairBacklog = fixture.Services.GetRequiredService<IStorageReplicaRepairBacklog>();
        var failedRepair = CreateRepairEntry(
            StorageReplicaRepairOrigin.PartialWriteFailure,
            StorageReplicaRepairStatus.Failed,
            StorageOperationType.PutObject,
            primaryBackend.Name,
            replicaBackend.Name,
            "repair-bucket",
            "docs/read.txt",
            attemptCount: 1,
            lastError: new StorageError
            {
                Code = StorageErrorCode.ProviderUnavailable,
                Message = "Replica write failed.",
                BucketName = "repair-bucket",
                ObjectKey = "docs/read.txt",
                ProviderName = replicaBackend.Name,
                SuggestedHttpStatusCode = 503
            });
        await repairBacklog.AddAsync(failedRepair);

        var seededRepair = Assert.Single(await repairBacklog.ListOutstandingAsync(replicaBackend.Name));
        Assert.Equal(failedRepair.Id, seededRepair.Id);
        Assert.Equal(StorageReplicaRepairStatus.Failed, seededRepair.Status);

        var getObject = await storageService.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "repair-bucket",
            Key = "docs/read.txt"
        });

        Assert.True(getObject.IsSuccess);
        await using (var response = getObject.Value!) {
            Assert.Equal("primary payload", await ReadContentAsStringAsync(response.Content));
        }
        Assert.Equal(1, primaryBackend.GetObjectCallCount);
        Assert.Equal(0, replicaBackend.GetObjectCallCount);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("blocked payload"));
        var putResult = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "repair-bucket",
            Key = "docs/blocked.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        });

        Assert.False(putResult.IsSuccess);
        Assert.Equal(StorageErrorCode.PreconditionFailed, putResult.Error!.Code);
        Assert.Equal(replicaBackend.Name, putResult.Error.ProviderName);
        Assert.Contains("incomplete failed repair attempts", putResult.Error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, primaryBackend.PutObjectCallCount);
        Assert.Equal(0, replicaBackend.PutObjectCallCount);

        var remainingRepair = Assert.Single(await repairBacklog.ListOutstandingAsync(replicaBackend.Name));
        Assert.Equal(failedRepair.Id, remainingRepair.Id);
        Assert.Equal(StorageReplicaRepairStatus.Failed, remainingRepair.Status);
        Assert.Equal(1, remainingRepair.AttemptCount);
        Assert.Equal(StorageErrorCode.ProviderUnavailable, remainingRepair.LastErrorCode);
        Assert.Equal("Replica write failed.", remainingRepair.LastErrorMessage);
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
    public async Task OrchestratedStorageService_WriteThroughAll_ReplicatesObjectTags()
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
        var catalogStore = fixture.Services.GetRequiredService<IStorageCatalogStore>();
        var backends = fixture.Services.GetServices<IStorageBackend>().OrderBy(static backend => backend.Name, StringComparer.Ordinal).ToArray();

        Assert.Equal(2, backends.Length);

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "tagged-bucket"
        })).IsSuccess);

        await using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("tagged payload"));
        Assert.True((await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "tagged-bucket",
            Key = "docs/tagged.txt",
            Content = uploadStream,
            ContentType = "text/plain"
        })).IsSuccess);

        var tagResult = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "tagged-bucket",
            Key = "docs/tagged.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["environment"] = "test",
                ["owner"] = "copilot"
            }
        });

        Assert.True(tagResult.IsSuccess);

        foreach (var backend in backends) {
            var headObject = await backend.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = "tagged-bucket",
                Key = "docs/tagged.txt"
            });

            Assert.True(headObject.IsSuccess);
            Assert.Equal("test", headObject.Value!.Tags!["environment"]);
            Assert.Equal("copilot", headObject.Value.Tags["owner"]);
        }

        var primaryCatalogObject = Assert.Single(await catalogStore.ListObjectsAsync("catalog-disk", "tagged-bucket"));
        Assert.Equal("test", primaryCatalogObject.Tags!["environment"]);
    }

    [Fact]
    public async Task OrchestratedStorageService_VersionSpecificTagDeletes_RefreshCatalogForHistoricalEntries()
    {
        await using var fixture = new CoreStorageFixture();
        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var catalogStore = fixture.Services.GetRequiredService<IStorageCatalogStore>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "catalog-version-tags",
            EnableVersioning = true
        })).IsSuccess);

        await using var v1Stream = new MemoryStream(Encoding.UTF8.GetBytes("catalog version one"));
        var v1Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "catalog-version-tags",
            Key = "docs/history.txt",
            Content = v1Stream,
            ContentType = "text/plain"
        });
        Assert.True(v1Put.IsSuccess);

        await using var v2Stream = new MemoryStream(Encoding.UTF8.GetBytes("catalog version two"));
        var v2Put = await storageService.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "catalog-version-tags",
            Key = "docs/history.txt",
            Content = v2Stream,
            ContentType = "text/plain"
        });
        Assert.True(v2Put.IsSuccess);

        var putHistoricalTags = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "catalog-version-tags",
            Key = "docs/history.txt",
            VersionId = v1Put.Value!.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["generation"] = "one"
            }
        });
        Assert.True(putHistoricalTags.IsSuccess);

        var deleteHistoricalTags = await storageService.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
        {
            BucketName = "catalog-version-tags",
            Key = "docs/history.txt",
            VersionId = v1Put.Value.VersionId
        });
        Assert.True(deleteHistoricalTags.IsSuccess);
        Assert.Equal(v1Put.Value.VersionId, deleteHistoricalTags.Value!.VersionId);

        var catalogObjects = await catalogStore.ListObjectsAsync("catalog-disk", "catalog-version-tags");
        var historicalCatalogObject = Assert.Single(catalogObjects, entry => entry.VersionId == v1Put.Value.VersionId);
        var currentCatalogObject = Assert.Single(catalogObjects, entry => entry.VersionId == v2Put.Value!.VersionId);

        Assert.True(historicalCatalogObject.Tags is null || historicalCatalogObject.Tags.Count == 0);
        Assert.Null(currentCatalogObject.Tags);
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
    public async Task OrchestratedStorageService_WriteThroughAll_PersistsEarlierReplicaWritesWhenLaterReplicaFails()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        var replicaBackend = new InMemoryStorageBackend("replica-memory");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteThroughAll;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
            services.AddSingleton<IStorageBackend, FailingReplicaStorageBackend>();
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var catalogStore = Assert.IsType<FakeCatalogStore>(fixture.Services.GetRequiredService<IStorageCatalogStore>());

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "partial-bucket"
        })).IsSuccess);

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

        Assert.True((await primaryBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "partial-bucket",
            Key = "docs/partial.txt"
        })).IsSuccess);

        Assert.True((await replicaBackend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "partial-bucket",
            Key = "docs/partial.txt"
        })).IsSuccess);

        var catalogProviderNames = catalogStore.Objects
            .Where(entry => entry.BucketName == "partial-bucket" && entry.Key == "docs/partial.txt")
            .Select(entry => entry.ProviderName)
            .OrderBy(static providerName => providerName, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(new[] { "primary-memory", "replica-memory" }, catalogProviderNames);
    }

    [Fact]
    public async Task OrchestratedStorageService_WriteThroughAll_ReplicatesBucketCorsConfiguration()
    {
        var primaryBackend = new InMemoryStorageBackend("primary-memory", isPrimary: true);
        var replicaBackend = new InMemoryStorageBackend("replica-memory");

        await using var fixture = new CoreStorageFixture(overrideCatalogStore: true, addDefaultDiskStorage: false, configureServices: services => {
            services.Configure<IntegratedS3CoreOptions>(options => {
                options.ConsistencyMode = StorageConsistencyMode.WriteThroughAll;
            });
            services.AddSingleton<IStorageBackend>(primaryBackend);
            services.AddSingleton<IStorageBackend>(replicaBackend);
        });

        var storageService = fixture.Services.GetRequiredService<IStorageService>();

        Assert.True((await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "cors-bucket"
        })).IsSuccess);

        var putCors = await storageService.PutBucketCorsAsync(new PutBucketCorsRequest
        {
            BucketName = "cors-bucket",
            Rules =
            [
                new BucketCorsRule
                {
                    AllowedOrigins = ["https://app.example"],
                    AllowedMethods = [BucketCorsMethod.Get, BucketCorsMethod.Put],
                    AllowedHeaders = ["authorization"],
                    ExposeHeaders = ["etag"],
                    MaxAgeSeconds = 120
                }
            ]
        });

        Assert.True(putCors.IsSuccess);

        var primaryCors = await primaryBackend.GetBucketCorsAsync("cors-bucket");
        var replicaCors = await replicaBackend.GetBucketCorsAsync("cors-bucket");

        Assert.True(primaryCors.IsSuccess);
        Assert.True(replicaCors.IsSuccess);
        Assert.Equal([BucketCorsMethod.Get, BucketCorsMethod.Put], Assert.Single(primaryCors.Value!.Rules).AllowedMethods);
        Assert.Equal([BucketCorsMethod.Get, BucketCorsMethod.Put], Assert.Single(replicaCors.Value!.Rules).AllowedMethods);

        var deleteCors = await storageService.DeleteBucketCorsAsync(new DeleteBucketCorsRequest
        {
            BucketName = "cors-bucket"
        });

        Assert.True(deleteCors.IsSuccess);
        Assert.Equal(StorageErrorCode.CorsConfigurationNotFound, (await primaryBackend.GetBucketCorsAsync("cors-bucket")).Error!.Code);
        Assert.Equal(StorageErrorCode.CorsConfigurationNotFound, (await replicaBackend.GetBucketCorsAsync("cors-bucket")).Error!.Code);
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

    [Fact]
    public async Task StorageOperations_EmitActivitiesAndMetricsWithProviderAndCorrelationTags()
    {
        using var observability = new TestObservabilityCollector();
        await using var fixture = new CoreStorageFixture(configureServices: services => {
            services.AddLogging(logging => {
                logging.ClearProviders();
                logging.SetMinimumLevel(LogLevel.Debug);
                logging.AddProvider(observability);
            });
        });

        var requestContextAccessor = fixture.Services.GetRequiredService<IIntegratedS3RequestContextAccessor>();
        requestContextAccessor.Current = new IntegratedS3RequestContext
        {
            Principal = CreatePrincipal("storage.write"),
            CorrelationId = "core-correlation-001",
            RequestId = "core-request-001"
        };

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var result = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "telemetry-bucket"
        });

        Assert.True(result.IsSuccess);

        var activity = Assert.Single(observability.Activities, candidate => string.Equals(candidate.OperationName, "IntegratedS3.Storage.CreateBucket", StringComparison.Ordinal));
        Assert.Equal("core-correlation-001", activity.Tags[IntegratedS3Observability.Tags.CorrelationId]);
        Assert.Equal("catalog-disk", activity.Tags[IntegratedS3Observability.Tags.Provider]);
        Assert.Equal("catalog-disk", activity.Tags[IntegratedS3Observability.Tags.PrimaryProvider]);

        var countMeasurement = Assert.Single(observability.Measurements, measurement =>
            string.Equals(measurement.InstrumentName, IntegratedS3Observability.Metrics.StorageOperationCount, StringComparison.Ordinal)
            && measurement.Tags.TryGetValue(IntegratedS3Observability.Tags.Operation, out var operation)
            && string.Equals(operation, StorageOperationType.CreateBucket.ToString(), StringComparison.Ordinal)
            && measurement.Tags.TryGetValue(IntegratedS3Observability.Tags.Result, out var resultTag)
            && string.Equals(resultTag, "success", StringComparison.Ordinal));
        Assert.Equal(1d, countMeasurement.Value);

        Assert.Contains(observability.Logs, entry =>
            entry.Level == LogLevel.Debug
            && entry.CategoryName.EndsWith("AuthorizingStorageService", StringComparison.Ordinal)
            && string.Equals(entry.State["CorrelationId"], "core-correlation-001", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AuthorizationFailures_EmitMetricsAndStructuredLogs()
    {
        using var observability = new TestObservabilityCollector();
        await using var fixture = new CoreStorageFixture(configureServices: services => {
            services.AddLogging(logging => {
                logging.ClearProviders();
                logging.SetMinimumLevel(LogLevel.Debug);
                logging.AddProvider(observability);
            });
            services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });

        var requestContextAccessor = fixture.Services.GetRequiredService<IIntegratedS3RequestContextAccessor>();
        requestContextAccessor.Current = new IntegratedS3RequestContext
        {
            Principal = new ClaimsPrincipal(new ClaimsIdentity()),
            CorrelationId = "authz-correlation-001",
            RequestId = "authz-request-001"
        };

        var storageService = fixture.Services.GetRequiredService<IStorageService>();
        var result = await storageService.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "denied-bucket"
        });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.AccessDenied, result.Error!.Code);

        Assert.Contains(observability.Measurements, measurement =>
            string.Equals(measurement.InstrumentName, IntegratedS3Observability.Metrics.StorageAuthorizationFailures, StringComparison.Ordinal)
            && measurement.Tags.TryGetValue(IntegratedS3Observability.Tags.Operation, out var operation)
            && string.Equals(operation, StorageOperationType.CreateBucket.ToString(), StringComparison.Ordinal)
            && measurement.Tags.TryGetValue(IntegratedS3Observability.Tags.ErrorCode, out var errorCode)
            && string.Equals(errorCode, StorageErrorCode.AccessDenied.ToString(), StringComparison.Ordinal));

        Assert.Contains(observability.Logs, entry =>
            entry.Level == LogLevel.Warning
            && entry.CategoryName.EndsWith("AuthorizingStorageService", StringComparison.Ordinal)
            && string.Equals(entry.State["CorrelationId"], "authz-correlation-001", StringComparison.Ordinal)
            && entry.Message.Contains("authorization denied", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task ReplicaRepairBacklog_ExposesBacklogSizeAndLagMetrics()
    {
        using var observability = new TestObservabilityCollector();
        await using var fixture = new CoreStorageFixture(configureServices: services => {
            services.AddLogging(logging => {
                logging.ClearProviders();
                logging.SetMinimumLevel(LogLevel.Debug);
                logging.AddProvider(observability);
            });
        });

        var backlog = fixture.Services.GetRequiredService<IStorageReplicaRepairBacklog>();
        await backlog.AddAsync(new StorageReplicaRepairEntry
        {
            Id = Guid.NewGuid().ToString("N"),
            Origin = StorageReplicaRepairOrigin.AsyncReplication,
            Status = StorageReplicaRepairStatus.Pending,
            Operation = StorageOperationType.PutObject,
            PrimaryBackendName = "catalog-disk",
            ReplicaBackendName = "replica-disk",
            BucketName = "telemetry-bucket",
            ObjectKey = "docs/lag.txt",
            CreatedAtUtc = DateTimeOffset.UtcNow.AddMinutes(-3),
            UpdatedAtUtc = DateTimeOffset.UtcNow.AddMinutes(-3)
        });

        observability.RecordObservableInstruments();

        Assert.Contains(observability.Measurements, measurement =>
            string.Equals(measurement.InstrumentName, IntegratedS3Observability.Metrics.ReplicaRepairBacklogSize, StringComparison.Ordinal)
            && measurement.Tags.TryGetValue(IntegratedS3Observability.Tags.ReplicaBackend, out var replicaBackend)
            && string.Equals(replicaBackend, "replica-disk", StringComparison.Ordinal)
            && measurement.Tags.TryGetValue(IntegratedS3Observability.Tags.RepairStatus, out var repairStatus)
            && string.Equals(repairStatus, StorageReplicaRepairStatus.Pending.ToString(), StringComparison.Ordinal)
            && measurement.Value >= 1d);

        Assert.Contains(observability.Measurements, measurement =>
            string.Equals(measurement.InstrumentName, IntegratedS3Observability.Metrics.ReplicaRepairOldestAge, StringComparison.Ordinal)
            && measurement.Tags.TryGetValue(IntegratedS3Observability.Tags.ReplicaBackend, out var replicaBackend)
            && string.Equals(replicaBackend, "replica-disk", StringComparison.Ordinal)
            && measurement.Value >= 150d);
    }

    private sealed class CoreStorageFixture : IAsyncDisposable
    {
        public CoreStorageFixture(bool overrideCatalogStore = false, bool addDefaultDiskStorage = true, Action<IServiceCollection>? configureServices = null)
        {
            RootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Core.Tests", Guid.NewGuid().ToString("N"));
            DatabasePath = Path.Combine(RootPath, "catalog.db");
            Directory.CreateDirectory(RootPath);

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

    private static StorageReplicaRepairEntry CreateRepairEntry(
        StorageReplicaRepairOrigin origin,
        StorageReplicaRepairStatus status,
        StorageOperationType operation,
        string primaryBackendName,
        string replicaBackendName,
        string bucketName,
        string? objectKey,
        string? versionId = null,
        int attemptCount = 0,
        StorageError? lastError = null)
    {
        var now = DateTimeOffset.UtcNow;
        return new StorageReplicaRepairEntry
        {
            Id = Guid.NewGuid().ToString("N"),
            Origin = origin,
            Status = status,
            Operation = operation,
            PrimaryBackendName = primaryBackendName,
            ReplicaBackendName = replicaBackendName,
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            CreatedAtUtc = now,
            UpdatedAtUtc = now,
            AttemptCount = attemptCount,
            LastErrorCode = lastError?.Code,
            LastErrorMessage = lastError?.Message
        };
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
                StorageOperationType.PresignGetObject => "storage.read",
                StorageOperationType.GetBucketLocation => "storage.read",
                StorageOperationType.GetBucketCors => "storage.read",
                StorageOperationType.GetObjectTags => "storage.read",
                StorageOperationType.HeadObject => "storage.read",
                StorageOperationType.PresignPutObject => "storage.write",
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

    private sealed class ConfigurableStorageBackendHealthProbe(IReadOnlyDictionary<string, StorageBackendHealthStatus> statuses) : IStorageBackendHealthProbe
    {
        private readonly ConcurrentDictionary<string, StorageBackendHealthStatus> _statuses = new(statuses, StringComparer.Ordinal);

        public ValueTask<StorageBackendHealthStatus> ProbeAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(_statuses.TryGetValue(backend.Name, out var status)
                ? status
                : StorageBackendHealthStatus.Unknown);
        }

        public void SetStatus(string backendName, StorageBackendHealthStatus status)
        {
            _statuses[backendName] = status;
        }
    }

    private sealed class DelegatingStorageBackendHealthProbe(IReadOnlyDictionary<string, Func<CancellationToken, ValueTask<StorageBackendHealthStatus>>> handlers) : IStorageBackendHealthProbe
    {
        private readonly IReadOnlyDictionary<string, Func<CancellationToken, ValueTask<StorageBackendHealthStatus>>> _handlers = handlers;

        public ValueTask<StorageBackendHealthStatus> ProbeAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _handlers.TryGetValue(backend.Name, out var handler)
                ? handler(cancellationToken)
                : ValueTask.FromResult(StorageBackendHealthStatus.Unknown);
        }
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan duration)
        {
            _utcNow = _utcNow.Add(duration);
        }
    }

    private sealed class RecordingReplicaRepairDispatcher(IStorageReplicaRepairBacklog repairBacklog) : IStorageReplicaRepairDispatcher
    {
        public List<RecordedDispatch> Dispatches { get; } = [];

        public async ValueTask DispatchAsync(
            StorageReplicaRepairEntry entry,
            Func<CancellationToken, ValueTask<StorageError?>> repairOperation,
            CancellationToken cancellationToken = default)
        {
            await repairBacklog.AddAsync(entry, cancellationToken);
            Dispatches.Add(new RecordedDispatch(entry, repairOperation, repairBacklog));
        }

        public sealed class RecordedDispatch(
            StorageReplicaRepairEntry entry,
            Func<CancellationToken, ValueTask<StorageError?>> repairOperation,
            IStorageReplicaRepairBacklog repairBacklog)
        {
            public StorageReplicaRepairEntry Entry { get; } = entry;

            public bool WasExecuted { get; private set; }

            public async ValueTask<StorageError?> ExecuteAsync(CancellationToken cancellationToken = default)
            {
                if (WasExecuted) {
                    throw new InvalidOperationException("Recorded replica repair dispatch has already been executed.");
                }

                WasExecuted = true;
                await repairBacklog.MarkInProgressAsync(Entry.Id, cancellationToken);

                StorageError? error;
                try {
                    error = await repairOperation(cancellationToken);
                }
                catch (Exception ex) {
                    error = new StorageError
                    {
                        Code = StorageErrorCode.ProviderUnavailable,
                        Message = $"Recorded replica repair dispatch failed during test execution: {ex.Message}",
                        BucketName = Entry.BucketName,
                        ObjectKey = Entry.ObjectKey,
                        VersionId = Entry.VersionId,
                        ProviderName = Entry.ReplicaBackendName,
                        SuggestedHttpStatusCode = 503
                    };
                }

                if (error is null) {
                    await repairBacklog.MarkCompletedAsync(Entry.Id, cancellationToken);
                }
                else {
                    await repairBacklog.MarkFailedAsync(Entry.Id, error, cancellationToken);
                }

                return error;
            }
        }
    }

    private sealed class SelectiveThrowingReplicaRepairDispatcher(
        IStorageReplicaRepairBacklog repairBacklog,
        IReadOnlyCollection<string> replicaNamesThatThrow) : IStorageReplicaRepairDispatcher
    {
        private readonly HashSet<string> _replicaNamesThatThrow = new(replicaNamesThatThrow, StringComparer.Ordinal);

        public List<string> AttemptedReplicaNames { get; } = [];

        public List<StorageReplicaRepairEntry> RecordedEntries { get; } = [];

        public async ValueTask DispatchAsync(
            StorageReplicaRepairEntry entry,
            Func<CancellationToken, ValueTask<StorageError?>> repairOperation,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(entry);
            ArgumentNullException.ThrowIfNull(repairOperation);
            cancellationToken.ThrowIfCancellationRequested();

            AttemptedReplicaNames.Add(entry.ReplicaBackendName);
            if (_replicaNamesThatThrow.Contains(entry.ReplicaBackendName)) {
                throw new InvalidOperationException($"Simulated async replica repair tracking failure for provider '{entry.ReplicaBackendName}'.");
            }

            await repairBacklog.AddAsync(entry, cancellationToken);
            RecordedEntries.Add(entry);
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

        public ValueTask<IntegratedS3.Abstractions.Capabilities.StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new IntegratedS3.Abstractions.Capabilities.StorageSupportStateDescriptor());
        }

        public ValueTask<IntegratedS3.Abstractions.Models.StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(IntegratedS3.Abstractions.Models.StorageProviderMode.Managed);
        }

        public ValueTask<IntegratedS3.Abstractions.Models.StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new IntegratedS3.Abstractions.Models.StorageObjectLocationDescriptor());
        }

        public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
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

        public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketVersioningInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.BucketNotFound,
                Message = $"Bucket '{bucketName}' was not found.",
                BucketName = bucketName,
                ProviderName = Name,
                SuggestedHttpStatusCode = 404
            }));
        }

        public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketVersioningInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.ProviderUnavailable,
                Message = "Replica bucket versioning write failed.",
                BucketName = request.BucketName,
                ProviderName = Name,
                SuggestedHttpStatusCode = 503
            }));
        }

        public ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(new StorageError
            {
                Code = StorageErrorCode.BucketNotFound,
                Message = $"Bucket '{bucketName}' was not found.",
                BucketName = bucketName,
                ProviderName = Name,
                SuggestedHttpStatusCode = 404
            }));
        }

        public ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(new StorageError
            {
                Code = StorageErrorCode.ProviderUnavailable,
                Message = "Replica bucket CORS write failed.",
                BucketName = request.BucketName,
                ProviderName = Name,
                SuggestedHttpStatusCode = 503
            }));
        }

        public ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult.Failure(new StorageError
            {
                Code = StorageErrorCode.ProviderUnavailable,
                Message = "Replica bucket CORS delete failed.",
                BucketName = request.BucketName,
                ProviderName = Name,
                SuggestedHttpStatusCode = 503
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

        public ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<ObjectTagSet>.Failure(new StorageError
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

        public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<ObjectTagSet>.Failure(new StorageError
            {
                Code = StorageErrorCode.ProviderUnavailable,
                Message = "Replica tag write failed.",
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                ProviderName = Name,
                SuggestedHttpStatusCode = 503
            }));
        }

        public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<ObjectTagSet>.Failure(new StorageError
            {
                Code = StorageErrorCode.ProviderUnavailable,
                Message = "Replica tag delete failed.",
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

        public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId
            }));
        }
    }

    private sealed class InMemoryStorageBackend(string name, bool isPrimary = false) : IStorageBackend
    {
        private readonly Dictionary<string, BucketInfo> _buckets = new(StringComparer.Ordinal);
        private readonly Dictionary<string, BucketCorsConfiguration> _bucketCorsConfigurations = new(StringComparer.Ordinal);
        private readonly Dictionary<(string BucketName, string Key), StoredObject> _objects = new();

        public string Name => name;

        public string Kind => "test-memory";

        public bool IsPrimary => isPrimary;

        public string? Description => $"In-memory backend '{name}'.";

        public int GetObjectCallCount { get; private set; }

        public int PutObjectCallCount { get; private set; }

        public bool FailGetObjectWithProviderUnavailable { get; set; }

        public StorageErrorCode? GetObjectFailureCode { get; set; }

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
            var checksum = Convert.ToBase64String(System.Security.Cryptography.SHA256.HashData(bytes));
            _objects[(bucketName, key)] = new StoredObject
            {
                Content = bytes,
                Info = new ObjectInfo
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = Guid.CreateVersion7().ToString("N"),
                    ContentLength = bytes.Length,
                    ContentType = contentType,
                    ETag = $"{bucketName}:{key}:{bytes.Length}",
                    LastModifiedUtc = DateTimeOffset.UtcNow,
                    Metadata = metadata,
                    Tags = null,
                    Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["sha256"] = checksum
                    }
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

        public ValueTask<IntegratedS3.Abstractions.Capabilities.StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new IntegratedS3.Abstractions.Capabilities.StorageSupportStateDescriptor());
        }

        public ValueTask<IntegratedS3.Abstractions.Models.StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(IntegratedS3.Abstractions.Models.StorageProviderMode.Managed);
        }

        public ValueTask<IntegratedS3.Abstractions.Models.StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new IntegratedS3.Abstractions.Models.StorageObjectLocationDescriptor());
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

        public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult(_buckets.TryGetValue(bucketName, out var bucket)
                ? StorageResult<BucketVersioningInfo>.Success(new BucketVersioningInfo
                {
                    BucketName = bucketName,
                    Status = bucket.VersioningEnabled ? BucketVersioningStatus.Enabled : BucketVersioningStatus.Disabled
                })
                : StorageResult<BucketVersioningInfo>.Failure(new StorageError
                {
                    Code = StorageErrorCode.BucketNotFound,
                    Message = $"Bucket '{bucketName}' was not found.",
                    BucketName = bucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
        }

        public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_buckets.TryGetValue(request.BucketName, out var bucket)) {
                return ValueTask.FromResult(StorageResult<BucketVersioningInfo>.Failure(new StorageError
                {
                    Code = StorageErrorCode.BucketNotFound,
                    Message = $"Bucket '{request.BucketName}' was not found.",
                    BucketName = request.BucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            var updatedBucket = new BucketInfo
            {
                Name = bucket.Name,
                CreatedAtUtc = bucket.CreatedAtUtc,
                VersioningEnabled = request.Status == BucketVersioningStatus.Enabled
            };

            _buckets[request.BucketName] = updatedBucket;

            return ValueTask.FromResult(StorageResult<BucketVersioningInfo>.Success(new BucketVersioningInfo
            {
                BucketName = request.BucketName,
                Status = request.Status
            }));
        }

        public ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_buckets.ContainsKey(bucketName)) {
                return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(new StorageError
                {
                    Code = StorageErrorCode.BucketNotFound,
                    Message = $"Bucket '{bucketName}' was not found.",
                    BucketName = bucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            return ValueTask.FromResult(_bucketCorsConfigurations.TryGetValue(bucketName, out var configuration)
                ? StorageResult<BucketCorsConfiguration>.Success(CloneBucketCorsConfiguration(configuration))
                : StorageResult<BucketCorsConfiguration>.Failure(new StorageError
                {
                    Code = StorageErrorCode.CorsConfigurationNotFound,
                    Message = $"Bucket '{bucketName}' does not have a CORS configuration.",
                    BucketName = bucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
        }

        public ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_buckets.ContainsKey(request.BucketName)) {
                return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(new StorageError
                {
                    Code = StorageErrorCode.BucketNotFound,
                    Message = $"Bucket '{request.BucketName}' was not found.",
                    BucketName = request.BucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            var configuration = new BucketCorsConfiguration
            {
                BucketName = request.BucketName,
                Rules = request.Rules.Select(CloneBucketCorsRule).ToArray()
            };

            _bucketCorsConfigurations[request.BucketName] = configuration;
            return ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Success(CloneBucketCorsConfiguration(configuration)));
        }

        public ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_buckets.ContainsKey(request.BucketName)) {
                return ValueTask.FromResult(StorageResult.Failure(new StorageError
                {
                    Code = StorageErrorCode.BucketNotFound,
                    Message = $"Bucket '{request.BucketName}' was not found.",
                    BucketName = request.BucketName,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            _bucketCorsConfigurations.Remove(request.BucketName);
            return ValueTask.FromResult(StorageResult.Success());
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

            _bucketCorsConfigurations.Remove(request.BucketName);

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

        public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
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

            var simulatedFailureCode = GetObjectFailureCode;
            if (simulatedFailureCode is null && FailGetObjectWithProviderUnavailable) {
                simulatedFailureCode = StorageErrorCode.ProviderUnavailable;
            }

            if (simulatedFailureCode is { } errorCode) {
                return ValueTask.FromResult(StorageResult<GetObjectResponse>.Failure(CreateGetObjectFailure(request, errorCode)));
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

            if (!MatchesRequestedVersion(request.VersionId, storedObject.Info.VersionId)) {
                return ValueTask.FromResult(StorageResult<GetObjectResponse>.Failure(StorageError.Unsupported(
                    $"Object '{request.Key}' was requested with version '{request.VersionId}', but only current-version access is currently supported.",
                    request.BucketName,
                    request.Key)));
            }

            return ValueTask.FromResult(StorageResult<GetObjectResponse>.Success(new GetObjectResponse
            {
                Object = storedObject.Info,
                Content = new MemoryStream(storedObject.Content, writable: false),
                TotalContentLength = storedObject.Info.ContentLength
            }));
        }

        private StorageError CreateGetObjectFailure(GetObjectRequest request, StorageErrorCode errorCode)
        {
            var (message, suggestedHttpStatusCode) = errorCode switch
            {
                StorageErrorCode.Throttled => ("Simulated throttled read.", 429),
                StorageErrorCode.ProviderUnavailable => ("Simulated provider outage.", 503),
                StorageErrorCode.ObjectNotFound => ($"Object '{request.Key}' was not found in bucket '{request.BucketName}'.", 404),
                StorageErrorCode.AccessDenied => ("Simulated access denied.", 403),
                StorageErrorCode.UnsupportedCapability => ("Simulated unsupported capability.", 400),
                StorageErrorCode.QuotaExceeded => ("Simulated quota exceeded.", 507),
                _ => ($"Simulated {errorCode} failure.", 500)
            };

            return new StorageError
            {
                Code = errorCode,
                Message = message,
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                ProviderName = Name,
                SuggestedHttpStatusCode = suggestedHttpStatusCode
            };
        }

        public ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult(_objects.TryGetValue((request.BucketName, request.Key), out var storedObject)
                ? MatchesRequestedVersion(request.VersionId, storedObject.Info.VersionId)
                    ? StorageResult<ObjectTagSet>.Success(new ObjectTagSet
                    {
                        BucketName = request.BucketName,
                        Key = request.Key,
                        VersionId = storedObject.Info.VersionId,
                        Tags = storedObject.Info.Tags ?? new Dictionary<string, string>(StringComparer.Ordinal)
                    })
                    : StorageResult<ObjectTagSet>.Failure(StorageError.Unsupported(
                        $"Object '{request.Key}' was requested with version '{request.VersionId}', but only current-version access is currently supported.",
                        request.BucketName,
                        request.Key))
                : StorageResult<ObjectTagSet>.Failure(new StorageError
                {
                    Code = StorageErrorCode.ObjectNotFound,
                    Message = $"Object '{request.Key}' was not found in bucket '{request.BucketName}'.",
                    BucketName = request.BucketName,
                    ObjectKey = request.Key,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
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
            PutObjectCallCount++;

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
                VersionId = Guid.CreateVersion7().ToString("N"),
                IsLatest = true,
                ContentLength = bytes.Length,
                ContentType = request.ContentType,
                ETag = $"{request.BucketName}:{request.Key}:{bytes.Length}",
                LastModifiedUtc = DateTimeOffset.UtcNow,
                Metadata = request.Metadata,
                Tags = null,
                Checksums = request.Checksums
            };
            _objects[(request.BucketName, request.Key)] = new StoredObject
            {
                Content = bytes,
                Info = objectInfo
            };

            return StorageResult<ObjectInfo>.Success(objectInfo);
        }

        public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_objects.TryGetValue((request.BucketName, request.Key), out var storedObject)) {
                return ValueTask.FromResult(StorageResult<ObjectTagSet>.Failure(new StorageError
                {
                    Code = StorageErrorCode.ObjectNotFound,
                    Message = $"Object '{request.Key}' was not found in bucket '{request.BucketName}'.",
                    BucketName = request.BucketName,
                    ObjectKey = request.Key,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            if (!MatchesRequestedVersion(request.VersionId, storedObject.Info.VersionId)) {
                return ValueTask.FromResult(StorageResult<ObjectTagSet>.Failure(StorageError.Unsupported(
                    $"Object '{request.Key}' was requested with version '{request.VersionId}', but only current-version access is currently supported.",
                    request.BucketName,
                    request.Key)));
            }

            var normalizedTags = request.Tags.Count == 0
                ? null
                : new Dictionary<string, string>(request.Tags, StringComparer.Ordinal);
            var updatedInfo = new ObjectInfo
            {
                BucketName = storedObject.Info.BucketName,
                Key = storedObject.Info.Key,
                VersionId = storedObject.Info.VersionId,
                ContentLength = storedObject.Info.ContentLength,
                ContentType = storedObject.Info.ContentType,
                ETag = storedObject.Info.ETag,
                LastModifiedUtc = DateTimeOffset.UtcNow,
                Metadata = storedObject.Info.Metadata,
                Tags = normalizedTags,
                Checksums = storedObject.Info.Checksums
            };

            _objects[(request.BucketName, request.Key)] = new StoredObject
            {
                Content = storedObject.Content,
                Info = updatedInfo
            };

            return ValueTask.FromResult(StorageResult<ObjectTagSet>.Success(new ObjectTagSet
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = updatedInfo.VersionId,
                Tags = normalizedTags ?? new Dictionary<string, string>(StringComparer.Ordinal)
            }));
        }

        public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return PutObjectTagsAsync(new PutObjectTagsRequest
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            }, cancellationToken);
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
                ? MatchesRequestedVersion(request.VersionId, storedObject.Info.VersionId)
                    ? StorageResult<ObjectInfo>.Success(storedObject.Info)
                    : StorageResult<ObjectInfo>.Failure(StorageError.Unsupported(
                        $"Object '{request.Key}' was requested with version '{request.VersionId}', but only current-version access is currently supported.",
                        request.BucketName,
                        request.Key))
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

        public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_objects.TryGetValue((request.BucketName, request.Key), out var storedObject)
                && !MatchesRequestedVersion(request.VersionId, storedObject.Info.VersionId)) {
                return ValueTask.FromResult(StorageResult<DeleteObjectResult>.Failure(StorageError.Unsupported(
                    $"Object '{request.Key}' was requested with version '{request.VersionId}', but only current-version access is currently supported.",
                    request.BucketName,
                    request.Key)));
            }

            if (!_objects.Remove((request.BucketName, request.Key))) {
                return ValueTask.FromResult(StorageResult<DeleteObjectResult>.Failure(new StorageError
                {
                    Code = StorageErrorCode.ObjectNotFound,
                    Message = $"Object '{request.Key}' was not found in bucket '{request.BucketName}'.",
                    BucketName = request.BucketName,
                    ObjectKey = request.Key,
                    ProviderName = Name,
                    SuggestedHttpStatusCode = 404
                }));
            }

            return ValueTask.FromResult(StorageResult<DeleteObjectResult>.Success(new DeleteObjectResult
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = storedObject.Info.VersionId
            }));
        }

        private static BucketCorsConfiguration CloneBucketCorsConfiguration(BucketCorsConfiguration configuration)
        {
            return new BucketCorsConfiguration
            {
                BucketName = configuration.BucketName,
                Rules = configuration.Rules.Select(CloneBucketCorsRule).ToArray()
            };
        }

        private static BucketCorsRule CloneBucketCorsRule(BucketCorsRule rule)
        {
            return new BucketCorsRule
            {
                Id = rule.Id,
                AllowedOrigins = rule.AllowedOrigins.ToArray(),
                AllowedMethods = rule.AllowedMethods.ToArray(),
                AllowedHeaders = rule.AllowedHeaders.ToArray(),
                ExposeHeaders = rule.ExposeHeaders.ToArray(),
                MaxAgeSeconds = rule.MaxAgeSeconds
            };
        }

        private sealed class StoredObject
        {
            public required byte[] Content { get; init; }

            public required ObjectInfo Info { get; init; }
        }

        private static bool MatchesRequestedVersion(string? requestedVersionId, string? currentVersionId)
        {
            return string.IsNullOrWhiteSpace(requestedVersionId)
                   || string.Equals(requestedVersionId, currentVersionId, StringComparison.Ordinal);
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
            if (@object.IsLatest) {
                Objects.RemoveAll(existing => existing.ProviderName == providerName && existing.BucketName == @object.BucketName && existing.Key == @object.Key && existing.IsLatest);
            }

            Objects.RemoveAll(existing => existing.ProviderName == providerName
                && existing.BucketName == @object.BucketName
                && existing.Key == @object.Key
                && string.Equals(existing.VersionId, @object.VersionId, StringComparison.Ordinal));

            Objects.Add(new StoredObjectEntry
            {
                ProviderName = providerName,
                BucketName = @object.BucketName,
                Key = @object.Key,
                VersionId = @object.VersionId,
                IsLatest = @object.IsLatest,
                IsDeleteMarker = @object.IsDeleteMarker,
                ContentLength = @object.ContentLength,
                ContentType = @object.ContentType,
                ETag = @object.ETag,
                LastModifiedUtc = @object.LastModifiedUtc,
                Metadata = @object.Metadata,
                Tags = @object.Tags,
                Checksums = @object.Checksums,
                LastSyncedAtUtc = DateTimeOffset.UtcNow
            });
            return ValueTask.CompletedTask;
        }

        public ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
        {
            Objects.RemoveAll(existing => existing.ProviderName == providerName
                && existing.BucketName == bucketName
                && existing.Key == key
                && (string.IsNullOrWhiteSpace(versionId) || string.Equals(existing.VersionId, versionId, StringComparison.Ordinal)));
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

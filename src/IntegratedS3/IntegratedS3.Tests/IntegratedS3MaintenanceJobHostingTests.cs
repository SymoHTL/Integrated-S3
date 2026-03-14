using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Maintenance;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3MaintenanceJobHostingTests
{
    [Fact]
    public async Task AddIntegratedS3MaintenanceJob_BindsNamedOptionsAndSkipsDisabledJobs()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["IntegratedS3:Maintenance:MirrorReplay:Enabled"] = "false",
                ["IntegratedS3:Maintenance:MirrorReplay:RunOnStartup"] = "true",
                ["IntegratedS3:Maintenance:MirrorReplay:Interval"] = "00:00:00.050"
            })
            .Build();

        var executionCount = 0;
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddIntegratedS3MaintenanceJob(
            IntegratedS3MaintenanceJobNames.MirrorReplay,
            configuration.GetSection("IntegratedS3:Maintenance:MirrorReplay"),
            (_, _) => {
                Interlocked.Increment(ref executionCount);
                return ValueTask.CompletedTask;
            });

        await using var serviceProvider = services.BuildServiceProvider();
        var hostedService = Assert.Single(serviceProvider.GetServices<IHostedService>());
        var options = serviceProvider
            .GetRequiredService<IOptionsMonitor<IntegratedS3MaintenanceJobOptions>>()
            .Get(IntegratedS3MaintenanceJobNames.MirrorReplay);

        Assert.False(options.Enabled);
        Assert.True(options.RunOnStartup);
        Assert.Equal(TimeSpan.FromMilliseconds(50), options.Interval);

        await hostedService.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await hostedService.StopAsync(CancellationToken.None);

        Assert.Equal(0, executionCount);
    }

    [Fact]
    public async Task ScheduledMaintenanceJobHostedService_RunsDelegatesOnStartupAndInterval()
    {
        var executionCount = 0;
        var secondExecution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddIntegratedS3MaintenanceJob(
            IntegratedS3MaintenanceJobNames.MultipartCleanup,
            (_, _) => {
                if (Interlocked.Increment(ref executionCount) >= 2) {
                    secondExecution.TrySetResult();
                }

                return ValueTask.CompletedTask;
            },
            options => {
                options.RunOnStartup = true;
                options.Interval = TimeSpan.FromMilliseconds(40);
            });

        await using var serviceProvider = services.BuildServiceProvider();
        var hostedService = Assert.Single(serviceProvider.GetServices<IHostedService>());

        await hostedService.StartAsync(CancellationToken.None);
        await secondExecution.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await hostedService.StopAsync(CancellationToken.None);

        Assert.True(executionCount >= 2);
    }

    [Fact]
    public async Task AddIntegratedS3MaintenanceJob_GenericOverloadResolvesScopedJobs()
    {
        var recorder = new MaintenanceJobRecorder();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(recorder);
        services.AddScoped<ScopedDependency>();
        services.AddIntegratedS3MaintenanceJob<ScopedMaintenanceJob>(
            IntegratedS3MaintenanceJobNames.ChecksumVerification,
            options => {
                options.RunOnStartup = true;
                options.Interval = TimeSpan.FromHours(1);
            });

        await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateScopes = true
        });
        var hostedService = Assert.Single(serviceProvider.GetServices<IHostedService>());

        await hostedService.StartAsync(CancellationToken.None);
        await recorder.Executed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await hostedService.StopAsync(CancellationToken.None);

        Assert.Equal(1, recorder.ExecutionCount);
    }

    private sealed class MaintenanceJobRecorder
    {
        public int ExecutionCount;

        public TaskCompletionSource Executed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class ScopedDependency;

    private sealed class ScopedMaintenanceJob(ScopedDependency scopedDependency, MaintenanceJobRecorder recorder) : IIntegratedS3MaintenanceJob
    {
        public ValueTask ExecuteAsync(CancellationToken cancellationToken = default)
        {
            _ = scopedDependency;
            Interlocked.Increment(ref recorder.ExecutionCount);
            recorder.Executed.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }
}

using IntegratedS3.AspNetCore.Maintenance;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IntegratedS3.AspNetCore.HostedServices;

internal sealed class IntegratedS3ScheduledMaintenanceJobHostedService(
    IServiceScopeFactory serviceScopeFactory,
    IOptionsMonitor<IntegratedS3MaintenanceJobOptions> optionsMonitor,
    TimeProvider timeProvider,
    ILogger<IntegratedS3ScheduledMaintenanceJobHostedService> logger,
    string jobName,
    IntegratedS3MaintenanceJobDelegate execute) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var firstIteration = true;
        while (!stoppingToken.IsCancellationRequested) {
            var options = optionsMonitor.Get(jobName);
            if (options.Enabled && (!firstIteration || options.RunOnStartup)) {
                await RunOnceAsync(stoppingToken);
            }

            firstIteration = false;

            try {
                await Task.Delay(options.Interval, timeProvider, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
                break;
            }
        }
    }

    private async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        await using var scope = serviceScopeFactory.CreateAsyncScope();

        logger.LogInformation("Running IntegratedS3 maintenance job '{JobName}'.", jobName);
        await execute(scope.ServiceProvider, cancellationToken);
        logger.LogInformation("Completed IntegratedS3 maintenance job '{JobName}'.", jobName);
    }
}

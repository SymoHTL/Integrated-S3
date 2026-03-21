using System.Diagnostics;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Observability;
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
    private static readonly Histogram<double> JobDuration = IntegratedS3Observability.Meter.CreateHistogram<double>(
        IntegratedS3Observability.Metrics.MaintenanceJobDuration, "ms", "Duration of maintenance job executions");

    private static readonly Counter<long> JobFailures = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.MaintenanceJobFailures, "{failure}", "Count of maintenance job failures");

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
        using var activity = IntegratedS3Observability.ActivitySource.StartActivity("IntegratedS3.MaintenanceJob");
        activity?.SetTag("integrateds3.job_name", jobName);
        var sw = Stopwatch.StartNew();

        try
        {
            await using var scope = serviceScopeFactory.CreateAsyncScope();
            logger.LogInformation("Running IntegratedS3 maintenance job '{JobName}'.", jobName);
            await execute(scope.ServiceProvider, cancellationToken);
            sw.Stop();
            logger.LogInformation("Completed IntegratedS3 maintenance job '{JobName}' in {ElapsedMs}ms.", jobName, sw.ElapsedMilliseconds);
            activity?.SetTag(IntegratedS3Observability.Tags.Result, "success");
            JobDuration.Record(sw.Elapsed.TotalMilliseconds,
                new KeyValuePair<string, object?>("integrateds3.job_name", jobName),
                new KeyValuePair<string, object?>(IntegratedS3Observability.Tags.Result, "success"));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            sw.Stop();
            logger.LogInformation("IntegratedS3 maintenance job '{JobName}' was cancelled.", jobName);
            activity?.SetStatus(ActivityStatusCode.Ok, "Cancelled");
        }
        catch (Exception ex)
        {
            sw.Stop();
            logger.LogError(ex, "IntegratedS3 maintenance job '{JobName}' failed after {ElapsedMs}ms.", jobName, sw.ElapsedMilliseconds);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag(IntegratedS3Observability.Tags.Result, "failure");
            JobFailures.Add(1, new KeyValuePair<string, object?>("integrateds3.job_name", jobName));
            JobDuration.Record(sw.Elapsed.TotalMilliseconds,
                new KeyValuePair<string, object?>("integrateds3.job_name", jobName),
                new KeyValuePair<string, object?>(IntegratedS3Observability.Tags.Result, "failure"));
        }
    }
}

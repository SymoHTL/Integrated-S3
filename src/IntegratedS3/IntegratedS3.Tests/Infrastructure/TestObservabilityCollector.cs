using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Observability;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Tests.Infrastructure;

internal sealed class TestObservabilityCollector : ILoggerProvider, ISupportExternalScope, IDisposable
{
    private static readonly SemaphoreSlim ListenerGate = new(1, 1);

    private readonly ConcurrentQueue<ObservedLogEntry> _logs = new();
    private readonly ConcurrentQueue<ObservedMeasurement> _measurements = new();
    private readonly ConcurrentQueue<ObservedActivity> _activities = new();
    private readonly MeterListener _meterListener = new();
    private readonly ActivityListener _activityListener;
    private IExternalScopeProvider _scopeProvider = new LoggerExternalScopeProvider();

    public TestObservabilityCollector()
    {
        ListenerGate.Wait();
        try {
            _meterListener.InstrumentPublished = (instrument, listener) => {
                if (string.Equals(instrument.Meter.Name, IntegratedS3Observability.MeterName, StringComparison.Ordinal)) {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _meterListener.SetMeasurementEventCallback<long>(RecordMeasurement);
            _meterListener.SetMeasurementEventCallback<int>(RecordMeasurement);
            _meterListener.SetMeasurementEventCallback<double>(RecordMeasurement);
            _meterListener.Start();

            _activityListener = new ActivityListener
            {
                ShouldListenTo = static source => string.Equals(source.Name, IntegratedS3Observability.ActivitySourceName, StringComparison.Ordinal),
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => _activities.Enqueue(new ObservedActivity(
                    activity.OperationName,
                    activity.DisplayName,
                    activity.Status,
                    activity.TagObjects.ToDictionary(
                        static pair => pair.Key,
                        static pair => pair.Value?.ToString(),
                        StringComparer.Ordinal)))
            };

            ActivitySource.AddActivityListener(_activityListener);
        }
        catch {
            ListenerGate.Release();
            throw;
        }
    }

    public IReadOnlyList<ObservedLogEntry> Logs => _logs.ToArray();

    public IReadOnlyList<ObservedMeasurement> Measurements => _measurements.ToArray();

    public IReadOnlyList<ObservedActivity> Activities => _activities.ToArray();

    public ILogger CreateLogger(string categoryName)
    {
        return new TestLogger(categoryName, _logs, () => _scopeProvider);
    }

    public void SetScopeProvider(IExternalScopeProvider scopeProvider)
    {
        _scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
    }

    public void RecordObservableInstruments()
    {
        _meterListener.RecordObservableInstruments();
    }

    public void Dispose()
    {
        _meterListener.Dispose();
        _activityListener.Dispose();
        ListenerGate.Release();
    }

    private void RecordMeasurement<T>(
        Instrument instrument,
        T measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
        where T : struct
    {
        var tagDictionary = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (var tag in tags) {
            tagDictionary[tag.Key] = tag.Value?.ToString();
        }

        _measurements.Enqueue(new ObservedMeasurement(
            instrument.Name,
            Convert.ToDouble(measurement),
            tagDictionary));
    }

    internal sealed record ObservedLogEntry(
        string CategoryName,
        LogLevel Level,
        string Message,
        IReadOnlyDictionary<string, string?> State);

    internal sealed record ObservedMeasurement(
        string InstrumentName,
        double Value,
        IReadOnlyDictionary<string, string?> Tags);

    internal sealed record ObservedActivity(
        string OperationName,
        string DisplayName,
        ActivityStatusCode Status,
        IReadOnlyDictionary<string, string?> Tags);

    private sealed class TestLogger(
        string categoryName,
        ConcurrentQueue<ObservedLogEntry> entries,
        Func<IExternalScopeProvider> scopeProviderAccessor) : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) where TState : notnull
        {
            return scopeProviderAccessor().Push(state);
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var values = new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["EventId"] = eventId.Id.ToString()
            };

            if (state is IEnumerable<KeyValuePair<string, object?>> structuredState) {
                foreach (var pair in structuredState) {
                    if (string.Equals(pair.Key, "{OriginalFormat}", StringComparison.Ordinal)) {
                        continue;
                    }

                    values[pair.Key] = pair.Value?.ToString();
                }
            }

            scopeProviderAccessor().ForEachScope((scope, stateDictionary) => {
                if (scope is IEnumerable<KeyValuePair<string, object?>> structuredScope) {
                    foreach (var pair in structuredScope) {
                        if (string.Equals(pair.Key, "{OriginalFormat}", StringComparison.Ordinal)) {
                            continue;
                        }

                        stateDictionary[pair.Key] = pair.Value?.ToString();
                    }
                }
            }, values);

            if (exception is not null) {
                values["ExceptionType"] = exception.GetType().Name;
            }

            entries.Enqueue(new ObservedLogEntry(
                categoryName,
                logLevel,
                formatter(state, exception),
                values));
        }
    }
}

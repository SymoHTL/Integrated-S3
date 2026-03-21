using System.Diagnostics;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Observability;

namespace IntegratedS3.Protocol.Internal;

internal static class ProtocolTelemetry
{
    private static readonly Counter<long> XmlParseErrors = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.ProtocolXmlParseErrors, "{error}", "Count of XML request parse errors");

    private static readonly Counter<long> SignatureErrors = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.ProtocolSignatureErrors, "{error}", "Count of signature computation/verification errors");

    public static void RecordXmlParseError(string operation, string? detail = null)
    {
        XmlParseErrors.Add(1,
            new KeyValuePair<string, object?>(IntegratedS3Observability.Tags.Operation, operation));

        Activity.Current?.AddEvent(new ActivityEvent("integrateds3.xml.parse_error",
            tags: new ActivityTagsCollection
            {
                { IntegratedS3Observability.Tags.Operation, operation },
                { "integrateds3.error_detail", detail ?? "unknown" }
            }));
    }

    public static void RecordSignatureError(string signatureType, string? detail = null)
    {
        SignatureErrors.Add(1,
            new KeyValuePair<string, object?>("integrateds3.signature_type", signatureType));

        Activity.Current?.AddEvent(new ActivityEvent("integrateds3.signature.error",
            tags: new ActivityTagsCollection
            {
                { "integrateds3.signature_type", signatureType },
                { "integrateds3.error_detail", detail ?? "unknown" }
            }));
    }
}

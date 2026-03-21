namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the S3 SelectObjectContent operation.</summary>
public sealed class SelectObjectContentRequest
{
    /// <summary>The name of the bucket containing the object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key to query.</summary>
    public required string Key { get; init; }

    /// <summary>The SQL expression used to query the object.</summary>
    public required string Expression { get; init; }

    /// <summary>The type of the expression (e.g. "SQL").</summary>
    public required string ExpressionType { get; init; }

    /// <summary>JSON input serialization configuration, if the input format is JSON.</summary>
    public string? InputSerializationJson { get; init; }

    /// <summary>CSV input serialization configuration, if the input format is CSV.</summary>
    public string? InputSerializationCsv { get; init; }

    /// <summary>Parquet input serialization configuration, if the input format is Parquet.</summary>
    public string? InputSerializationParquet { get; init; }

    /// <summary>JSON output serialization configuration, if the output format is JSON.</summary>
    public string? OutputSerializationJson { get; init; }

    /// <summary>CSV output serialization configuration, if the output format is CSV.</summary>
    public string? OutputSerializationCsv { get; init; }
}

namespace IngestApi;

/// <summary>
/// Strongly typed Kafka settings. Every default is committed so the service runs
/// out-of-the-box under Docker Compose; Compose only overrides the broker list.
/// </summary>
public sealed class KafkaConfig
{
    /// <summary>Comma-separated broker list, e.g. <c>kafka:9092</c>.</summary>
    public string BootstrapServers { get; init; } = "localhost:9092";

    /// <summary>Topic that ad-click events are appended to.</summary>
    public string Topic { get; init; } = "ad-clicks";

    /// <summary>Stable client id reported to the broker for this producer.</summary>
    public string ClientId { get; init; } = "ingest-api";

    /// <summary>Reads the config block from environment variables.</summary>
    public static KafkaConfig FromEnv() => new()
    {
        BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
        Topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "ad-clicks",
        ClientId = Environment.GetEnvironmentVariable("KAFKA_CLIENT_ID") ?? "ingest-api",
    };
}

namespace CountConsumer;

/// <summary>
/// Strongly typed Kafka settings for the consumer. Defaults are committed so the
/// service runs out-of-the-box under Compose; only the broker is overridden.
/// </summary>
public sealed class KafkaConfig
{
    public string BootstrapServers { get; init; } = "localhost:9092";
    public string Topic { get; init; } = "ad-clicks";
    public string ClientId { get; init; } = "count-consumer";

    /// <summary>
    /// Consumer group id. Offsets are tracked per group, so keeping this stable
    /// across restarts is what lets the consumer resume from its last offset.
    /// </summary>
    public string GroupId { get; init; } = "ad-click-counter";

    public static KafkaConfig FromEnv() => new()
    {
        BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
        Topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "ad-clicks",
        ClientId = Environment.GetEnvironmentVariable("KAFKA_CLIENT_ID") ?? "count-consumer",
        GroupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "ad-click-counter",
    };
}

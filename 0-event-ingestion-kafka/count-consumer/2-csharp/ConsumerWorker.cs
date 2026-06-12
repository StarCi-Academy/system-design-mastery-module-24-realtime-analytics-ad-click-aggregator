using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace CountConsumer;

/// <summary>
/// Background worker: ensures the topic exists, then reads click events from
/// Kafka and feeds them to <see cref="CounterService"/>.
/// </summary>
/// <remarks>
/// The consumer's <c>GroupId</c> is kept stable so that on restart Kafka hands
/// back the group's committed offsets and the consumer resumes exactly where it
/// left off — the offset-resume behaviour the lesson demonstrates.
/// </remarks>
public sealed class ConsumerWorker : BackgroundService
{
    private readonly KafkaConfig _config;
    private readonly CounterService _counter;
    private readonly ILogger<ConsumerWorker> _logger;

    public ConsumerWorker(KafkaConfig config, CounterService counter, ILogger<ConsumerWorker> logger)
    {
        _config = config;
        _counter = counter;
        _logger = logger;
    }

    /// <summary>
    /// Creates the topic with 3 partitions before subscribing so the consumer
    /// never races a missing topic on a cold start. Retries while the broker is
    /// still coming up; a no-op if the topic already exists.
    /// </summary>
    private async Task EnsureTopicAsync()
    {
        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = _config.BootstrapServers }).Build();
        for (var attempt = 0; attempt < 30; attempt++)
        {
            try
            {
                await admin.CreateTopicsAsync(new[]
                {
                    new TopicSpecification { Name = _config.Topic, NumPartitions = 3, ReplicationFactor = 1 },
                });
                return;
            }
            catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            {
                return; // already created by another service — fine.
            }
            catch (Exception ex)
            {
                _logger.LogWarning("waiting for broker: {Message}", ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureTopicAsync();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _config.BootstrapServers,
            ClientId = _config.ClientId,
            // Same GroupId across restarts = offsets committed per group, so a
            // restarted consumer resumes from the last committed offset.
            GroupId = _config.GroupId,
            // First run with no committed offset starts at the beginning;
            // afterwards the committed offset wins.
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(_config.Topic);
        _logger.LogInformation("count-consumer subscribed topic={Topic} group={Group}", _config.Topic, _config.GroupId);

        // Run the blocking consume loop off the startup thread.
        await Task.Run(() =>
        {
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var result = consumer.Consume(stoppingToken);
                    if (result?.Message?.Value is null) continue;
                    using var doc = JsonDocument.Parse(result.Message.Value);
                    var root = doc.RootElement;
                    _counter.Record(root.GetProperty("clickId").GetString()!, root.GetProperty("adId").GetString()!);
                }
            }
            catch (OperationCanceledException) { /* graceful shutdown */ }
            finally { consumer.Close(); }
        }, stoppingToken);
    }
}

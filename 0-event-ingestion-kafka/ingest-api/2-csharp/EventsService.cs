using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace IngestApi;

/// <summary>Request body for <c>POST /clicks</c>.</summary>
/// <remarks>
/// <c>ClickId</c> is the idempotency key: re-sending the same id (retry,
/// double-fire) must be counted once by the consumer, so it is a first-class
/// field. It is optional so the API can mint one when omitted.
/// </remarks>
public sealed record PublishClickDto(string? ClickId, string AdId, Dictionary<string, object>? Payload);

/// <summary>
/// Producer service: builds a click envelope and appends it to the Kafka log.
/// </summary>
/// <remarks>
/// The service is deliberately thin — it owns no counting logic. Counting and
/// deduplication live in the consumer; the producer only guarantees the event
/// carries a stable <c>clickId</c> so the consumer can deduplicate downstream.
/// </remarks>
public sealed class EventsService : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaConfig _config;
    private readonly ILogger<EventsService> _logger;

    public EventsService(KafkaConfig config, ILogger<EventsService> logger)
    {
        _config = config;
        _logger = logger;
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config.BootstrapServers,
            ClientId = config.ClientId,
        };
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    /// <summary>Appends one click event to the topic and returns an acknowledgement.</summary>
    /// <param name="dto">Incoming click payload from <c>POST /clicks</c>.</param>
    /// <returns>An object confirming the append only: status, topic, clickId, adId.</returns>
    public async Task<object> PublishAsync(PublishClickDto dto)
    {
        // Mint an idempotency key when the caller did not supply one.
        var clickId = string.IsNullOrWhiteSpace(dto.ClickId) ? Guid.NewGuid().ToString() : dto.ClickId;
        var envelope = new
        {
            clickId,
            adId = dto.AdId,
            payload = dto.Payload ?? new Dictionary<string, object>(),
            timestamp = DateTime.UtcNow.ToString("o"),
        };
        var value = JsonSerializer.Serialize(envelope);

        var message = new Message<string, string>
        {
            // Key = adId so all clicks of one ad land on the same partition and
            // keep relative order.
            Key = dto.AdId,
            Value = value,
            // The clickId header lets the consumer deduplicate without parsing
            // the value body first.
            Headers = new Headers { { "clickId", Encoding.UTF8.GetBytes(clickId) } },
        };
        // Fire-and-forget at the API level: we confirm the append, not consumer
        // processing.
        await _producer.ProduceAsync(_config.Topic, message);

        _logger.LogInformation("Produced click clickId={ClickId} adId={AdId}", clickId, dto.AdId);
        return new { status = "accepted", topic = _config.Topic, clickId, adId = dto.AdId };
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}

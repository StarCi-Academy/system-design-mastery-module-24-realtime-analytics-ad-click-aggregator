package com.starci.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Producer service: builds a click envelope and appends it to the Kafka log.
 *
 * <p>The service is deliberately thin — it owns no counting logic. Counting and
 * deduplication live in the consumer; the producer only guarantees the event
 * carries a stable {@code clickId} so the consumer can deduplicate downstream.
 */
@Service
public class EventsService {

    private static final Logger log = LoggerFactory.getLogger(EventsService.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${kafka.topic}")
    private String topic;

    public EventsService(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    /**
     * Appends one click event to the topic and returns an acknowledgement.
     *
     * @param dto incoming click payload from {@code POST /clicks}
     * @return a map confirming the append only: {@code status, topic, clickId, adId}
     */
    public Map<String, String> publish(PublishClickDto dto) throws Exception {
        // Mint an idempotency key when the caller did not supply one.
        String clickId = (dto.clickId == null || dto.clickId.isBlank())
                ? UUID.randomUUID().toString()
                : dto.clickId;

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("clickId", clickId);
        envelope.put("adId", dto.adId);
        envelope.put("payload", dto.payload == null ? Map.of() : dto.payload);
        envelope.put("timestamp", Instant.now().toString());
        String value = mapper.writeValueAsString(envelope);

        // key = adId so all clicks of one ad land on the same partition and keep
        // relative order; the clickId header lets the consumer deduplicate
        // without parsing the value body first.
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, dto.adId, value);
        record.headers().add(new RecordHeader("clickId", clickId.getBytes()));
        // Fire-and-forget: we confirm the append, not consumer processing.
        kafka.send(record);

        log.info("Produced click clickId={} adId={}", clickId, dto.adId);
        return Map.of("status", "accepted", "topic", topic, "clickId", clickId, "adId", dto.adId);
    }
}

package com.starci.counter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes click events from the {@code ad-clicks} topic.
 *
 * <p>{@code @KafkaListener} binds the topic to the consumer group; offsets are
 * committed per group, so a restart with the same group id resumes from the
 * last committed offset.
 */
@Component
public class CounterListener {

    private final CounterService counter;
    private final ObjectMapper mapper = new ObjectMapper();

    public CounterListener(CounterService counter) {
        this.counter = counter;
    }

    /**
     * Handles one click record from the topic.
     *
     * @param value the raw JSON envelope value
     */
    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group-id}")
    public void handle(String value) throws Exception {
        JsonNode env = mapper.readTree(value);
        counter.record(env.get("clickId").asText(), env.get("adId").asText());
    }
}

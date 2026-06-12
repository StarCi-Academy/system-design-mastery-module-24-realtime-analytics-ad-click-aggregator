package com.starci.counter;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Boots the count-consumer as both an HTTP server (for {@code GET /counts}) and
 * a Kafka consumer.
 *
 * <p>The consumer group id is kept stable so that on restart Kafka hands back
 * the group's committed offsets and the consumer resumes exactly where it left
 * off — the offset-resume behaviour the lesson demonstrates.
 */
@SpringBootApplication
public class CountConsumerApplication {

    @Value("${kafka.topic}")
    private String topic;

    public static void main(String[] args) {
        SpringApplication.run(CountConsumerApplication.class, args);
    }

    /**
     * Declares the topic with 3 partitions so KafkaAdmin creates it on boot.
     *
     * <p>A consumer that subscribes to a missing topic stalls; declaring it
     * up-front makes the stack start deterministically regardless of whether the
     * producer has run yet. It is a no-op if the topic already exists.
     */
    @Bean
    public NewTopic adClicksTopic() {
        return new NewTopic(topic, 3, (short) 1);
    }
}

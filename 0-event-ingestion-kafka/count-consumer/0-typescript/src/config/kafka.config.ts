import { registerAs } from "@nestjs/config"

/**
 * Strongly typed shape of the consumer's Kafka configuration block.
 */
export interface KafkaConfig {
    /** Comma-separated broker list, e.g. `kafka:9092`. */
    brokers: string[]
    /** Topic the consumer subscribes to. */
    topic: string
    /** Stable client id reported to the broker. */
    clientId: string
    /**
     * Consumer group id. Offsets are tracked per group, so keeping this stable
     * across restarts is what lets the consumer resume from its last offset.
     */
    groupId: string
}

/**
 * Registers the `kafka` namespace for the consumer.
 *
 * Defaults are committed so the service runs out-of-the-box under Compose; only
 * the broker address is overridden by the environment.
 */
export default registerAs<KafkaConfig>("kafka", () => ({
    brokers: (process.env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092").split(","),
    topic: process.env.KAFKA_TOPIC ?? "ad-clicks",
    clientId: process.env.KAFKA_CLIENT_ID ?? "count-consumer",
    groupId: process.env.KAFKA_GROUP_ID ?? "ad-click-counter",
}))

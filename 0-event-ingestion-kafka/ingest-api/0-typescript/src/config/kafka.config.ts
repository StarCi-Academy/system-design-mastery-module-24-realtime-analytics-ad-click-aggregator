import { registerAs } from "@nestjs/config"

/**
 * Strongly typed shape of the Kafka configuration block.
 *
 * Centralising the type lets every consumer of `ConfigService` read the block
 * with `getOrThrow<KafkaConfig>("kafka")` and get compile-time field checks.
 */
export interface KafkaConfig {
    /** Comma-separated broker list, e.g. `kafka:9092`. */
    brokers: string[]
    /** Topic that ad-click events are appended to. */
    topic: string
    /** Stable client id reported to the broker for this producer. */
    clientId: string
}

/**
 * Registers the `kafka` configuration namespace from environment variables.
 *
 * Every default is committed so the service runs out-of-the-box under Docker
 * Compose without any `.env` file; Compose only overrides the broker address.
 */
export default registerAs<KafkaConfig>("kafka", () => ({
    // Split on comma so multiple brokers can be passed via one env var.
    brokers: (process.env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092").split(","),
    topic: process.env.KAFKA_TOPIC ?? "ad-clicks",
    clientId: process.env.KAFKA_CLIENT_ID ?? "ingest-api",
}))

import { registerAs } from "@nestjs/config"

/** Kafka connection parameters for the ingest-api producer. */
export interface KafkaConfig {
    bootstrapServers: string[]
    topic: string
    clientId: string
}

/**
 * Reads Kafka settings from environment variables with safe defaults.
 *
 * All defaults work out-of-the-box when running via Docker Compose.
 */
export default registerAs(
    "kafka",
    (): KafkaConfig => ({
        bootstrapServers: (
            process.env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092"
        ).split(","),
        topic: process.env.KAFKA_TOPIC ?? "ad-clicks",
        clientId: process.env.KAFKA_CLIENT_ID ?? "ingest-api",
    }),
)

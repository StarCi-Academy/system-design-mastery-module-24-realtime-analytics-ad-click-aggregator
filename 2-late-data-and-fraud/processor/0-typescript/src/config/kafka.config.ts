import { registerAs } from "@nestjs/config"

/** Kafka consumer configuration for the processor. */
export interface KafkaConfig {
    bootstrapServers: string[]
    topic: string
    clientId: string
    groupId: string
}

export default registerAs(
    "kafka",
    (): KafkaConfig => ({
        bootstrapServers: (
            process.env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092"
        ).split(","),
        topic: process.env.KAFKA_TOPIC ?? "ad-clicks",
        clientId: process.env.KAFKA_CLIENT_ID ?? "processor",
        groupId: process.env.KAFKA_GROUP_ID ?? "ad-click-processor",
    }),
)

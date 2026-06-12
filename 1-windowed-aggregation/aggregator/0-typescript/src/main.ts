import { Logger } from "@nestjs/common"
import { NestFactory } from "@nestjs/core"
import { MicroserviceOptions, Transport } from "@nestjs/microservices"
import { Kafka } from "kafkajs"
import { AppModule } from "./app.module"

/**
 * Ensures the topic exists with 3 partitions before the aggregator subscribes.
 *
 * Without this, a cold start where ingest-api has not yet produced any events
 * causes the KafkaJS server to crash with UNKNOWN_TOPIC_OR_PARTITION.
 */
async function ensureTopic(brokers: string[], topic: string): Promise<void> {
    const admin = new Kafka({ clientId: "aggregator-admin", brokers }).admin()
    await admin.connect()
    await admin.createTopics({
        topics: [{ topic, numPartitions: 3, replicationFactor: 1 }],
    })
    await admin.disconnect()
    new Logger("Bootstrap").log(`Topic "${topic}" ensured`)
}

/**
 * Bootstrap the aggregator as a hybrid app (HTTP + Kafka microservice).
 *
 * HTTP server exposes `GET /snapshot` and `GET /flush` for e2e inspection.
 * The Kafka microservice consumes the `ad-clicks` topic.
 */
async function bootstrap(): Promise<void> {
    const app = await NestFactory.create(AppModule)

    const brokers = (
        process.env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092"
    ).split(",")
    const topic = process.env.KAFKA_TOPIC ?? "ad-clicks"
    const clientId = process.env.KAFKA_CLIENT_ID ?? "aggregator"
    const groupId = process.env.KAFKA_GROUP_ID ?? "ad-click-aggregator"

    // Create the topic before subscribing so the consumer never races a missing
    // topic on a cold start.
    await ensureTopic(brokers, topic)

    app.connectMicroservice<MicroserviceOptions>({
        transport: Transport.KAFKA,
        options: {
            client: { clientId, brokers },
            consumer: { groupId },
            subscribe: { fromBeginning: true },
        },
    })

    await app.startAllMicroservices()
    const port = parseInt(process.env.PORT ?? "3002", 10)
    await app.listen(port, "0.0.0.0")
    new Logger("Bootstrap").log(`aggregator listening on :${port} group=${groupId}`)
}

void bootstrap()

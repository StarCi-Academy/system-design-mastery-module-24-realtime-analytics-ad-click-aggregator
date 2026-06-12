import { Kafka } from "kafkajs"
import { NestFactory } from "@nestjs/core"
import { MicroserviceOptions, Transport } from "@nestjs/microservices"
import { AppModule } from "./app.module"

/**
 * Bootstrap the processor as a hybrid app (HTTP + Kafka microservice).
 *
 * HTTP server exposes `GET /snapshot` for e2e inspection.
 * The Kafka microservice consumes the `ad-clicks` topic.
 */
async function bootstrap(): Promise<void> {
    const app = await NestFactory.create(AppModule)

    const brokers = (
        process.env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092"
    ).split(",")
    const clientId = process.env.KAFKA_CLIENT_ID ?? "processor"
    const groupId = process.env.KAFKA_GROUP_ID ?? "ad-click-processor"
    const topic = process.env.KAFKA_TOPIC ?? "ad-clicks"

    // Create the topic proactively so the consumer never subscribes to a
    // missing topic-partition (which would crash KafkaJS on first metadata
    // refresh). Idempotent: a no-op when the topic already exists.
    const admin = new Kafka({ clientId, brokers }).admin()
    await admin.connect()
    await admin.createTopics({
        topics: [{ topic, numPartitions: 3 }],
        waitForLeaders: true,
    })
    await admin.disconnect()

    app.connectMicroservice<MicroserviceOptions>({
        transport: Transport.KAFKA,
        options: {
            client: { clientId, brokers },
            consumer: { groupId },
            subscribe: { fromBeginning: true },
        },
    })

    await app.startAllMicroservices()
    const port = parseInt(process.env.PORT ?? "3003", 10)
    // Bind 0.0.0.0 so the host can reach the container across the Docker bridge.
    await app.listen(port, "0.0.0.0")
}
bootstrap()

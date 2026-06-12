import { Logger } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import { NestFactory } from "@nestjs/core"
import { MicroserviceOptions, Transport } from "@nestjs/microservices"
import { Kafka } from "kafkajs"
import { AppModule } from "./app.module"
import { KafkaConfig } from "./config/kafka.config"

/**
 * Ensures the topic exists with 3 partitions before the consumer subscribes.
 *
 * A consumer that subscribes to a missing topic crashes with
 * UNKNOWN_TOPIC_OR_PARTITION; creating it up-front makes the stack start
 * deterministically regardless of whether the producer has run yet.
 *
 * @param kafka Resolved Kafka config (brokers, topic, clientId).
 */
async function ensureTopic(kafka: KafkaConfig): Promise<void> {
    const admin = new Kafka({ clientId: `${kafka.clientId}-admin`, brokers: kafka.brokers }).admin()
    await admin.connect()
    // `createTopics` is a no-op if the topic already exists.
    await admin.createTopics({
        topics: [{ topic: kafka.topic, numPartitions: 3, replicationFactor: 1 }],
    })
    await admin.disconnect()
}

/**
 * Bootstraps the count-consumer as a hybrid app.
 *
 * It is both an HTTP server (for `GET /counts`) and a Kafka microservice
 * consumer. The consumer's `groupId` is read from config and kept stable so
 * that on restart Kafka hands back the group's committed offsets and the
 * consumer resumes exactly where it left off — the offset-resume behaviour the
 * lesson demonstrates.
 */
async function bootstrap(): Promise<void> {
    const app = await NestFactory.create(AppModule)
    const config = app.get(ConfigService)
    const kafka = config.getOrThrow<KafkaConfig>("kafka")

    // Create the topic before subscribing so the consumer never races a missing
    // topic on a cold start.
    await ensureTopic(kafka)

    app.connectMicroservice<MicroserviceOptions>({
        transport: Transport.KAFKA,
        options: {
            client: { clientId: kafka.clientId, brokers: kafka.brokers },
            // Same groupId across restarts = offsets are committed per group, so
            // a restarted consumer resumes from the last committed offset.
            consumer: { groupId: kafka.groupId },
            // On the group's very first run (no committed offset yet) start from
            // the beginning of the log; afterwards the committed offset wins.
            subscribe: { fromBeginning: true },
        },
    })

    await app.startAllMicroservices()
    const port = Number(process.env.PORT ?? 3001)
    // Bind 0.0.0.0 so the host can curl GET /counts from outside the container.
    await app.listen(port, "0.0.0.0")
    new Logger("Bootstrap").log(
        `count-consumer listening on :${port} group=${kafka.groupId}`,
    )
}

void bootstrap()

import { randomUUID } from "node:crypto"
import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import { ClientKafka } from "@nestjs/microservices"
import { KafkaConfig } from "../config/kafka.config"
import { PublishClickDto } from "./publish-click.dto"

/**
 * Producer service: builds a click envelope and appends it to the Kafka log.
 *
 * The service is deliberately thin — it owns no counting logic. Counting and
 * deduplication live in the consumer; the producer only guarantees the event
 * carries a stable `clickId` so the consumer can deduplicate downstream.
 */
@Injectable()
export class EventsService implements OnModuleInit {
    private readonly logger = new Logger(EventsService.name)

    constructor(
        // Injected Kafka client wired by `@nestjs/microservices` ClientsModule.
        @Inject("KAFKA_PRODUCER") private readonly kafka: ClientKafka,
        private readonly config: ConfigService,
    ) {}

    /**
     * Connects the producer once when the module boots.
     *
     * KafkaJS lazily connects on first emit, but connecting here surfaces broker
     * problems at startup instead of on the first HTTP request.
     */
    async onModuleInit(): Promise<void> {
        await this.kafka.connect()
    }

    /**
     * Appends one click event to the topic and returns an acknowledgement.
     *
     * @param dto Incoming click payload from `POST /clicks`.
     * @returns `{ status, topic, clickId, adId }` confirming the append only.
     */
    async publish(
        dto: PublishClickDto,
    ): Promise<{ status: string; topic: string; clickId: string; adId: string }> {
        const kafka = this.config.getOrThrow<KafkaConfig>("kafka")
        // Mint an idempotency key when the caller did not supply one.
        const clickId = dto.clickId ?? randomUUID()
        const envelope = {
            clickId,
            adId: dto.adId,
            payload: dto.payload ?? {},
            timestamp: new Date().toISOString(),
        }
        // `key = adId` so all clicks of one ad land on the same partition and
        // keep relative order; `headers.clickId` lets the consumer deduplicate
        // without parsing the value body first.
        this.kafka.emit(kafka.topic, {
            key: dto.adId,
            value: envelope,
            headers: { clickId },
        })
        this.logger.log(`Produced click clickId=${clickId} adId=${dto.adId}`)
        // Fire-and-forget: we confirm the append, not consumer processing.
        return { status: "accepted", topic: kafka.topic, clickId, adId: dto.adId }
    }
}

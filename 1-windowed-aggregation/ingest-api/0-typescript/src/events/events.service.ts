import { randomUUID } from "node:crypto"
import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import { ClientKafka } from "@nestjs/microservices"
import { KafkaConfig } from "../config/kafka.config"
import { PublishClickDto } from "./publish-click.dto"

/**
 * Producer service: builds a time-stamped click envelope and appends it to the
 * Kafka log. The `eventTime` field drives the tumbling-window boundary on the
 * aggregator; `clickId` is the idempotency key for deduplication.
 */
@Injectable()
export class EventsService implements OnModuleInit {
    private readonly logger = new Logger(EventsService.name)

    constructor(
        @Inject("KAFKA_PRODUCER") private readonly kafka: ClientKafka,
        private readonly config: ConfigService,
    ) {}

    /**
     * Connects the producer once when the module boots.
     *
     * Connecting eagerly surfaces broker problems at startup rather than on
     * the first HTTP request.
     */
    async onModuleInit(): Promise<void> {
        await this.kafka.connect()
    }

    /**
     * Appends one click event to the topic and returns an acknowledgement.
     *
     * @param dto Incoming click payload.
     * @returns Acknowledgement with `eventTime` so callers can inspect the window bucket.
     */
    async publish(
        dto: PublishClickDto,
    ): Promise<{ status: string; topic: string; clickId: string; adId: string; eventTime: string }> {
        const kafka = this.config.getOrThrow<KafkaConfig>("kafka")
        // Mint an idempotency key when the caller did not supply one.
        const clickId = dto.clickId ?? randomUUID()
        // eventTime is the wall-clock time of the click, used by the aggregator
        // to assign the event to its tumbling-window bucket.
        const eventTime = new Date().toISOString()
        const envelope = {
            clickId,
            adId: dto.adId,
            payload: dto.payload ?? {},
            eventTime,
        }
        // key = adId pins all clicks of the same ad to one partition, keeping
        // relative ordering; the header carries clickId for fast dedup without
        // parsing the value body.
        this.kafka.emit(kafka.topic, {
            key: dto.adId,
            value: envelope,
            headers: { clickId },
        })
        this.logger.log(
            `Produced click clickId=${clickId} adId=${dto.adId} eventTime=${eventTime}`,
        )
        return { status: "accepted", topic: kafka.topic, clickId, adId: dto.adId, eventTime }
    }
}

import { randomUUID } from "node:crypto"
import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import { ClientKafka } from "@nestjs/microservices"
import { KafkaConfig } from "../config/kafka.config"
import { PublishClickDto } from "./publish-click.dto"

/**
 * Producer service: appends a click envelope to Kafka.
 *
 * The `eventTime` field can be overridden in the request to simulate
 * back-dated events — this is used to demonstrate watermark behaviour and
 * late-data detection in the downstream processor.
 */
@Injectable()
export class EventsService implements OnModuleInit {
    private readonly logger = new Logger(EventsService.name)

    constructor(
        @Inject("KAFKA_PRODUCER") private readonly kafka: ClientKafka,
        private readonly config: ConfigService,
    ) {}

    async onModuleInit(): Promise<void> {
        await this.kafka.connect()
    }

    /**
     * Appends one click event to the topic.
     *
     * @param dto Incoming click payload; `eventTime` defaults to server now.
     * @returns Acknowledgement with `eventTime` echoed back.
     */
    async publish(
        dto: PublishClickDto,
    ): Promise<{ status: string; topic: string; clickId: string; adId: string; eventTime: string }> {
        const kafka = this.config.getOrThrow<KafkaConfig>("kafka")
        const clickId = dto.clickId ?? randomUUID()
        // Allow the caller to supply an explicit eventTime (past or future) to
        // test watermarking and late-data behaviour in the processor.
        const eventTime = dto.eventTime ?? new Date().toISOString()
        const envelope = {
            clickId,
            adId: dto.adId,
            payload: dto.payload ?? {},
            eventTime,
        }
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

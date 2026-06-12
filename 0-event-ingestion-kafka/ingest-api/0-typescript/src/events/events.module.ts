import { Module } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import { ClientsModule, Transport } from "@nestjs/microservices"
import { KafkaConfig } from "../config/kafka.config"
import { EventsController } from "./events.controller"
import { EventsService } from "./events.service"

/**
 * Wires the Kafka producer client and the HTTP ingestion route together.
 *
 * The client is registered async so its broker list comes from `ConfigService`
 * (env-driven) rather than being hard-coded.
 */
@Module({
    imports: [
        ClientsModule.registerAsync([
            {
                name: "KAFKA_PRODUCER",
                inject: [ConfigService],
                useFactory: (config: ConfigService) => {
                    const kafka = config.getOrThrow<KafkaConfig>("kafka")
                    return {
                        transport: Transport.KAFKA,
                        options: {
                            client: { clientId: kafka.clientId, brokers: kafka.brokers },
                        },
                    }
                },
            },
        ]),
    ],
    controllers: [EventsController],
    providers: [EventsService],
})
export class EventsModule {}

import { Module } from "@nestjs/common"
import { ClientsModule, Transport } from "@nestjs/microservices"
import { ConfigModule, ConfigService } from "@nestjs/config"
import { EventsController } from "./events.controller"
import { EventsService } from "./events.service"
import { KafkaConfig } from "../config/kafka.config"

/**
 * Feature module: wires the Kafka producer client and the HTTP controller.
 */
@Module({
    imports: [
        ClientsModule.registerAsync([
            {
                name: "KAFKA_PRODUCER",
                imports: [ConfigModule],
                inject: [ConfigService],
                useFactory: (config: ConfigService) => {
                    const kafka = config.getOrThrow<KafkaConfig>("kafka")
                    return {
                        transport: Transport.KAFKA,
                        options: {
                            client: {
                                clientId: kafka.clientId,
                                brokers: kafka.bootstrapServers,
                            },
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

import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import kafkaConfig from "./config/kafka.config"
import { EventsModule } from "./events/events.module"

/**
 * Root module: loads config and mounts the Events feature module.
 */
@Module({
    imports: [
        ConfigModule.forRoot({ isGlobal: true, load: [kafkaConfig] }),
        EventsModule,
    ],
})
export class AppModule {}

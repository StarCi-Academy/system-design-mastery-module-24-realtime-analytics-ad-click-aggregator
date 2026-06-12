import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import kafkaConfig from "./config/kafka.config"
import clickhouseConfig from "./config/clickhouse.config"
import { WindowModule } from "./window/window.module"

/**
 * Root module: loads config and mounts the Window feature module.
 */
@Module({
    imports: [
        ConfigModule.forRoot({
            isGlobal: true,
            load: [kafkaConfig, clickhouseConfig],
        }),
        WindowModule,
    ],
})
export class AppModule {}

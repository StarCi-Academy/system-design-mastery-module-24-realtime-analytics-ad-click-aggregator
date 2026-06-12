import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import kafkaConfig from "./config/kafka.config"
import { WatermarkModule } from "./watermark/watermark.module"

/**
 * Root module: loads config and mounts the Watermark feature module.
 */
@Module({
    imports: [
        ConfigModule.forRoot({ isGlobal: true, load: [kafkaConfig] }),
        WatermarkModule,
    ],
})
export class AppModule {}

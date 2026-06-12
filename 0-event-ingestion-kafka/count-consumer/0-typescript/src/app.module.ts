import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import kafkaConfig from "./config/kafka.config"
import { CounterModule } from "./counter/counter.module"

/**
 * Root module: loads global config and the counting feature module.
 */
@Module({
    imports: [
        ConfigModule.forRoot({ isGlobal: true, load: [kafkaConfig] }),
        CounterModule,
    ],
})
export class AppModule {}

import { Module } from "@nestjs/common"
import { CounterController } from "./counter.controller"
import { CounterService } from "./counter.service"

/**
 * Groups the counting service and its Kafka/HTTP controller.
 */
@Module({
    controllers: [CounterController],
    providers: [CounterService],
})
export class CounterModule {}

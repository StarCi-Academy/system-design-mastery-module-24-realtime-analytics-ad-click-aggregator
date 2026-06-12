import { Module } from "@nestjs/common"
import { WindowController } from "./window.controller"
import { WindowService } from "./window.service"

/**
 * Feature module: wires the Kafka consumer and the ClickHouse aggregation service.
 */
@Module({
    controllers: [WindowController],
    providers: [WindowService],
})
export class WindowModule {}

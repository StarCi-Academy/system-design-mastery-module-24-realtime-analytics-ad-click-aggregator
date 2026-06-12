import { Module } from "@nestjs/common"
import { WatermarkController } from "./watermark.controller"
import { WatermarkService } from "./watermark.service"

/**
 * Feature module: wires the Kafka consumer and the watermark/fraud processor.
 */
@Module({
    controllers: [WatermarkController],
    providers: [WatermarkService],
})
export class WatermarkModule {}

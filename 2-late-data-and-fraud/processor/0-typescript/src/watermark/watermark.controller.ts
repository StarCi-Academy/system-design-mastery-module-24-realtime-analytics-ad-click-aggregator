import { Controller, Get } from "@nestjs/common"
import { EventPattern, Payload } from "@nestjs/microservices"
import { ClickEnvelope, WatermarkService } from "./watermark.service"

/**
 * Consumes click events from Kafka, runs watermark + fraud detection, and
 * exposes a snapshot endpoint for e2e inspection.
 */
@Controller()
export class WatermarkController {
    constructor(private readonly watermark: WatermarkService) {}

    /**
     * Handles one click record from the `ad-clicks` topic.
     *
     * @param data Decoded click envelope.
     */
    @EventPattern("ad-clicks")
    handle(@Payload() data: ClickEnvelope): void {
        this.watermark.process(data)
    }

    /**
     * Returns the current watermark, all processed clicks and aggregate stats.
     */
    @Get("snapshot")
    snapshot(): ReturnType<WatermarkService["snapshot"]> {
        return this.watermark.snapshot()
    }
}

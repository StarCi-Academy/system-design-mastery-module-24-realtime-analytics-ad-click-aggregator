import { Body, Controller, HttpCode, Post } from "@nestjs/common"
import { EventsService } from "./events.service"
import { PublishClickDto } from "./publish-click.dto"

/**
 * HTTP entrypoint for click ingestion.
 *
 * `POST /clicks` — accepts a click event, appends it to Kafka, and returns
 * HTTP 202 immediately (fire-and-forget contract).
 */
@Controller("clicks")
export class EventsController {
    constructor(private readonly events: EventsService) {}

    /**
     * Accepts a click event and appends it to the Kafka topic.
     *
     * @param dto Validated click payload.
     * @returns Acknowledgement with `eventTime` so callers can inspect the window bucket.
     */
    @Post()
    @HttpCode(202)
    async publish(
        @Body() dto: PublishClickDto,
    ): Promise<{ status: string; topic: string; clickId: string; adId: string; eventTime: string }> {
        return this.events.publish(dto)
    }
}

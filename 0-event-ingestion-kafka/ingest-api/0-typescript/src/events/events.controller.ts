import { Body, Controller, HttpCode, Post } from "@nestjs/common"
import { EventsService } from "./events.service"
import { PublishClickDto } from "./publish-click.dto"

/**
 * HTTP entrypoint for click ingestion.
 *
 * One route only: `POST /clicks`. The 202 status code makes the fire-and-forget
 * contract explicit — the event is accepted into the log, not yet processed.
 */
@Controller("clicks")
export class EventsController {
    constructor(private readonly events: EventsService) {}

    /**
     * Accepts a click event and appends it to the Kafka topic.
     *
     * @param dto Validated click payload.
     * @returns Acknowledgement of the append (status, topic, ids).
     */
    @Post()
    @HttpCode(202)
    async publish(
        @Body() dto: PublishClickDto,
    ): Promise<{ status: string; topic: string; clickId: string; adId: string }> {
        return this.events.publish(dto)
    }
}

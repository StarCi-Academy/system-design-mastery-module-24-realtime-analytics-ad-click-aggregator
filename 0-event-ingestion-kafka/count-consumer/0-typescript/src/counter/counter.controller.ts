import { Controller, Get } from "@nestjs/common"
import { EventPattern, Payload } from "@nestjs/microservices"
import { ClickEnvelope, CounterService } from "./counter.service"

/**
 * Consumes click events from Kafka and exposes the running counts over HTTP.
 *
 * `@EventPattern` binds the topic to the Kafka consumer; the `GET /counts`
 * route lets the e2e test and learners observe the deduplicated totals without
 * scraping logs.
 */
@Controller()
export class CounterController {
    constructor(private readonly counter: CounterService) {}

    /**
     * Handles one click record from the `ad-clicks` topic.
     *
     * @param data Decoded click envelope (Nest auto-parses the JSON value).
     */
    @EventPattern("ad-clicks")
    handle(@Payload() data: ClickEnvelope): void {
        this.counter.record(data)
    }

    /**
     * Returns the current per-ad counts and duplicate tally.
     */
    @Get("counts")
    counts(): {
        counts: Record<string, number>
        totalUnique: number
        duplicatesRejected: number
    } {
        return this.counter.snapshot()
    }
}

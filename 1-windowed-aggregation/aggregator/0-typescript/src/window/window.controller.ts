import { Controller, Get } from "@nestjs/common"
import { EventPattern, Payload } from "@nestjs/microservices"
import { ClickEnvelope, WindowService } from "./window.service"

/**
 * Consumes click events from Kafka and exposes the in-memory snapshot over HTTP.
 *
 * `@EventPattern` binds the topic to the Kafka consumer; the `GET /snapshot`
 * route lets the e2e test observe bucket counts before the flush interval fires.
 */
@Controller()
export class WindowController {
    constructor(private readonly window: WindowService) {}

    /**
     * Handles one click record from the `ad-clicks` topic.
     *
     * @param data Decoded click envelope.
     */
    @EventPattern("ad-clicks")
    handle(@Payload() data: ClickEnvelope): void {
        this.window.record(data)
    }

    /**
     * Returns the current in-memory window bucket snapshot.
     */
    @Get("snapshot")
    snapshot(): Array<{ adId: string; windowStart: string; count: number }> {
        return this.window.snapshot()
    }

    /**
     * Triggers an immediate flush to ClickHouse (useful for e2e tests).
     */
    @Get("flush")
    async flush(): Promise<{ flushed: boolean }> {
        await this.window.flush()
        return { flushed: true }
    }
}

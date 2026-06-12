import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import { createClient, ClickHouseClient } from "@clickhouse/client"
import { ClickHouseConfig } from "../config/clickhouse.config"

/** Shape of one click envelope emitted by ingest-api. */
export interface ClickEnvelope {
    clickId: string
    adId: string
    payload: Record<string, unknown>
    eventTime: string
}

/**
 * Windowed aggregation service.
 *
 * Groups click events by a **1-minute tumbling window** (floor the event
 * timestamp to the minute). For each (adId, windowStart) pair it accumulates
 * a count in memory, then flushes the batch to ClickHouse.
 *
 * ClickHouse table uses `ReplacingMergeTree` so upserts are idempotent: if the
 * aggregator restarts and replays a window, ClickHouse keeps only the latest
 * row per (adId, windowStart) key after the background merge.
 */
@Injectable()
export class WindowService implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger(WindowService.name)

    /** In-memory accumulator: key = `${adId}|${windowStart}`, value = count. */
    private readonly buckets = new Map<string, number>()

    /** ClickHouse client instance. */
    private ch!: ClickHouseClient

    /** Flush interval handle — flushes every 10 seconds. */
    private flushInterval?: NodeJS.Timeout

    constructor(private readonly config: ConfigService) {}

    /**
     * Initialises the ClickHouse client and ensures the target table exists.
     *
     * The table `ad_click_counts` stores one row per (adId, 1-minute window).
     * `ReplacingMergeTree` deduplicates on (adId, windowStart) after background
     * merge, making upserts safe under at-least-once delivery.
     */
    async onModuleInit(): Promise<void> {
        const ch = this.config.getOrThrow<ClickHouseConfig>("clickhouse")
        this.ch = createClient({
            url: ch.url,
            database: ch.database,
            username: ch.username,
            password: ch.password,
        })
        await this.ch.command({
            query: `
                CREATE TABLE IF NOT EXISTS ad_click_counts (
                    adId       String,
                    windowStart DateTime,
                    count       UInt64
                )
                ENGINE = ReplacingMergeTree()
                ORDER BY (adId, windowStart)
            `,
        })
        this.logger.log("ClickHouse table ad_click_counts ready")
        // Flush every 10 seconds so counts reach ClickHouse without waiting
        // for a large batch to fill.
        this.flushInterval = setInterval(() => this.flush(), 10_000)
    }

    /**
     * Cleans up the flush interval and ClickHouse client on shutdown.
     */
    async onModuleDestroy(): Promise<void> {
        if (this.flushInterval) clearInterval(this.flushInterval)
        await this.flush()
        await this.ch.close()
    }

    /**
     * Records one click event into the in-memory tumbling-window bucket.
     *
     * The window start is the event timestamp truncated to the minute.
     * This is the simplest tumbling-window implementation: no state store, no
     * watermark, no out-of-order tolerance — those are introduced in the next
     * lesson (2-late-data-and-fraud).
     *
     * @param envelope Decoded click event from the Kafka topic.
     */
    record(envelope: ClickEnvelope): void {
        // Truncate event time to 1-minute boundary.
        const windowStart = toWindowStart(envelope.eventTime)
        const key = `${envelope.adId}|${windowStart}`
        const prev = this.buckets.get(key) ?? 0
        this.buckets.set(key, prev + 1)
        this.logger.debug(
            `bucket adId=${envelope.adId} window=${windowStart} count=${prev + 1}`,
        )
    }

    /**
     * Flushes all in-memory buckets to ClickHouse as a batch INSERT.
     *
     * ClickHouse DateTime columns expect `YYYY-MM-DD HH:MM:SS` format (not
     * ISO 8601 with 'T' and 'Z'). The `toClickHouseDateTime` helper converts
     * the stored ISO window key to the required format.
     *
     * After a successful flush the in-memory map is cleared.  If ClickHouse is
     * unavailable the error is logged but the map is NOT cleared — the next
     * flush will retry with the accumulated counts.
     */
    async flush(): Promise<void> {
        if (this.buckets.size === 0) return
        const rows = Array.from(this.buckets.entries()).map(([key, count]) => {
            const [adId, windowStart] = key.split("|")
            return {
                adId,
                // ClickHouse DateTime requires 'YYYY-MM-DD HH:MM:SS' without the T/Z.
                windowStart: toClickHouseDateTime(windowStart),
                count,
            }
        })
        try {
            await this.ch.insert({
                table: "ad_click_counts",
                values: rows,
                format: "JSONEachRow",
            })
            this.logger.log(`Flushed ${rows.length} buckets to ClickHouse`)
            this.buckets.clear()
        } catch (err) {
            this.logger.error(`Flush failed, will retry: ${(err as Error).message}`)
        }
    }

    /**
     * Returns the current in-memory bucket snapshot (for e2e inspection
     * before the flush interval fires).
     */
    snapshot(): Array<{ adId: string; windowStart: string; count: number }> {
        return Array.from(this.buckets.entries()).map(([key, count]) => {
            const [adId, windowStart] = key.split("|")
            return { adId, windowStart, count }
        })
    }
}

/**
 * Truncates an ISO timestamp to the 1-minute boundary.
 *
 * @example
 *   toWindowStart("2026-06-12T10:05:34.123Z") // "2026-06-12T10:05:00.000Z"
 */
function toWindowStart(iso: string): string {
    const d = new Date(iso)
    d.setSeconds(0, 0)
    return d.toISOString()
}

/**
 * Converts an ISO 8601 timestamp to ClickHouse DateTime format.
 *
 * ClickHouse `DateTime` columns expect `YYYY-MM-DD HH:MM:SS` (no T, no Z).
 * This strips the T/Z and the milliseconds fragment.
 *
 * @example
 *   toClickHouseDateTime("2026-06-12T10:05:00.000Z") // "2026-06-12 10:05:00"
 */
function toClickHouseDateTime(iso: string): string {
    // "2026-06-12T10:05:00.000Z" -> "2026-06-12 10:05:00"
    return iso.replace("T", " ").substring(0, 19)
}

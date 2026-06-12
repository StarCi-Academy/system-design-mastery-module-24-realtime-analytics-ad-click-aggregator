import { Injectable, Logger } from "@nestjs/common"

/** Envelope shape emitted by ingest-api. */
export interface ClickEnvelope {
    clickId: string
    adId: string
    payload: Record<string, unknown>
    timestamp: string
}

/**
 * In-memory counting + deduplication state for ad clicks.
 *
 * The dedup store (`seen`) is the idempotency mechanism: the first time a
 * `clickId` arrives it is counted; any later arrival of the SAME `clickId` is
 * dropped. This makes consumption idempotent under at-least-once delivery,
 * where Kafka may redeliver a record after a rebalance or restart.
 *
 * The store is in-memory on purpose to keep the lesson dependency-free; the
 * theory section discusses why production swaps it for Redis/RocksDB so dedup
 * survives a consumer crash.
 */
@Injectable()
export class CounterService {
    private readonly logger = new Logger(CounterService.name)

    /** Set of clickIds already counted — the deduplication index. */
    private readonly seen = new Set<string>()

    /** Per-ad accepted (unique) click counts. */
    private readonly counts = new Map<string, number>()

    /** Total duplicates rejected since boot, for observability. */
    private duplicates = 0

    /**
     * Records one click, deduplicating by `clickId`.
     *
     * @param envelope The decoded click event.
     * @returns `true` if counted as new, `false` if rejected as a duplicate.
     */
    record(envelope: ClickEnvelope): boolean {
        // Duplicate check FIRST — an already-seen clickId never re-increments.
        if (this.seen.has(envelope.clickId)) {
            this.duplicates += 1
            this.logger.warn(
                `DUPLICATE dropped clickId=${envelope.clickId} adId=${envelope.adId}`,
            )
            return false
        }
        this.seen.add(envelope.clickId)
        const next = (this.counts.get(envelope.adId) ?? 0) + 1
        this.counts.set(envelope.adId, next)
        this.logger.log(
            `COUNTED clickId=${envelope.clickId} adId=${envelope.adId} total=${next}`,
        )
        return true
    }

    /**
     * Returns a snapshot of current counts.
     *
     * @returns Per-ad counts, total unique clicks, and duplicates rejected.
     */
    snapshot(): {
        counts: Record<string, number>
        totalUnique: number
        duplicatesRejected: number
    } {
        const counts: Record<string, number> = {}
        let totalUnique = 0
        for (const [adId, n] of this.counts.entries()) {
            counts[adId] = n
            totalUnique += n
        }
        return { counts, totalUnique, duplicatesRejected: this.duplicates }
    }
}

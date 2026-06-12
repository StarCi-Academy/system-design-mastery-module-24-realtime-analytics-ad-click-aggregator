import { Injectable, Logger } from "@nestjs/common"

/** Shape of one click envelope emitted by ingest-api. */
export interface ClickEnvelope {
    clickId: string
    adId: string
    ip?: string
    payload: Record<string, unknown>
    eventTime: string
}

/** Classification of a processed click event. */
export interface ProcessedClick {
    clickId: string
    adId: string
    ip: string
    eventTime: string
    /** "accepted" | "late" | "fraud" */
    status: "accepted" | "late" | "fraud"
    lateMs?: number
    reason?: string
}

/**
 * Watermark-based late-data processor with click-fraud detection.
 *
 * **Watermark:** tracks the maximum `eventTime` seen so far. A new event is
 * classified as *late* if its `eventTime` is more than `WATERMARK_TOLERANCE_MS`
 * (default 30 seconds) behind the current watermark. Late events are still
 * counted but flagged — the downstream analytics can decide to include or
 * exclude them.
 *
 * **Fraud detection:** counts clicks per (ip, 1-minute window). If a single IP
 * exceeds `FRAUD_THRESHOLD` (default 5) clicks in one window it is flagged as
 * fraud. The detection is intentionally simple to demonstrate the mechanism;
 * production upgrades to Redis TTL windows or a streaming framework.
 */
@Injectable()
export class WatermarkService {
    private readonly logger = new Logger(WatermarkService.name)

    /** Watermark tolerance — events older than this relative to the watermark are "late". */
    private readonly watermarkToleranceMs =
        parseInt(process.env.WATERMARK_TOLERANCE_MS ?? "30000", 10)

    /** Fraud threshold — clicks-per-IP-per-minute above this are "fraud". */
    private readonly fraudThreshold =
        parseInt(process.env.FRAUD_THRESHOLD ?? "5", 10)

    /** Current watermark: the maximum eventTime seen (ms since epoch). */
    private watermarkMs = 0

    /** Per-(ip,windowStart) click count. */
    private readonly ipWindowCounts = new Map<string, number>()

    /** Processed click log for e2e inspection. */
    private readonly processed: ProcessedClick[] = []

    /**
     * Processes one click event: advances the watermark, checks for late data
     * and fraud, and records the result.
     *
     * @param envelope Decoded click envelope from the Kafka topic.
     * @returns The `ProcessedClick` result with status and diagnostic fields.
     */
    process(envelope: ClickEnvelope): ProcessedClick {
        const ip = envelope.ip ?? (envelope.payload?.ip as string) ?? "unknown"
        const eventMs = new Date(envelope.eventTime).getTime()
        const windowStart = toWindowStart(envelope.eventTime)
        const windowKey = `${ip}|${windowStart}`

        // Advance watermark only if this event is not already late.
        let status: ProcessedClick["status"] = "accepted"
        let lateMs: number | undefined
        let reason: string | undefined

        // Check for late data first.
        if (this.watermarkMs > 0 && eventMs < this.watermarkMs - this.watermarkToleranceMs) {
            lateMs = this.watermarkMs - eventMs
            status = "late"
            reason = `eventTime is ${lateMs}ms behind watermark (tolerance ${this.watermarkToleranceMs}ms)`
            this.logger.warn(
                `LATE clickId=${envelope.clickId} lateMs=${lateMs}`,
            )
        } else {
            // Advance watermark.
            if (eventMs > this.watermarkMs) {
                this.watermarkMs = eventMs
            }

            // Fraud detection: increment per-IP-window count.
            const ipCount = (this.ipWindowCounts.get(windowKey) ?? 0) + 1
            this.ipWindowCounts.set(windowKey, ipCount)

            if (ipCount > this.fraudThreshold) {
                status = "fraud"
                reason = `IP ${ip} sent ${ipCount} clicks in window ${windowStart} (threshold ${this.fraudThreshold})`
                this.logger.warn(
                    `FRAUD clickId=${envelope.clickId} ip=${ip} windowCount=${ipCount}`,
                )
            } else {
                this.logger.log(
                    `ACCEPTED clickId=${envelope.clickId} adId=${envelope.adId} ip=${ip} ipWindowCount=${ipCount}`,
                )
            }
        }

        const result: ProcessedClick = {
            clickId: envelope.clickId,
            adId: envelope.adId,
            ip,
            eventTime: envelope.eventTime,
            status,
            ...(lateMs !== undefined && { lateMs }),
            ...(reason !== undefined && { reason }),
        }
        this.processed.push(result)
        return result
    }

    /**
     * Returns a snapshot of all processed clicks (for e2e inspection).
     */
    snapshot(): {
        watermarkIso: string
        processed: ProcessedClick[]
        stats: { accepted: number; late: number; fraud: number }
    } {
        const stats = { accepted: 0, late: 0, fraud: 0 }
        for (const p of this.processed) stats[p.status]++
        return {
            watermarkIso: this.watermarkMs
                ? new Date(this.watermarkMs).toISOString()
                : "none",
            processed: this.processed,
            stats,
        }
    }
}

/**
 * Truncates an ISO timestamp to the 1-minute boundary.
 */
function toWindowStart(iso: string): string {
    const d = new Date(iso)
    d.setSeconds(0, 0)
    return d.toISOString()
}

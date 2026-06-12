package com.starci.counter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * In-memory counting + deduplication state for ad clicks.
 *
 * <p>The dedup store ({@code seen}) is the idempotency mechanism: the first time
 * a {@code clickId} arrives it is counted; any later arrival of the SAME
 * {@code clickId} is dropped. This makes consumption idempotent under
 * at-least-once delivery, where Kafka may redeliver a record after a rebalance
 * or restart.
 *
 * <p>The store is in-memory on purpose to keep the lesson dependency-free; the
 * theory section discusses why production swaps it for Redis/RocksDB so dedup
 * survives a consumer crash.
 */
@Service
public class CounterService {

    private static final Logger log = LoggerFactory.getLogger(CounterService.class);

    /** Set of clickIds already counted — the deduplication index. */
    private final Set<String> seen = ConcurrentHashMap.newKeySet();

    /** Per-ad accepted (unique) click counts. */
    private final Map<String, Integer> counts = new ConcurrentHashMap<>();

    /** Total duplicates rejected since boot, for observability. */
    private volatile int duplicates = 0;

    /**
     * Records one click, deduplicating by {@code clickId}.
     *
     * @param clickId the idempotency key
     * @param adId    the ad the click belongs to
     * @return {@code true} if counted as new, {@code false} if rejected as duplicate
     */
    public synchronized boolean record(String clickId, String adId) {
        // Duplicate check FIRST — an already-seen clickId never re-increments.
        if (seen.contains(clickId)) {
            duplicates += 1;
            log.warn("DUPLICATE dropped clickId={} adId={}", clickId, adId);
            return false;
        }
        seen.add(clickId);
        int next = counts.merge(adId, 1, Integer::sum);
        log.info("COUNTED clickId={} adId={} total={}", clickId, adId, next);
        return true;
    }

    /**
     * Returns a snapshot of the current counts.
     *
     * @return per-ad counts, total unique clicks, and duplicates rejected
     */
    public synchronized Map<String, Object> snapshot() {
        Map<String, Integer> snapshotCounts = new LinkedHashMap<>();
        int totalUnique = 0;
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            snapshotCounts.put(e.getKey(), e.getValue());
            totalUnique += e.getValue();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("counts", snapshotCounts);
        out.put("totalUnique", totalUnique);
        out.put("duplicatesRejected", duplicates);
        return out;
    }
}

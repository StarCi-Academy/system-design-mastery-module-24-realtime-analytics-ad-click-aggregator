namespace CountConsumer;

/// <summary>
/// In-memory counting + deduplication state for ad clicks.
/// </summary>
/// <remarks>
/// The dedup store (<c>_seen</c>) is the idempotency mechanism: the first time a
/// <c>clickId</c> arrives it is counted; any later arrival of the SAME
/// <c>clickId</c> is dropped. This makes consumption idempotent under
/// at-least-once delivery, where Kafka may redeliver a record after a rebalance
/// or restart.
/// <para>
/// The store is in-memory on purpose to keep the lesson dependency-free; the
/// theory section discusses why production swaps it for Redis/RocksDB so dedup
/// survives a consumer crash.
/// </para>
/// </remarks>
public sealed class CounterService
{
    private readonly ILogger<CounterService> _logger;
    private readonly object _lock = new();

    /// <summary>Set of clickIds already counted — the deduplication index.</summary>
    private readonly HashSet<string> _seen = new();

    /// <summary>Per-ad accepted (unique) click counts.</summary>
    private readonly Dictionary<string, int> _counts = new();

    /// <summary>Total duplicates rejected since boot, for observability.</summary>
    private int _duplicates;

    public CounterService(ILogger<CounterService> logger) => _logger = logger;

    /// <summary>Records one click, deduplicating by <c>clickId</c>.</summary>
    /// <returns><c>true</c> if counted as new, <c>false</c> if rejected as duplicate.</returns>
    public bool Record(string clickId, string adId)
    {
        lock (_lock)
        {
            // Duplicate check FIRST — an already-seen clickId never re-increments.
            if (_seen.Contains(clickId))
            {
                _duplicates += 1;
                _logger.LogWarning("DUPLICATE dropped clickId={ClickId} adId={AdId}", clickId, adId);
                return false;
            }
            _seen.Add(clickId);
            var next = _counts.GetValueOrDefault(adId) + 1;
            _counts[adId] = next;
            _logger.LogInformation("COUNTED clickId={ClickId} adId={AdId} total={Total}", clickId, adId, next);
            return true;
        }
    }

    /// <summary>Returns a snapshot of the current counts.</summary>
    public object Snapshot()
    {
        lock (_lock)
        {
            var counts = new Dictionary<string, int>();
            var totalUnique = 0;
            foreach (var (adId, n) in _counts)
            {
                counts[adId] = n;
                totalUnique += n;
            }
            return new { counts, totalUnique, duplicatesRejected = _duplicates };
        }
    }
}

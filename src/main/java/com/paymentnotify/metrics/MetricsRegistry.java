package com.paymentnotify.metrics;

import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.DeliverySource;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.NotificationHistoryEntry;
import com.paymentnotify.domain.PaymentEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central, lightweight metrics hub. Everything here is in-memory counters
 * and a capped recent-history ring buffer — no per-event durable storage,
 * per AGENTS.md ("do not write every successful event to a relational DB").
 *
 * Consumed by the REST metrics API (Phase 9) and updated by C1/C2/retry/DLQ
 * as they process events.
 */
@Component
public class MetricsRegistry {

    private static final int HISTORY_CAPACITY = 500;

    public final RateCounter processedRate = new RateCounter();
    public final RateCounter deliveredRate = new RateCounter();
    public final RateCounter retryScheduledRate = new RateCounter();
    public final RateCounter dlqRate = new RateCounter();

    private final Map<MerchantId, MerchantStats> merchantStats = new ConcurrentHashMap<>();
    private final Deque<NotificationHistoryEntry> recentHistory = new ArrayDeque<>(HISTORY_CAPACITY);
    private final Object historyLock = new Object();

    public MerchantStats statsFor(MerchantId merchantId) {
        return merchantStats.computeIfAbsent(merchantId, id -> new MerchantStats());
    }

    public Map<MerchantId, MerchantStats> allMerchantStats() {
        return merchantStats;
    }

    public void recordProcessed() {
        processedRate.increment();
    }

    public void recordDelivery(PaymentEvent event, DeliverySource source, int attempt, DeliveryResult result) {
        deliveredRate.increment();
        MerchantStats stats = statsFor(event.merchantId());
        stats.recordOutcome(result.success(), result.latencyMs());

        NotificationHistoryEntry entry = new NotificationHistoryEntry(
                event.eventId(), event.paymentId(), event.merchantId(), attempt, source,
                result.success(), result.httpStatus(), result.latencyMs(), result.failureDetail(), Instant.now());
        synchronized (historyLock) {
            if (recentHistory.size() >= HISTORY_CAPACITY) {
                recentHistory.removeFirst();
            }
            recentHistory.addLast(entry);
        }
    }

    public void recordDeliveryStart(MerchantId merchantId) {
        statsFor(merchantId).recordStart();
    }

    public void recordRetryScheduled() {
        retryScheduledRate.increment();
    }

    public void recordDlq() {
        dlqRate.increment();
    }

    public List<NotificationHistoryEntry> recentHistory() {
        synchronized (historyLock) {
            return List.copyOf(recentHistory);
        }
    }
}

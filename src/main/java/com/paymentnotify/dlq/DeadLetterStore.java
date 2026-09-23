package com.paymentnotify.dlq;

import com.paymentnotify.domain.DeadLetterEntry;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Where notifications land once they can never succeed: max retry attempts
 * exhausted, or a permanent (4xx-class) failure. In-memory and capped, like
 * the rest of this system's observability state — the demo's notion of a
 * dead-letter "queue" is a bounded, inspectable list, not a durable log.
 * {@code totalCount()} tracks the lifetime count even once old entries have
 * been evicted from the bounded view.
 */
@Component
public class DeadLetterStore implements DeadLetterSink {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterStore.class);
    private static final int CAPACITY = 5000;

    private final Deque<DeadLetterEntry> entries = new ArrayDeque<>(CAPACITY);
    private final Object lock = new Object();
    private final AtomicLong totalCount = new AtomicLong();
    private final MetricsRegistry metrics;

    public DeadLetterStore(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    @Override
    public void send(PaymentEvent event, int attemptsMade, String reason) {
        DeadLetterEntry entry = new DeadLetterEntry(event.eventId(), event.paymentId(), event.merchantId(),
                attemptsMade, reason, event, Instant.now());
        synchronized (lock) {
            if (entries.size() >= CAPACITY) {
                entries.removeFirst();
            }
            entries.addLast(entry);
        }
        totalCount.incrementAndGet();
        metrics.recordDlq();
        log.warn("Moved to DLQ: eventId={} merchant={} attempts={} reason={}",
                event.eventId(), event.merchantId(), attemptsMade, reason);
    }

    public List<DeadLetterEntry> all() {
        synchronized (lock) {
            return List.copyOf(entries);
        }
    }

    public List<DeadLetterEntry> forMerchant(MerchantId merchantId) {
        synchronized (lock) {
            return entries.stream().filter(e -> e.merchantId().equals(merchantId)).toList();
        }
    }

    public long totalCount() {
        return totalCount.get();
    }

    public int currentSize() {
        synchronized (lock) {
            return entries.size();
        }
    }
}

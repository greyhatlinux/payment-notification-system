package com.paymentnotify.metrics;

import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe per-merchant counters. One instance per merchant, never removed (bounded by merchant count). */
public class MerchantStats {

    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong successes = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong totalLatencyMs = new AtomicLong();
    private final AtomicLong activeRequests = new AtomicLong();
    public final RateCounter requestRate = new RateCounter();

    public void recordStart() {
        activeRequests.incrementAndGet();
    }

    public void recordOutcome(boolean success, long latencyMs) {
        activeRequests.decrementAndGet();
        requests.incrementAndGet();
        requestRate.increment();
        totalLatencyMs.addAndGet(latencyMs);
        if (success) {
            successes.incrementAndGet();
        } else {
            failures.incrementAndGet();
        }
    }

    public long requests() {
        return requests.get();
    }

    public long successes() {
        return successes.get();
    }

    public long failures() {
        return failures.get();
    }

    public long activeRequests() {
        return activeRequests.get();
    }

    public double successRate() {
        long total = requests.get();
        return total == 0 ? 1.0 : (double) successes.get() / total;
    }

    public double averageLatencyMs() {
        long total = requests.get();
        return total == 0 ? 0.0 : (double) totalLatencyMs.get() / total;
    }
}

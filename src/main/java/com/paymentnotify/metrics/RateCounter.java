package com.paymentnotify.metrics;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Lightweight, lock-free events-per-second counter using a ring of
 * per-second buckets. Used throughout for ingestion/processing/delivery
 * rates. Deliberately approximate (a metrics counter, not a ledger) — a
 * handful of races around second boundaries are acceptable.
 */
public final class RateCounter {

    private final int windowSeconds;
    private final AtomicLongArray counts;
    private final AtomicLongArray slotEpoch;
    private final AtomicLong total = new AtomicLong();

    public RateCounter(int windowSeconds) {
        this.windowSeconds = Math.max(windowSeconds, 2);
        this.counts = new AtomicLongArray(this.windowSeconds);
        this.slotEpoch = new AtomicLongArray(this.windowSeconds);
    }

    public RateCounter() {
        this(10);
    }

    public void increment() {
        increment(1);
    }

    public void increment(long n) {
        long epochSecond = Instant.now().getEpochSecond();
        int slot = (int) Math.floorMod(epochSecond, windowSeconds);
        if (slotEpoch.getAndSet(slot, epochSecond) != epochSecond) {
            counts.set(slot, 0);
        }
        counts.addAndGet(slot, n);
        total.addAndGet(n);
    }

    /** Average rate per second over the last full window, excluding the current (partial) second. */
    public double ratePerSecond() {
        long nowEpoch = Instant.now().getEpochSecond();
        long sum = 0;
        int full = 0;
        for (int i = 1; i < windowSeconds; i++) {
            long epochSecond = nowEpoch - i;
            int slot = (int) Math.floorMod(epochSecond, windowSeconds);
            if (slotEpoch.get(slot) == epochSecond) {
                sum += counts.get(slot);
                full++;
            }
        }
        return full == 0 ? 0.0 : (double) sum / full;
    }

    public long total() {
        return total.get();
    }
}

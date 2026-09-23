package com.paymentnotify.resilience;

/**
 * Simple per-merchant token bucket. Capacity equals one second's worth of
 * permits (i.e. a burst of up to {@code permitsPerSecond} is allowed), which
 * refills continuously. {@code tryAcquire()} never blocks — a rejected
 * acquire just means "try again slightly later", surfaced to the caller as
 * a retryable outcome rather than a merchant failure.
 */
public class RateLimiter {

    private final double permitsPerSecond;
    private final double capacity;
    private double availableTokens;
    private long lastRefillNanos;

    public RateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        this.capacity = Math.max(permitsPerSecond, 1.0);
        this.availableTokens = capacity;
        this.lastRefillNanos = System.nanoTime();
    }

    public synchronized boolean tryAcquire() {
        refill();
        if (availableTokens >= 1.0) {
            availableTokens -= 1.0;
            return true;
        }
        return false;
    }

    private void refill() {
        long now = System.nanoTime();
        double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000.0;
        if (elapsedSeconds <= 0) {
            return;
        }
        availableTokens = Math.min(capacity, availableTokens + elapsedSeconds * permitsPerSecond);
        lastRefillNanos = now;
    }
}

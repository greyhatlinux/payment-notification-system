package com.paymentnotify.resilience;

import java.util.concurrent.Semaphore;

/**
 * Bundles the three merchant-scoped resilience controls the caller must
 * pass through before making an HTTP call: circuit breaker, rate limit and
 * concurrency limit. One instance per merchant; a failing/overloaded
 * merchant's guard never affects another merchant's.
 */
public class MerchantGuard {

    private final CircuitBreaker circuitBreaker;
    private final RateLimiter rateLimiter;
    private final Semaphore concurrencySemaphore;
    private final int concurrencyLimit;

    public MerchantGuard(CircuitBreaker circuitBreaker, RateLimiter rateLimiter, int concurrencyLimit) {
        this.circuitBreaker = circuitBreaker;
        this.rateLimiter = rateLimiter;
        this.concurrencyLimit = Math.max(concurrencyLimit, 1);
        this.concurrencySemaphore = new Semaphore(this.concurrencyLimit, false);
    }

    public CircuitBreaker circuitBreaker() {
        return circuitBreaker;
    }

    public RateLimiter rateLimiter() {
        return rateLimiter;
    }

    public boolean tryAcquireConcurrency() {
        return concurrencySemaphore.tryAcquire();
    }

    public void releaseConcurrency() {
        concurrencySemaphore.release();
    }

    public int activeConcurrency() {
        return concurrencyLimit - concurrencySemaphore.availablePermits();
    }

    public int concurrencyLimit() {
        return concurrencyLimit;
    }
}

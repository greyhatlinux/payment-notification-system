package com.paymentnotify.resilience;

import com.paymentnotify.config.CircuitBreakerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standard CLOSED -> OPEN -> HALF_OPEN -> CLOSED circuit breaker, scoped to
 * a single merchant. One instance is shared between C1 and C2 for that
 * merchant (see {@code MerchantGuardRegistry}), so a merchant identified as
 * unhealthy by a synchronous C1 call immediately stops retry workers from
 * hammering it too, and vice versa.
 *
 * CLOSED:    calls flow normally; outcomes are recorded in a sliding window.
 *            Once at least {@code minimumCalls} have been recorded and the
 *            failure rate reaches {@code failureRateThreshold}, trips OPEN.
 * OPEN:      calls are rejected outright (no HTTP call made) until
 *            {@code openStateWaitMs} has elapsed, then the next caller
 *            transitions the breaker to HALF_OPEN.
 * HALF_OPEN: a small number of trial calls ({@code halfOpenPermittedCalls})
 *            are allowed through. Any failure trips back to OPEN; enough
 *            successes closes the breaker and resets the window.
 */
public class CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    private final String name;
    private final int windowSize;
    private final int minimumCalls;
    private final double failureRateThreshold;
    private final long openStateWaitMs;
    private final int halfOpenPermittedCalls;

    private final Object lock = new Object();
    private final boolean[] window;
    private int windowIndex = 0;
    private int windowCount = 0;
    private int windowFailures = 0;

    private volatile CircuitState state = CircuitState.CLOSED;
    private volatile long stateChangedAtMillis = System.currentTimeMillis();
    private final AtomicInteger halfOpenInFlight = new AtomicInteger(0);
    private final AtomicInteger halfOpenSuccesses = new AtomicInteger(0);

    public CircuitBreaker(String name, CircuitBreakerProperties props) {
        this.name = name;
        this.windowSize = Math.max(props.slidingWindowSize(), 1);
        this.minimumCalls = Math.max(props.minimumCalls(), 1);
        this.failureRateThreshold = props.failureRateThreshold();
        this.openStateWaitMs = props.openStateWaitMs();
        this.halfOpenPermittedCalls = Math.max(props.halfOpenPermittedCalls(), 1);
        this.window = new boolean[windowSize];
    }

    /** Whether a caller may attempt an HTTP call right now. */
    public boolean allowRequest() {
        if (state == CircuitState.CLOSED) {
            return true;
        }
        synchronized (lock) {
            if (state == CircuitState.OPEN) {
                if (System.currentTimeMillis() - stateChangedAtMillis >= openStateWaitMs) {
                    transitionTo(CircuitState.HALF_OPEN);
                } else {
                    return false;
                }
            }
            if (state == CircuitState.HALF_OPEN) {
                if (halfOpenInFlight.get() >= halfOpenPermittedCalls) {
                    return false;
                }
                halfOpenInFlight.incrementAndGet();
                return true;
            }
            return state == CircuitState.CLOSED;
        }
    }

    /** Record the outcome of a call that {@link #allowRequest()} permitted. */
    public void recordResult(boolean success) {
        synchronized (lock) {
            switch (state) {
                case CLOSED -> {
                    recordInWindow(success);
                    if (windowCount >= minimumCalls) {
                        double failureRate = (double) windowFailures / windowCount;
                        if (failureRate >= failureRateThreshold) {
                            transitionTo(CircuitState.OPEN);
                        }
                    }
                }
                case HALF_OPEN -> {
                    halfOpenInFlight.decrementAndGet();
                    if (!success) {
                        transitionTo(CircuitState.OPEN);
                    } else if (halfOpenSuccesses.incrementAndGet() >= halfOpenPermittedCalls) {
                        transitionTo(CircuitState.CLOSED);
                    }
                }
                case OPEN -> {
                    // Calls shouldn't complete while OPEN (allowRequest() blocks them), but
                    // ignore defensively rather than corrupt state.
                }
            }
        }
    }

    public CircuitState state() {
        return state;
    }

    public double currentFailureRate() {
        synchronized (lock) {
            return windowCount == 0 ? 0.0 : (double) windowFailures / windowCount;
        }
    }

    private void recordInWindow(boolean success) {
        if (windowCount == windowSize) {
            boolean evicted = window[windowIndex];
            if (!evicted) {
                windowFailures--;
            }
        } else {
            windowCount++;
        }
        window[windowIndex] = success;
        if (!success) {
            windowFailures++;
        }
        windowIndex = (windowIndex + 1) % windowSize;
    }

    private void transitionTo(CircuitState newState) {
        if (state == newState) {
            return;
        }
        log.info("Circuit breaker [{}] {} -> {}", name, state, newState);
        state = newState;
        stateChangedAtMillis = System.currentTimeMillis();
        if (newState == CircuitState.HALF_OPEN) {
            halfOpenInFlight.set(0);
            halfOpenSuccesses.set(0);
        } else if (newState == CircuitState.CLOSED) {
            windowCount = 0;
            windowFailures = 0;
            windowIndex = 0;
        }
    }
}

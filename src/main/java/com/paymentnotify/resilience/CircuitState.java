package com.paymentnotify.resilience;

/** Standard three-state circuit breaker state machine: CLOSED -> OPEN -> HALF_OPEN -> CLOSED. */
public enum CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN
}

package com.paymentnotify.api.dto;

import com.paymentnotify.resilience.CircuitState;

public record CircuitBreakerStatusResponse(
        String merchantId,
        CircuitState state,
        double currentFailureRate
) {
}

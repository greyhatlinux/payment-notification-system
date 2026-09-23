package com.paymentnotify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.circuit-breaker")
public record CircuitBreakerProperties(
        double failureRateThreshold,
        int slidingWindowSize,
        int minimumCalls,
        long openStateWaitMs,
        int halfOpenPermittedCalls
) {
}

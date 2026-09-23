package com.paymentnotify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Exponential backoff with jitter, configurable rather than hardcoded.
 * Default policy mirrors the example in AGENTS.md:
 *   attempt 1 ~5s, 2 ~30s, 3 ~2m, 4 ~10m, 5 ~30m, 6 ~2h
 * which falls out of baseDelay=5s, multiplier=6.0, capped at maxDelayMs.
 */
@ConfigurationProperties(prefix = "app.retry")
public record RetryProperties(
        int maxAttempts,
        long baseDelayMs,
        double backoffMultiplier,
        long maxDelayMs,
        double jitterRatio,
        int queueCapacity,
        int pollBatchSize,
        long pollIntervalMs
) {
    public RetryProperties {
        if (queueCapacity <= 0) {
            queueCapacity = 200_000;
        }
        if (pollBatchSize <= 0) {
            pollBatchSize = 200;
        }
        if (pollIntervalMs <= 0) {
            pollIntervalMs = 200;
        }
    }
}

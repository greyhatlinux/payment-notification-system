package com.paymentnotify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Configurable failure classification. Defaults follow AGENTS.md:
 * retryable = timeout, connection failure, 408, 429, 5xx.
 * permanent = 400, 401, 403, 404.
 */
@ConfigurationProperties(prefix = "app.failure-classification")
public record FailureClassificationProperties(
        List<Integer> retryableStatusCodes,
        List<Integer> permanentStatusCodes
) {
    public FailureClassificationProperties {
        if (retryableStatusCodes == null) {
            retryableStatusCodes = List.of(408, 429);
        }
        if (permanentStatusCodes == null) {
            permanentStatusCodes = List.of(400, 401, 403, 404);
        }
    }
}

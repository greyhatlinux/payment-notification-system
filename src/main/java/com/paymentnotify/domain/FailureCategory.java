package com.paymentnotify.domain;

/**
 * How a failed delivery attempt should be treated.
 *
 * RETRYABLE   - transient: timeout, connection failure, 408, 429, 5xx.
 * PERMANENT   - will not be retried: 400, 401, 403, 404 and similar.
 *
 * The mapping from raw outcome (status code / exception) to this category is
 * configurable — see {@code com.paymentnotify.delivery.FailureClassifier}.
 */
public enum FailureCategory {
    RETRYABLE,
    PERMANENT
}

package com.paymentnotify.domain;

import java.time.Instant;

/**
 * A payment notification sitting in the scheduled retry queue.
 *
 * Carries everything a retry worker (C2) needs without going back to Kafka:
 * the original payload, which merchant to call, which attempt this is, and
 * when it becomes eligible to run.
 *
 * Immutable — {@link #withNextAttempt} produces a new instance for
 * rescheduling rather than mutating in place, so the queue never holds two
 * different views of the same logical retry.
 */
public record RetryMessage(
        String eventId,
        String paymentId,
        MerchantId merchantId,
        int attempt,
        Instant nextAttemptAt,
        PaymentEvent payload,
        String lastFailureDetail
) {

    public static RetryMessage firstRetry(PaymentEvent event, Instant nextAttemptAt, String failureDetail) {
        return new RetryMessage(event.eventId(), event.paymentId(), event.merchantId(),
                1, nextAttemptAt, event, failureDetail);
    }

    public RetryMessage withNextAttempt(Instant newNextAttemptAt, String failureDetail) {
        return new RetryMessage(eventId, paymentId, merchantId, attempt + 1, newNextAttemptAt, payload, failureDetail);
    }
}

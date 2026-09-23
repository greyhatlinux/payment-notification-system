package com.paymentnotify.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Immutable payment event as it would arrive from Kafka after S1 has
 * published it. This system starts here — S1 is out of scope.
 *
 * eventId: unique id of this publication (at-least-once delivery means the
 *          same eventId may be seen more than once).
 * paymentId: identifies the underlying payment; used to partition Kafka and
 *          to preserve per-payment ordering.
 */
public record PaymentEvent(
        String eventId,
        String paymentId,
        MerchantId merchantId,
        BigDecimal amount,
        String currency,
        Instant createdAt
) {

    public PaymentEvent {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (paymentId == null || paymentId.isBlank()) {
            throw new IllegalArgumentException("paymentId must not be blank");
        }
        if (merchantId == null) {
            throw new IllegalArgumentException("merchantId must not be null");
        }
        if (amount == null) {
            throw new IllegalArgumentException("amount must not be null");
        }
    }
}

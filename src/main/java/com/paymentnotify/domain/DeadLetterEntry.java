package com.paymentnotify.domain;

import java.time.Instant;

/** A notification that exhausted all retry attempts (or hit a permanent failure). */
public record DeadLetterEntry(
        String eventId,
        String paymentId,
        MerchantId merchantId,
        int attemptsMade,
        String failureReason,
        PaymentEvent payload,
        Instant movedToDlqAt
) {
}

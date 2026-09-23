package com.paymentnotify.domain;

import java.time.Instant;

/**
 * One line of bounded recent history, kept for the dashboard. Not persisted
 * anywhere durable — per AGENTS.md, we deliberately do not write every event
 * to a relational DB, we just keep a capped in-memory ring buffer.
 */
public record NotificationHistoryEntry(
        String eventId,
        String paymentId,
        MerchantId merchantId,
        int attempt,
        DeliverySource source,
        boolean success,
        Integer httpStatus,
        long latencyMs,
        String failureDetail,
        Instant timestamp
) {
}

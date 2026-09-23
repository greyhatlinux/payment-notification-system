package com.paymentnotify.ingestion;

import com.paymentnotify.domain.PaymentEvent;

import java.time.Instant;

/**
 * A single record as it sits in a mock Kafka partition: the payload plus the
 * partition/offset coordinates a real Kafka record would carry.
 */
public record KafkaMessage(
        int partition,
        long offset,
        PaymentEvent event,
        Instant publishedAt
) {
}

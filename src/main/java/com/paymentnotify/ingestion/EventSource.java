package com.paymentnotify.ingestion;

import java.time.Duration;

/**
 * Consumption-side abstraction over "Kafka". S2 depends only on this
 * interface, never on {@link MockKafkaBroker} directly, so a real Kafka
 * consumer adapter can be dropped in later without touching S2.
 *
 * Semantics deliberately mirror a real Kafka consumer:
 *   - one partition is owned by at most one consumer at a time
 *   - polling returns null on timeout rather than blocking forever, so a
 *     consumer loop can stay responsive to shutdown signals
 *   - offsets are committed explicitly, after processing, so consumer lag
 *     (endOffset - committedOffset) reflects real backlog
 */
public interface EventSource {

    int partitionCount();

    /**
     * Poll a specific partition for the next message, waiting up to
     * {@code timeout} if none is immediately available.
     *
     * @return the next message, or {@code null} if none arrived within the timeout
     */
    KafkaMessage poll(int partition, Duration timeout) throws InterruptedException;

    /** Mark {@code offset} on {@code partition} as processed. */
    void commit(int partition, long offset);

    /** Next offset that will be assigned on this partition (i.e. the high-water mark). */
    long endOffset(int partition);

    /** Highest offset committed so far on this partition (-1 if none). */
    long committedOffset(int partition);
}

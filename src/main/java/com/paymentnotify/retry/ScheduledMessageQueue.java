package com.paymentnotify.retry;

import com.paymentnotify.domain.RetryMessage;

import java.util.List;

/**
 * The failure-path buffer: holds notifications waiting for their next retry
 * attempt, ordered by {@code nextAttemptAt}. Kept behind this interface so
 * the in-memory default can later be swapped for a Redis-ZSET-backed
 * implementation (score = nextAttemptAt epoch millis) without touching
 * {@code RetryScheduler} or C2.
 */
public interface ScheduledMessageQueue {

    /**
     * Schedule a message. Returns {@code false} if the queue is at capacity
     * (a bounded safety valve — this never grows without limit); callers
     * should treat that as "route to DLQ instead".
     */
    boolean schedule(RetryMessage message);

    /** Remove and return up to {@code maxBatchSize} messages that are due now. */
    List<RetryMessage> pollDue(int maxBatchSize);

    /** Current number of messages waiting (due or not yet due). */
    int size();

    /** Snapshot of every pending message, for API/UI inspection. Does not remove anything. */
    List<RetryMessage> peekAll();
}

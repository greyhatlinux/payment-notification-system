package com.paymentnotify.retry;

import com.paymentnotify.dlq.DeadLetterSink;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.domain.RetryMessage;
import com.paymentnotify.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * C1's real failure path: schedules a first retry attempt on the shared
 * queue, computing {@code nextAttemptAt} via {@link BackoffPolicy} (attempt
 * 1's delay — subsequent attempts are scheduled by C2 itself, see Phase 7).
 *
 * If the queue is at capacity (bounded — see {@link InMemoryScheduledQueue}),
 * falls back to the DLQ rather than blocking C1 or growing without limit.
 */
@Component
public class ScheduledQueueRetryEnqueuer implements RetryEnqueuer {

    private static final Logger log = LoggerFactory.getLogger(ScheduledQueueRetryEnqueuer.class);

    private final ScheduledMessageQueue queue;
    private final BackoffPolicy backoffPolicy;
    private final DeadLetterSink deadLetterSink;
    private final MetricsRegistry metrics;

    public ScheduledQueueRetryEnqueuer(ScheduledMessageQueue queue,
                                        BackoffPolicy backoffPolicy,
                                        DeadLetterSink deadLetterSink,
                                        MetricsRegistry metrics) {
        this.queue = queue;
        this.backoffPolicy = backoffPolicy;
        this.deadLetterSink = deadLetterSink;
        this.metrics = metrics;
    }

    @Override
    public void enqueue(PaymentEvent event, DeliveryResult failedResult) {
        RetryMessage message = RetryMessage.firstRetry(
                event, backoffPolicy.nextAttemptAt(1), failedResult.failureDetail());

        if (queue.schedule(message)) {
            metrics.recordRetryScheduled();
        } else {
            log.warn("Retry queue at capacity; sending eventId={} straight to DLQ", event.eventId());
            deadLetterSink.send(event, 1, "retry_queue_full");
        }
    }
}

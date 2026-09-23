package com.paymentnotify.retry;

import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.PaymentEvent;

/**
 * The failure path out of C1/C2: a retryable delivery failure is handed
 * here to be scheduled for a future attempt.
 *
 * Implemented by {@code ScheduledQueueRetryEnqueuer} from Phase 6 onward,
 * wired to the {@link ScheduledMessageQueue}. C1DeliveryService depends only
 * on this interface, so it never needs to change when the retry
 * infrastructure is built out.
 */
public interface RetryEnqueuer {

    void enqueue(PaymentEvent event, DeliveryResult failedResult);
}

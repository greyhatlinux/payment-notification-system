package com.paymentnotify.dlq;

import com.paymentnotify.domain.PaymentEvent;

/**
 * Where a notification goes once it can never succeed: either it exhausted
 * all retry attempts, or C1/C2 classified the failure as permanent.
 * Implemented by {@code DeadLetterStore} from Phase 8 onward.
 */
public interface DeadLetterSink {

    void send(PaymentEvent event, int attemptsMade, String reason);
}

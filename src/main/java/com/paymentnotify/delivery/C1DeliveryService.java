package com.paymentnotify.delivery;

import com.paymentnotify.dlq.DeadLetterSink;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.DeliverySource;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.metrics.MetricsRegistry;
import com.paymentnotify.retry.RetryEnqueuer;
import org.springframework.stereotype.Service;

/**
 * C1 — the synchronous, first-attempt delivery path. This is the normal
 * path: Kafka -> S2 -> C1 -> merchant. Called inline on the S2 partition
 * consumer thread so a payment confirmation gets an immediate delivery
 * attempt before anything falls back to async retry.
 *
 * Only failed, retryable deliveries fall through to the retry system —
 * retry is the failure path, not the default one.
 */
@Service
public class C1DeliveryService {

    private final MerchantClient merchantClient;
    private final RetryEnqueuer retryEnqueuer;
    private final DeadLetterSink deadLetterSink;
    private final MetricsRegistry metrics;

    public C1DeliveryService(MerchantClient merchantClient,
                              RetryEnqueuer retryEnqueuer,
                              DeadLetterSink deadLetterSink,
                              MetricsRegistry metrics) {
        this.merchantClient = merchantClient;
        this.retryEnqueuer = retryEnqueuer;
        this.deadLetterSink = deadLetterSink;
        this.metrics = metrics;
    }

    public DeliveryResult deliver(PaymentEvent event) {
        metrics.recordDeliveryStart(event.merchantId());
        DeliveryResult result = merchantClient.deliver(event);
        metrics.recordDelivery(event, DeliverySource.C1, 1, result);

        if (result.success()) {
            return result;
        }
        if (result.isRetryable()) {
            // The enqueuer records the retry-scheduled metric itself, since it
            // knows the true outcome (e.g. it may fall back to the DLQ if the
            // retry queue is at capacity).
            retryEnqueuer.enqueue(event, result);
        } else {
            deadLetterSink.send(event, 1, result.failureDetail());
        }
        return result;
    }
}

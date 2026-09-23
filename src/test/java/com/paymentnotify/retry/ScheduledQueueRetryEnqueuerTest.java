package com.paymentnotify.retry;

import com.paymentnotify.config.RetryProperties;
import com.paymentnotify.dlq.DeadLetterSink;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.FailureCategory;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.metrics.MetricsRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ScheduledQueueRetryEnqueuerTest {

    private final PaymentEvent event = new PaymentEvent("evt-1", "pay-1", MerchantId.of("amazon"),
            BigDecimal.TEN, "USD", Instant.now());
    private final DeliveryResult failure = DeliveryResult.failure(503, 10, FailureCategory.RETRYABLE, "HTTP 503");

    @Test
    void enqueuesToQueueOnSuccess() {
        RetryProperties props = new RetryProperties(6, 5000, 4.5, 7_200_000, 0.2, 100, 10, 100);
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(props);
        DeadLetterSink dlq = mock(DeadLetterSink.class);
        MetricsRegistry metrics = new MetricsRegistry();
        ScheduledQueueRetryEnqueuer enqueuer = new ScheduledQueueRetryEnqueuer(
                queue, new BackoffPolicy(props), dlq, metrics);

        enqueuer.enqueue(event, failure);

        assertThat(queue.size()).isEqualTo(1);
        assertThat(queue.peekAll().get(0).attempt()).isEqualTo(1);
        verify(dlq, times(0)).send(event, 1, "retry_queue_full");
    }

    @Test
    void fallsBackToDlqWhenQueueIsFull() {
        RetryProperties props = new RetryProperties(6, 5000, 4.5, 7_200_000, 0.2, 1, 10, 100);
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(props); // capacity 1
        DeadLetterSink dlq = mock(DeadLetterSink.class);
        MetricsRegistry metrics = new MetricsRegistry();
        ScheduledQueueRetryEnqueuer enqueuer = new ScheduledQueueRetryEnqueuer(
                queue, new BackoffPolicy(props), dlq, metrics);

        // Fill the single slot directly, then try to enqueue via the real path.
        PaymentEvent filler = new PaymentEvent("evt-filler", "pay-filler", MerchantId.of("amazon"),
                BigDecimal.ONE, "USD", Instant.now());
        queue.schedule(com.paymentnotify.domain.RetryMessage.firstRetry(filler, Instant.now().plusSeconds(60), "fail"));

        enqueuer.enqueue(event, failure);

        assertThat(queue.size()).isEqualTo(1);
        verify(dlq, times(1)).send(event, 1, "retry_queue_full");
    }
}

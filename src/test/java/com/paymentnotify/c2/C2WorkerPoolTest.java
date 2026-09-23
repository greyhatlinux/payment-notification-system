package com.paymentnotify.c2;

import com.paymentnotify.config.ConcurrencyProperties;
import com.paymentnotify.config.RetryProperties;
import com.paymentnotify.delivery.MerchantClient;
import com.paymentnotify.dlq.DeadLetterSink;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.FailureCategory;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.domain.RetryMessage;
import com.paymentnotify.metrics.MetricsRegistry;
import com.paymentnotify.retry.BackoffPolicy;
import com.paymentnotify.retry.InMemoryScheduledQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class C2WorkerPoolTest {

    private final PaymentEvent event = new PaymentEvent("evt-1", "pay-1", MerchantId.of("amazon"),
            BigDecimal.TEN, "USD", Instant.now());

    private C2WorkerPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.stop();
        }
    }

    private RetryProperties defaultProps(int maxAttempts) {
        return new RetryProperties(maxAttempts, 5000, 4.5, 7_200_000, 0.2, 1000, 50, 100);
    }

    @Test
    void successfulRetryIsNotRescheduled() {
        RetryProperties props = defaultProps(6);
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(props);
        MerchantClient client = e -> DeliveryResult.success(200, 5);
        DeadLetterSink dlq = mock(DeadLetterSink.class);
        MetricsRegistry metrics = new MetricsRegistry();
        pool = new C2WorkerPool(client, queue, new BackoffPolicy(props), dlq, metrics, new ConcurrencyProperties(1, 4, 10));

        RetryMessage message = RetryMessage.firstRetry(event, Instant.now(), "prior failure");
        pool.dispatch(message);

        await().atMost(Duration.ofSeconds(2)).until(() -> metrics.deliveredRate.total() == 1);
        assertThat(queue.size()).isZero();
    }

    @Test
    void retryableFailureReschedulesSameMessageWithIncrementedAttempt() {
        RetryProperties props = defaultProps(6);
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(props);
        MerchantClient client = e -> DeliveryResult.failure(503, 5, FailureCategory.RETRYABLE, "HTTP 503");
        DeadLetterSink dlq = mock(DeadLetterSink.class);
        MetricsRegistry metrics = new MetricsRegistry();
        pool = new C2WorkerPool(client, queue, new BackoffPolicy(props), dlq, metrics, new ConcurrencyProperties(1, 4, 10));

        RetryMessage message = RetryMessage.firstRetry(event, Instant.now(), "prior failure");
        pool.dispatch(message);

        await().atMost(Duration.ofSeconds(2)).until(() -> queue.size() == 1);
        RetryMessage rescheduled = queue.peekAll().get(0);
        assertThat(rescheduled.eventId()).isEqualTo(event.eventId());
        assertThat(rescheduled.attempt()).isEqualTo(2);
        assertThat(rescheduled.nextAttemptAt()).isAfter(Instant.now());
    }

    @Test
    void exhaustingMaxAttemptsRoutesToDlq() {
        RetryProperties props = defaultProps(2); // small max for a fast test
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(props);
        MerchantClient client = e -> DeliveryResult.failure(503, 5, FailureCategory.RETRYABLE, "HTTP 503");
        DeadLetterSink dlq = mock(DeadLetterSink.class);
        MetricsRegistry metrics = new MetricsRegistry();
        pool = new C2WorkerPool(client, queue, new BackoffPolicy(props), dlq, metrics, new ConcurrencyProperties(1, 4, 10));

        // attempt=2 is already the last permitted retry attempt (maxAttempts=2); a failure here should go to DLQ.
        RetryMessage lastAttemptMessage = new RetryMessage(event.eventId(), event.paymentId(), event.merchantId(),
                2, Instant.now(), event, "prior failure");
        pool.dispatch(lastAttemptMessage);

        await().atMost(Duration.ofSeconds(2)).until(() -> queue.size() == 0);
        verify(dlq, times(1)).send(event, 3, "max_attempts_exhausted: HTTP 503");
    }

    @Test
    void permanentFailureGoesStraightToDlqWithoutRescheduling() {
        RetryProperties props = defaultProps(6);
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(props);
        MerchantClient client = e -> DeliveryResult.failure(404, 5, FailureCategory.PERMANENT, "HTTP 404");
        DeadLetterSink dlq = mock(DeadLetterSink.class);
        MetricsRegistry metrics = new MetricsRegistry();
        pool = new C2WorkerPool(client, queue, new BackoffPolicy(props), dlq, metrics, new ConcurrencyProperties(1, 4, 10));

        RetryMessage message = RetryMessage.firstRetry(event, Instant.now(), "prior failure");
        pool.dispatch(message);

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                verify(dlq, times(1)).send(event, 2, "HTTP 404"));
        assertThat(queue.size()).isZero();
    }

    @Test
    void workerConcurrencyIsBounded() throws Exception {
        RetryProperties props = defaultProps(6);
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(props);
        int poolSize = 3;
        CountDownLatch release = new CountDownLatch(1);
        MerchantClient client = e -> {
            try {
                release.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return DeliveryResult.success(200, 1);
        };
        DeadLetterSink dlq = mock(DeadLetterSink.class);
        MetricsRegistry metrics = new MetricsRegistry();
        pool = new C2WorkerPool(client, queue, new BackoffPolicy(props), dlq, metrics,
                new ConcurrencyProperties(1, poolSize, 100));

        for (int i = 0; i < 10; i++) {
            PaymentEvent e = new PaymentEvent("evt-" + i, "pay-" + i, MerchantId.of("amazon"),
                    BigDecimal.ONE, "USD", Instant.now());
            pool.dispatch(RetryMessage.firstRetry(e, Instant.now(), "fail"));
        }

        await().atMost(Duration.ofSeconds(2)).until(() -> pool.activeWorkers() == poolSize);
        assertThat(pool.activeWorkers()).isEqualTo(poolSize);

        release.countDown();
    }
}

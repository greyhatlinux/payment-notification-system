package com.paymentnotify.c2;

import com.paymentnotify.config.ConcurrencyProperties;
import com.paymentnotify.delivery.MerchantClient;
import com.paymentnotify.dlq.DeadLetterSink;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.DeliverySource;
import com.paymentnotify.domain.RetryMessage;
import com.paymentnotify.metrics.MetricsRegistry;
import com.paymentnotify.retry.BackoffPolicy;
import com.paymentnotify.retry.RetryDispatcher;
import com.paymentnotify.retry.ScheduledMessageQueue;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * C2 — the retry worker pool. Receives due messages from {@link
 * com.paymentnotify.retry.RetryScheduler} and calls the same {@link
 * MerchantClient} (and therefore the same per-merchant circuit breaker) as
 * C1.
 *
 * Retry-to-same-queue behavior: a retryable C2 failure is rescheduled back
 * onto the same {@link ScheduledMessageQueue} with an incremented attempt
 * count and the next backoff delay, exactly like C1's original failure —
 * there is no separate "C2 queue". A success needs no further action (the
 * message was already removed from the queue when it was polled as due). A
 * permanent failure, or exhausting {@code maxAttempts}, routes to the DLQ.
 *
 * Bounded by design: a fixed-size thread pool and a bounded work queue. If
 * the pool is saturated, the message is simply put back on the scheduled
 * queue a moment later rather than blocking the scheduler thread or
 * growing an unbounded backlog — this does not count as a failed delivery
 * attempt.
 */
@Component
public class C2WorkerPool implements RetryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(C2WorkerPool.class);
    private static final long SATURATION_REQUEUE_DELAY_MS = 500;

    private final MerchantClient merchantClient;
    private final ScheduledMessageQueue queue;
    private final BackoffPolicy backoffPolicy;
    private final DeadLetterSink deadLetterSink;
    private final MetricsRegistry metrics;
    private final ThreadPoolExecutor executor;
    private final AtomicInteger activeWorkers = new AtomicInteger(0);

    public C2WorkerPool(MerchantClient merchantClient,
                         ScheduledMessageQueue queue,
                         BackoffPolicy backoffPolicy,
                         DeadLetterSink deadLetterSink,
                         MetricsRegistry metrics,
                         ConcurrencyProperties concurrencyProperties) {
        this.merchantClient = merchantClient;
        this.queue = queue;
        this.backoffPolicy = backoffPolicy;
        this.deadLetterSink = deadLetterSink;
        this.metrics = metrics;

        int poolSize = Math.max(concurrencyProperties.c2WorkerPoolSize(), 1);
        this.executor = new ThreadPoolExecutor(
                poolSize, poolSize, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(poolSize * 4),
                runnable -> {
                    Thread t = new Thread(runnable, "c2-worker");
                    t.setDaemon(true);
                    return t;
                });
        log.info("C2WorkerPool started: {} workers", poolSize);
    }

    @PreDestroy
    public void stop() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("C2WorkerPool stopped");
    }

    public int activeWorkers() {
        return activeWorkers.get();
    }

    @Override
    public void dispatch(RetryMessage message) {
        try {
            executor.execute(() -> process(message));
        } catch (RejectedExecutionException e) {
            requeueAfterSaturation(message);
        }
    }

    private void process(RetryMessage message) {
        activeWorkers.incrementAndGet();
        try {
            DeliveryResult result = merchantClient.deliver(message.payload());
            metrics.recordDelivery(message.payload(), DeliverySource.C2, message.attempt(), result);

            if (result.success()) {
                return;
            }
            if (!result.isRetryable()) {
                deadLetterSink.send(message.payload(), message.attempt() + 1, result.failureDetail());
                return;
            }
            if (message.attempt() >= backoffPolicy.maxAttempts()) {
                deadLetterSink.send(message.payload(), message.attempt() + 1,
                        "max_attempts_exhausted: " + result.failureDetail());
                return;
            }

            RetryMessage rescheduled = message.withNextAttempt(
                    backoffPolicy.nextAttemptAt(message.attempt() + 1), result.failureDetail());
            if (queue.schedule(rescheduled)) {
                metrics.recordRetryScheduled();
            } else {
                deadLetterSink.send(message.payload(), message.attempt() + 1, "retry_queue_full");
            }
        } finally {
            activeWorkers.decrementAndGet();
        }
    }

    private void requeueAfterSaturation(RetryMessage message) {
        RetryMessage requeued = new RetryMessage(
                message.eventId(), message.paymentId(), message.merchantId(),
                message.attempt(), Instant.now().plusMillis(SATURATION_REQUEUE_DELAY_MS),
                message.payload(), message.lastFailureDetail());
        if (!queue.schedule(requeued)) {
            deadLetterSink.send(message.payload(), message.attempt() + 1, "retry_queue_full_after_worker_saturation");
        }
    }
}

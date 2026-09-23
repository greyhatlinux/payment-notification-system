package com.paymentnotify.retry;

import com.paymentnotify.config.RetryProperties;
import com.paymentnotify.domain.RetryMessage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The retry scheduler: a single dedicated thread that periodically polls
 * {@link ScheduledMessageQueue} for due messages and hands each one to
 * {@link RetryDispatcher} (the C2 worker pool). One thread, a small bounded
 * poll batch — this component never grows threads or queues.
 */
@Component
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final ScheduledMessageQueue queue;
    private final RetryDispatcher dispatcher;
    private final int pollBatchSize;
    private final long pollIntervalMs;

    private ScheduledExecutorService executor;

    public RetryScheduler(ScheduledMessageQueue queue, RetryDispatcher dispatcher, RetryProperties props) {
        this.queue = queue;
        this.dispatcher = dispatcher;
        this.pollBatchSize = props.pollBatchSize();
        this.pollIntervalMs = props.pollIntervalMs();
    }

    @PostConstruct
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable, "retry-scheduler");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::pollAndDispatch, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
        log.info("RetryScheduler started: polling every {}ms, batch size {}", pollIntervalMs, pollBatchSize);
    }

    @PreDestroy
    public void stop() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("RetryScheduler stopped");
    }

    private void pollAndDispatch() {
        try {
            List<RetryMessage> due = queue.pollDue(pollBatchSize);
            for (RetryMessage message : due) {
                dispatcher.dispatch(message);
            }
        } catch (Exception e) {
            // Never let one bad batch kill the scheduler's recurring task.
            log.error("RetryScheduler poll/dispatch failed", e);
        }
    }
}

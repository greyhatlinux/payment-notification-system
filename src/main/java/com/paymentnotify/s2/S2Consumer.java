package com.paymentnotify.s2;

import com.paymentnotify.config.ConcurrencyProperties;
import com.paymentnotify.delivery.C1DeliveryService;
import com.paymentnotify.ingestion.EventSource;
import com.paymentnotify.ingestion.KafkaMessage;
import com.paymentnotify.metrics.MetricsRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * S2 — the Kafka consumer. Runs one dedicated loop per partition (a bounded,
 * fixed-size pool, never one thread per message), reading in offset order
 * and calling C1 synchronously before committing.
 *
 * Because each partition has its own thread, a slow/unhealthy merchant only
 * ever stalls the partition(s) carrying its traffic — other partitions (and
 * the merchants whose events land there) keep flowing. Within an affected
 * partition, the per-call HTTP timeout and (from Phase 5) the circuit
 * breaker bound how long a bad merchant can hold up that partition's
 * consumer thread.
 */
@Component
public class S2Consumer {

    private static final Logger log = LoggerFactory.getLogger(S2Consumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    private final EventSource eventSource;
    private final C1DeliveryService c1DeliveryService;
    private final MetricsRegistry metrics;
    private final int threadsPerPartition;

    private ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public S2Consumer(EventSource eventSource,
                       C1DeliveryService c1DeliveryService,
                       MetricsRegistry metrics,
                       ConcurrencyProperties concurrencyProperties) {
        this.eventSource = eventSource;
        this.c1DeliveryService = c1DeliveryService;
        this.metrics = metrics;
        this.threadsPerPartition = Math.max(1, concurrencyProperties.s2ConsumerThreadsPerPartition());
    }

    @PostConstruct
    public void start() {
        int partitionCount = eventSource.partitionCount();
        int totalThreads = partitionCount * threadsPerPartition;
        executor = Executors.newFixedThreadPool(totalThreads, new S2ThreadFactory());
        running.set(true);

        for (int p = 0; p < partitionCount; p++) {
            int partition = p;
            for (int replica = 0; replica < threadsPerPartition; replica++) {
                executor.submit(() -> consumeLoop(partition));
            }
        }
        log.info("S2Consumer started: {} partitions, {} thread(s) each ({} total)",
                partitionCount, threadsPerPartition, totalThreads);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
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
        log.info("S2Consumer stopped");
    }

    private void consumeLoop(int partition) {
        while (running.get()) {
            try {
                KafkaMessage message = eventSource.poll(partition, POLL_TIMEOUT);
                if (message == null) {
                    continue;
                }
                metrics.recordProcessed();
                c1DeliveryService.deliver(message.event());
                eventSource.commit(partition, message.offset());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // A single bad message/merchant must not kill this partition's consumer loop.
                log.error("S2 consumer error on partition {}", partition, e);
            }
        }
    }

    private static final class S2ThreadFactory implements java.util.concurrent.ThreadFactory {
        private int counter = 0;

        @Override
        public synchronized Thread newThread(Runnable r) {
            Thread t = new Thread(r, "s2-consumer-" + (counter++));
            t.setDaemon(true);
            return t;
        }
    }
}

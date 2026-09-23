package com.paymentnotify.ingestion;

import com.paymentnotify.config.KafkaProperties;
import com.paymentnotify.domain.PaymentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Behavioral emulator of a Kafka topic — not a protocol implementation.
 * Models the semantics that matter for this system:
 *
 *   - a fixed number of partitions, each an ordered, bounded buffer
 *   - paymentId -> partition hashing, so per-payment ordering is preserved
 *   - monotonically increasing per-partition offsets
 *   - explicit offset commit (consumer lag = endOffset - committedOffset)
 *   - backpressure: each partition is a bounded queue. If S2 falls behind,
 *     {@link #publish} blocks (retrying a bounded offer) rather than
 *     growing without limit, exactly the way a real producer backs off
 *     when a broker/partition is saturated.
 *
 * This class owns both "production" (used by the traffic generator, since
 * in this demo S1 is out of scope and something has to originate events)
 * and "storage". {@link MockKafkaEventSource} exposes only the consumption
 * side ({@link EventSource}) to S2, mirroring how a real deployment would
 * separate "who writes to Kafka" from "how S2 reads from it".
 */
@Component
public class MockKafkaBroker {

    private static final Logger log = LoggerFactory.getLogger(MockKafkaBroker.class);

    private final int partitionCount;
    private final BlockingQueue<KafkaMessage>[] partitions;
    private final AtomicLongArray nextOffset;
    private final AtomicLongArray committedOffset;
    private final AtomicLong totalPublished = new AtomicLong();
    private final AtomicLong backpressureStalls = new AtomicLong();

    @SuppressWarnings("unchecked")
    public MockKafkaBroker(KafkaProperties props) {
        this.partitionCount = props.partitionCount();
        this.partitions = new BlockingQueue[partitionCount];
        this.nextOffset = new AtomicLongArray(partitionCount);
        this.committedOffset = new AtomicLongArray(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitions[i] = new ArrayBlockingQueue<>(props.partitionBufferCapacity());
            committedOffset.set(i, -1);
        }
        log.info("MockKafkaBroker started: {} partitions, {} capacity each",
                partitionCount, props.partitionBufferCapacity());
    }

    public int partitionCount() {
        return partitionCount;
    }

    /**
     * Publish an event, partitioning on paymentId. Blocks (with bounded
     * retries, not a busy spin) while the target partition is full — this
     * is the system's backpressure mechanism, and is deliberately visible
     * as reduced achieved throughput rather than an unbounded buffer.
     */
    public KafkaMessage publish(PaymentEvent event) throws InterruptedException {
        int partition = Partitioner.partitionFor(event.paymentId(), partitionCount);
        long offset = nextOffset.getAndIncrement(partition);
        KafkaMessage message = new KafkaMessage(partition, offset, event, Instant.now());

        while (!partitions[partition].offer(message, 20, TimeUnit.MILLISECONDS)) {
            backpressureStalls.incrementAndGet();
        }
        totalPublished.incrementAndGet();
        return message;
    }

    KafkaMessage poll(int partition, Duration timeout) throws InterruptedException {
        return partitions[partition].poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    void commit(int partition, long offset) {
        committedOffset.updateAndGet(partition, current -> Math.max(current, offset));
    }

    long endOffset(int partition) {
        return nextOffset.get(partition);
    }

    long committedOffset(int partition) {
        return committedOffset.get(partition);
    }

    public int queueDepth(int partition) {
        return partitions[partition].size();
    }

    public int partitionCapacity() {
        return partitions.length == 0 ? 0 : ((ArrayBlockingQueue<KafkaMessage>) partitions[0]).remainingCapacity()
                + partitions[0].size();
    }

    public long totalPublished() {
        return totalPublished.get();
    }

    public long backpressureStalls() {
        return backpressureStalls.get();
    }
}

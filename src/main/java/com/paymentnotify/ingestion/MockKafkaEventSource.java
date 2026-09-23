package com.paymentnotify.ingestion;

import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * The default, no-external-infrastructure {@link EventSource}. Delegates
 * storage/ordering/offsets to {@link MockKafkaBroker}. A future
 * {@code KafkaEventSource} (wrapping a real {@code KafkaConsumer}) would
 * implement the same interface, so S2 never needs to change.
 */
@Component
public class MockKafkaEventSource implements EventSource {

    private final MockKafkaBroker broker;

    public MockKafkaEventSource(MockKafkaBroker broker) {
        this.broker = broker;
    }

    @Override
    public int partitionCount() {
        return broker.partitionCount();
    }

    @Override
    public KafkaMessage poll(int partition, Duration timeout) throws InterruptedException {
        return broker.poll(partition, timeout);
    }

    @Override
    public void commit(int partition, long offset) {
        broker.commit(partition, offset);
    }

    @Override
    public long endOffset(int partition) {
        return broker.endOffset(partition);
    }

    @Override
    public long committedOffset(int partition) {
        return broker.committedOffset(partition);
    }
}

package com.paymentnotify.ingestion;

import com.paymentnotify.config.KafkaProperties;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class MockKafkaBrokerTest {

    private PaymentEvent eventFor(String paymentId) {
        return new PaymentEvent("evt-" + UUID.randomUUID(), paymentId, MerchantId.of("amazon"),
                BigDecimal.TEN, "USD", Instant.now());
    }

    @Test
    void sameSamePaymentIdAlwaysMapsToSamePartition() {
        String paymentId = "pay-fixed-123";
        int expectedPartition = Partitioner.partitionFor(paymentId, 8);
        for (int i = 0; i < 20; i++) {
            assertThat(Partitioner.partitionFor(paymentId, 8)).isEqualTo(expectedPartition);
        }
    }

    @Test
    void eventsForSamePaymentArePreservedInPublishOrderOnOnePartition() throws Exception {
        KafkaProperties props = new KafkaProperties(4, 100);
        MockKafkaBroker broker = new MockKafkaBroker(props);
        String paymentId = "pay-order-test";
        int partition = Partitioner.partitionFor(paymentId, 4);

        for (int i = 0; i < 10; i++) {
            broker.publish(new PaymentEvent("evt-" + i, paymentId, MerchantId.of("amazon"),
                    BigDecimal.valueOf(i), "USD", Instant.now()));
        }

        for (int i = 0; i < 10; i++) {
            KafkaMessage msg = broker.poll(partition, Duration.ofSeconds(1));
            assertThat(msg).isNotNull();
            assertThat(msg.event().eventId()).isEqualTo("evt-" + i);
            assertThat(msg.offset()).isEqualTo(i);
        }
    }

    @Test
    void offsetsAreMonotonicPerPartition() throws Exception {
        KafkaProperties props = new KafkaProperties(1, 1000);
        MockKafkaBroker broker = new MockKafkaBroker(props);

        for (int i = 0; i < 50; i++) {
            KafkaMessage msg = broker.publish(eventFor("pay-" + i));
            assertThat(msg.partition()).isEqualTo(0);
            assertThat(msg.offset()).isEqualTo(i);
        }
        assertThat(broker.endOffset(0)).isEqualTo(50);
    }

    @Test
    void publishBlocksWhenPartitionIsFullAndResumesOnceDrained() throws Exception {
        KafkaProperties props = new KafkaProperties(1, 2);
        MockKafkaBroker broker = new MockKafkaBroker(props);

        CountDownLatch done = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < 5; i++) {
                    broker.publish(eventFor("pay-" + i));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        });
        producer.start();

        // Buffer capacity is 2: producer should stall well before publishing all 5.
        await().atMost(Duration.ofSeconds(2)).until(() -> broker.backpressureStalls() > 0);
        assertThat(broker.totalPublished()).isLessThan(5);
        assertThat(broker.queueDepth(0)).isEqualTo(2);

        // Drain the partition; producer should then be able to finish.
        broker.poll(0, Duration.ofSeconds(1));
        broker.poll(0, Duration.ofSeconds(1));
        broker.poll(0, Duration.ofSeconds(1));
        broker.poll(0, Duration.ofSeconds(1));
        broker.poll(0, Duration.ofSeconds(1));

        assertThat(done.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(broker.totalPublished()).isEqualTo(5);
    }

    @Test
    void commitAdvancesCommittedOffsetAndLagReflectsBacklog() throws Exception {
        KafkaProperties props = new KafkaProperties(1, 100);
        MockKafkaBroker broker = new MockKafkaBroker(props);

        for (int i = 0; i < 5; i++) {
            broker.publish(eventFor("pay-" + i));
        }
        assertThat(broker.committedOffset(0)).isEqualTo(-1);
        assertThat(broker.endOffset(0) - (broker.committedOffset(0) + 1)).isEqualTo(5);

        broker.commit(0, 2);
        assertThat(broker.committedOffset(0)).isEqualTo(2);
        assertThat(broker.endOffset(0) - (broker.committedOffset(0) + 1)).isEqualTo(2);
    }
}

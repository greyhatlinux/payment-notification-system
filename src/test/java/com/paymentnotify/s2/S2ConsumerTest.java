package com.paymentnotify.s2;

import com.paymentnotify.config.ConcurrencyProperties;
import com.paymentnotify.config.KafkaProperties;
import com.paymentnotify.delivery.C1DeliveryService;
import com.paymentnotify.delivery.MerchantClient;
import com.paymentnotify.dlq.DeadLetterStore;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.ingestion.MockKafkaBroker;
import com.paymentnotify.ingestion.MockKafkaEventSource;
import com.paymentnotify.ingestion.Partitioner;
import com.paymentnotify.metrics.MetricsRegistry;
import com.paymentnotify.retry.RetryEnqueuer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class S2ConsumerTest {

    private S2Consumer consumer;

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.stop();
        }
    }

    @Test
    void preservesPerPaymentOrderingThroughC1() throws Exception {
        KafkaProperties kafkaProps = new KafkaProperties(4, 1000);
        MockKafkaBroker broker = new MockKafkaBroker(kafkaProps);
        MockKafkaEventSource eventSource = new MockKafkaEventSource(broker);

        List<String> observedOrder = new CopyOnWriteArrayList<>();
        MerchantClient alwaysSucceeds = event -> {
            observedOrder.add(event.eventId());
            return DeliveryResult.success(200, 1);
        };

        MetricsRegistry metrics = new MetricsRegistry();
        RetryEnqueuer noOpRetryEnqueuer = (event, failedResult) -> { };
        C1DeliveryService c1 = new C1DeliveryService(alwaysSucceeds, noOpRetryEnqueuer,
                new DeadLetterStore(metrics), metrics);
        consumer = new S2Consumer(eventSource, c1, metrics, new ConcurrencyProperties(1, 32, 10));
        consumer.start();

        String paymentId = "pay-order-fixed";
        int expectedCount = 25;
        for (int i = 0; i < expectedCount; i++) {
            broker.publish(new PaymentEvent("evt-" + i, paymentId, MerchantId.of("amazon"),
                    BigDecimal.valueOf(i), "USD", Instant.now()));
        }

        await().atMost(Duration.ofSeconds(5)).until(() -> observedOrder.size() == expectedCount);

        List<String> expected = java.util.stream.IntStream.range(0, expectedCount)
                .mapToObj(i -> "evt-" + i).toList();
        assertThat(observedOrder).containsExactlyElementsOf(expected);
    }

    @Test
    void slowMerchantOnOnePartitionDoesNotBlockOtherPartitions() throws Exception {
        KafkaProperties kafkaProps = new KafkaProperties(2, 1000);
        MockKafkaBroker broker = new MockKafkaBroker(kafkaProps);
        MockKafkaEventSource eventSource = new MockKafkaEventSource(broker);

        CountDownLatch releaseSlowMerchant = new CountDownLatch(1);
        CopyOnWriteArrayList<String> fastDelivered = new CopyOnWriteArrayList<>();

        MerchantClient slowThenFast = event -> {
            if (event.merchantId().value().equals("slow-merchant")) {
                try {
                    releaseSlowMerchant.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return DeliveryResult.success(200, 1);
            }
            fastDelivered.add(event.eventId());
            return DeliveryResult.success(200, 1);
        };

        MetricsRegistry metrics = new MetricsRegistry();
        RetryEnqueuer noOpRetryEnqueuer = (event, failedResult) -> { };
        C1DeliveryService c1 = new C1DeliveryService(slowThenFast, noOpRetryEnqueuer,
                new DeadLetterStore(metrics), metrics);
        consumer = new S2Consumer(eventSource, c1, metrics, new ConcurrencyProperties(1, 32, 10));
        consumer.start();

        String slowPaymentId = findPaymentIdForPartition(0, 2);
        String fastPaymentId = findPaymentIdForPartition(1, 2);

        broker.publish(new PaymentEvent("evt-slow", slowPaymentId, MerchantId.of("slow-merchant"),
                BigDecimal.ONE, "USD", Instant.now()));
        broker.publish(new PaymentEvent("evt-fast", fastPaymentId, MerchantId.of("fast-merchant"),
                BigDecimal.ONE, "USD", Instant.now()));

        // The fast partition should be processed quickly even though the other
        // partition's consumer thread is stuck waiting on the slow merchant.
        await().atMost(Duration.ofSeconds(2)).until(() -> fastDelivered.contains("evt-fast"));

        releaseSlowMerchant.countDown();
    }

    private static String findPaymentIdForPartition(int targetPartition, int partitionCount) {
        for (int i = 0; i < 10_000; i++) {
            String candidate = "pay-" + i;
            if (Partitioner.partitionFor(candidate, partitionCount) == targetPartition) {
                return candidate;
            }
        }
        throw new IllegalStateException("could not find a paymentId for partition " + targetPartition);
    }
}

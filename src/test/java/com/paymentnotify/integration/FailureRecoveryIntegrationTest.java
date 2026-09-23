package com.paymentnotify.integration;

import com.paymentnotify.dlq.DeadLetterStore;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.ingestion.MockKafkaBroker;
import com.paymentnotify.merchant.FailureInjectionConfig;
import com.paymentnotify.merchant.FailureMode;
import com.paymentnotify.merchant.MerchantSimulatorConfigService;
import com.paymentnotify.resilience.CircuitState;
import com.paymentnotify.resilience.MerchantGuardRegistry;
import com.paymentnotify.retry.ScheduledMessageQueue;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Full pipeline, real components, no mocks: Mock Kafka -> S2 -> C1 -> merchant
 * simulator -> circuit breaker -> scheduled retry queue -> RetryScheduler ->
 * C2 -> merchant simulator -> recovery. Exercises the exact demonstration
 * flow described in AGENTS.md's "Execution Instructions".
 *
 * Circuit breaker / retry timing is overridden to be fast so the test
 * doesn't wait on production-scale delays (minutes/hours); the merchant
 * simulator runs on its own port so it doesn't clash with other
 * Spring-context-backed tests in the same JVM.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(properties = {
        "app.merchant-simulator.port=8199",
        "app.circuit-breaker.sliding-window-size=4",
        "app.circuit-breaker.minimum-calls=2",
        "app.circuit-breaker.failure-rate-threshold=0.5",
        "app.circuit-breaker.open-state-wait-ms=200",
        "app.circuit-breaker.half-open-permitted-calls=1",
        "app.retry.base-delay-ms=100",
        "app.retry.backoff-multiplier=2.0",
        "app.retry.max-delay-ms=1000",
        "app.retry.jitter-ratio=0.1",
        "app.retry.poll-interval-ms=20",
        "app.retry.max-attempts=3",
})
class FailureRecoveryIntegrationTest {

    @Autowired
    private MockKafkaBroker broker;
    @Autowired
    private MerchantSimulatorConfigService failureInjection;
    @Autowired
    private MerchantGuardRegistry guardRegistry;
    @Autowired
    private ScheduledMessageQueue retryQueue;
    @Autowired
    private DeadLetterStore deadLetterStore;

    private PaymentEvent eventFor(MerchantId merchant, String suffix) {
        return new PaymentEvent("evt-" + suffix, "pay-" + suffix, merchant, BigDecimal.TEN, "USD", Instant.now());
    }

    @Test
    void merchantFailureOpensCircuitThenRecoversAndDrainsRetryQueue() throws Exception {
        MerchantId merchant = MerchantId.of("integ-recover");
        failureInjection.updateConfig(merchant, new FailureInjectionConfig(FailureMode.HTTP_503, 100, 1, 400));

        broker.publish(eventFor(merchant, "recover-1"));

        // C1 fails synchronously, circuit trips open, and the failure lands on the retry queue.
        await().atMost(Duration.ofSeconds(3)).until(() ->
                guardRegistry.forMerchant(merchant).circuitBreaker().state() == CircuitState.OPEN);
        await().atMost(Duration.ofSeconds(3)).until(() ->
                retryQueue.peekAll().stream().anyMatch(m -> m.merchantId().equals(merchant)));

        // Merchant recovers.
        failureInjection.updateConfig(merchant, FailureInjectionConfig.HEALTHY_DEFAULT);

        // Circuit breaker: OPEN -> HALF_OPEN -> CLOSED once retries succeed.
        await().atMost(Duration.ofSeconds(5)).until(() ->
                guardRegistry.forMerchant(merchant).circuitBreaker().state() == CircuitState.CLOSED);

        // The retried message eventually succeeds and drains off the queue; nothing for
        // this merchant should ever have reached the DLQ (it recovered within max-attempts=3).
        await().atMost(Duration.ofSeconds(5)).until(() ->
                retryQueue.peekAll().stream().noneMatch(m -> m.merchantId().equals(merchant)));
        assertThat(deadLetterStore.forMerchant(merchant)).isEmpty();
    }

    @Test
    void permanentlyFailingMerchantExhaustsRetriesAndLandsInDlq() throws Exception {
        MerchantId merchant = MerchantId.of("integ-permadown");
        // Stays down for the whole test: every call — including half-open trials — fails.
        failureInjection.updateConfig(merchant, new FailureInjectionConfig(FailureMode.HTTP_503, 100, 1, 400));

        broker.publish(eventFor(merchant, "dlq-1"));

        await().atMost(Duration.ofSeconds(10)).until(() ->
                !deadLetterStore.forMerchant(merchant).isEmpty());

        assertThat(retryQueue.peekAll()).noneMatch(m -> m.merchantId().equals(merchant));
        var dlqEntry = deadLetterStore.forMerchant(merchant).get(0);
        assertThat(dlqEntry.eventId()).isEqualTo("evt-dlq-1");
        // max-attempts=3 retries + the original C1 attempt.
        assertThat(dlqEntry.attemptsMade()).isEqualTo(4);
    }
}

package com.paymentnotify.delivery;

import com.paymentnotify.config.CircuitBreakerProperties;
import com.paymentnotify.config.ConcurrencyProperties;
import com.paymentnotify.config.MerchantsProperties;
import com.paymentnotify.config.RateLimitProperties;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.FailureCategory;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.merchant.MerchantRegistry;
import com.paymentnotify.resilience.MerchantGuardRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ResilientMerchantClientTest {

    private PaymentEvent eventFor(String merchant) {
        return new PaymentEvent("evt-1", "pay-1", MerchantId.of(merchant), BigDecimal.TEN, "USD", Instant.now());
    }

    @Test
    void openCircuitStopsHttpCallsButOtherMerchantIsUnaffected() {
        MerchantRegistry registry = new MerchantRegistry(new MerchantsProperties(List.of("bad-merchant", "good-merchant")));
        MerchantGuardRegistry guardRegistry = new MerchantGuardRegistry(
                registry,
                new CircuitBreakerProperties(0.5, 10, 2, 60_000, 2),
                new RateLimitProperties(1000),
                new ConcurrencyProperties(1, 32, 10));

        AtomicInteger callsToDelegate = new AtomicInteger();
        MerchantClient delegate = event -> {
            callsToDelegate.incrementAndGet();
            if (event.merchantId().value().equals("bad-merchant")) {
                return DeliveryResult.failure(503, 5, FailureCategory.RETRYABLE, "HTTP 503");
            }
            return DeliveryResult.success(200, 5);
        };

        ResilientMerchantClient client = new ResilientMerchantClient(delegate, guardRegistry);

        // Two failures trip the bad merchant's breaker open (minimumCalls=2, threshold=0.5).
        client.deliver(eventFor("bad-merchant"));
        client.deliver(eventFor("bad-merchant"));
        assertThat(guardRegistry.forMerchant(MerchantId.of("bad-merchant")).circuitBreaker().state().name())
                .isEqualTo("OPEN");

        int callsBefore = callsToDelegate.get();
        DeliveryResult blocked = client.deliver(eventFor("bad-merchant"));
        assertThat(blocked.success()).isFalse();
        assertThat(blocked.failureDetail()).isEqualTo("circuit_open");
        // No HTTP call should have been made for the blocked attempt.
        assertThat(callsToDelegate.get()).isEqualTo(callsBefore);

        // The healthy merchant must be completely unaffected.
        DeliveryResult goodResult = client.deliver(eventFor("good-merchant"));
        assertThat(goodResult.success()).isTrue();
        assertThat(guardRegistry.forMerchant(MerchantId.of("good-merchant")).circuitBreaker().state().name())
                .isEqualTo("CLOSED");
    }
}

package com.paymentnotify.resilience;

import com.paymentnotify.config.CircuitBreakerProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MerchantGuardTest {

    @Test
    void concurrencyIsBoundedAndReleasable() {
        MerchantGuard guard = new MerchantGuard(
                new CircuitBreaker("m", new CircuitBreakerProperties(0.5, 10, 4, 1000, 2)),
                new RateLimiter(1000),
                3);

        assertThat(guard.tryAcquireConcurrency()).isTrue();
        assertThat(guard.tryAcquireConcurrency()).isTrue();
        assertThat(guard.tryAcquireConcurrency()).isTrue();
        // 4th call exceeds the bound of 3.
        assertThat(guard.tryAcquireConcurrency()).isFalse();
        assertThat(guard.activeConcurrency()).isEqualTo(3);

        guard.releaseConcurrency();
        assertThat(guard.activeConcurrency()).isEqualTo(2);
        assertThat(guard.tryAcquireConcurrency()).isTrue();
    }
}

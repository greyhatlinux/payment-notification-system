package com.paymentnotify.resilience;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

class RateLimiterTest {

    @Test
    void allowsBurstUpToCapacityThenRejects() {
        RateLimiter limiter = new RateLimiter(5);

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    void refillsOverTime() {
        RateLimiter limiter = new RateLimiter(10); // 10/sec => 1 token every 100ms
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire();
        }
        assertThat(limiter.tryAcquire()).isFalse();

        await().atMost(Duration.ofSeconds(2)).until(limiter::tryAcquire);
    }
}

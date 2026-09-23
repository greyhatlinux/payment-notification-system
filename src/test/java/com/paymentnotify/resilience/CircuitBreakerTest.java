package com.paymentnotify.resilience;

import com.paymentnotify.config.CircuitBreakerProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

class CircuitBreakerTest {

    private CircuitBreaker breakerWith(double threshold, int windowSize, int minimumCalls,
                                        long openWaitMs, int halfOpenPermitted) {
        return new CircuitBreaker("test-merchant",
                new CircuitBreakerProperties(threshold, windowSize, minimumCalls, openWaitMs, halfOpenPermitted));
    }

    @Test
    void staysClosedBelowFailureThreshold() {
        CircuitBreaker breaker = breakerWith(0.5, 10, 4, 1000, 2);

        breaker.recordResult(true);
        breaker.recordResult(false);
        breaker.recordResult(true);
        breaker.recordResult(true);

        assertThat(breaker.state()).isEqualTo(CircuitState.CLOSED);
        assertThat(breaker.allowRequest()).isTrue();
    }

    @Test
    void opensWhenFailureRateReachesThreshold() {
        CircuitBreaker breaker = breakerWith(0.5, 10, 4, 1000, 2);

        breaker.recordResult(false);
        breaker.recordResult(false);
        breaker.recordResult(true);
        breaker.recordResult(true);

        assertThat(breaker.state()).isEqualTo(CircuitState.OPEN);
    }

    @Test
    void rejectsCallsWhileOpen() {
        CircuitBreaker breaker = breakerWith(0.5, 10, 2, 10_000, 2);
        breaker.recordResult(false);
        breaker.recordResult(false);
        assertThat(breaker.state()).isEqualTo(CircuitState.OPEN);

        // No HTTP call should be attempted while OPEN.
        assertThat(breaker.allowRequest()).isFalse();
        assertThat(breaker.allowRequest()).isFalse();
    }

    @Test
    void transitionsToHalfOpenAfterWaitAndRecoversOnSuccess() {
        CircuitBreaker breaker = breakerWith(0.5, 10, 2, 100, 2);
        breaker.recordResult(false);
        breaker.recordResult(false);
        assertThat(breaker.state()).isEqualTo(CircuitState.OPEN);

        await().atMost(Duration.ofSeconds(2)).until(() -> {
            boolean allowed = breaker.allowRequest();
            return allowed && breaker.state() == CircuitState.HALF_OPEN;
        });

        // Two trial successes (halfOpenPermittedCalls=2) should close the breaker.
        breaker.recordResult(true);
        assertThat(breaker.allowRequest()).isTrue();
        breaker.recordResult(true);

        assertThat(breaker.state()).isEqualTo(CircuitState.CLOSED);
    }

    @Test
    void halfOpenFailureReopensCircuit() {
        CircuitBreaker breaker = breakerWith(0.5, 10, 2, 100, 2);
        breaker.recordResult(false);
        breaker.recordResult(false);
        assertThat(breaker.state()).isEqualTo(CircuitState.OPEN);

        await().atMost(Duration.ofSeconds(2)).until(() -> {
            boolean allowed = breaker.allowRequest();
            return allowed && breaker.state() == CircuitState.HALF_OPEN;
        });

        breaker.recordResult(false);
        assertThat(breaker.state()).isEqualTo(CircuitState.OPEN);
        assertThat(breaker.allowRequest()).isFalse();
    }

    @Test
    void halfOpenLimitsConcurrentTrialCalls() {
        CircuitBreaker breaker = breakerWith(0.5, 10, 2, 50, 1);
        breaker.recordResult(false);
        breaker.recordResult(false);
        assertThat(breaker.state()).isEqualTo(CircuitState.OPEN);

        await().atMost(Duration.ofSeconds(2)).until(breaker::allowRequest);
        // halfOpenPermittedCalls=1: a second concurrent trial call should be rejected
        // until the first trial's outcome is recorded.
        assertThat(breaker.allowRequest()).isFalse();
    }
}

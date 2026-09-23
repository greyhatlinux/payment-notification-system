package com.paymentnotify.retry;

import com.paymentnotify.config.RetryProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffPolicyTest {

    @Test
    void delayGrowsExponentiallyWithAttempt() {
        RetryProperties props = new RetryProperties(6, 5000, 4.5, 7_200_000, 0.0, 0, 0, 0);
        BackoffPolicy policy = new BackoffPolicy(props);

        long attempt1 = policy.delayMillisFor(1);
        long attempt2 = policy.delayMillisFor(2);
        long attempt3 = policy.delayMillisFor(3);

        assertThat(attempt1).isEqualTo(5000);
        assertThat(attempt2).isGreaterThan(attempt1);
        assertThat(attempt3).isGreaterThan(attempt2);
    }

    @Test
    void delayIsCappedAtMaxDelay() {
        RetryProperties props = new RetryProperties(6, 5000, 4.5, 7_200_000, 0.0, 0, 0, 0);
        BackoffPolicy policy = new BackoffPolicy(props);

        long attempt6 = policy.delayMillisFor(6);

        assertThat(attempt6).isEqualTo(7_200_000);
    }

    @Test
    void jitterKeepsDelayWithinConfiguredRatio() {
        RetryProperties props = new RetryProperties(6, 10_000, 1.0, 7_200_000, 0.2, 0, 0, 0);
        BackoffPolicy policy = new BackoffPolicy(props);

        // multiplier=1.0 => base delay is always 10s; jitterRatio=0.2 => +/-2s.
        for (int i = 0; i < 200; i++) {
            long delay = policy.delayMillisFor(1);
            assertThat(delay).isBetween(8_000L, 12_000L);
        }
    }

    @Test
    void withoutJitterRepeatedCallsAreDeterministic() {
        RetryProperties props = new RetryProperties(6, 5000, 4.5, 7_200_000, 0.0, 0, 0, 0);
        BackoffPolicy policy = new BackoffPolicy(props);

        assertThat(policy.delayMillisFor(2)).isEqualTo(policy.delayMillisFor(2));
    }
}

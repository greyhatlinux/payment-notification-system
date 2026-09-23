package com.paymentnotify.retry;

import com.paymentnotify.config.RetryProperties;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with jitter, fully configurable (see AGENTS.md's
 * example policy: attempt 1 ~5s, 2 ~30s, 3 ~2m, 4 ~10m, 5 ~30m, 6 ~2h — the
 * defaults in application.yml approximate that curve, capped at
 * maxDelayMs).
 *
 * delay = min(baseDelay * multiplier^(attempt-1), maxDelay) +/- jitterRatio
 *
 * Jitter is applied as +/-(delay * jitterRatio) so retries from many
 * simultaneously-failing payments don't all land on the same instant and
 * hammer a recovering merchant (a "retry storm").
 */
@Component
public class BackoffPolicy {

    private final RetryProperties props;

    public BackoffPolicy(RetryProperties props) {
        this.props = props;
    }

    public int maxAttempts() {
        return props.maxAttempts();
    }

    public long delayMillisFor(int attempt) {
        double raw = props.baseDelayMs() * Math.pow(props.backoffMultiplier(), Math.max(attempt - 1, 0));
        double capped = Math.min(raw, props.maxDelayMs());
        double jitterSpan = capped * props.jitterRatio();
        double jitter = jitterSpan <= 0 ? 0 : ThreadLocalRandom.current().nextDouble(-jitterSpan, jitterSpan);
        return Math.max(Math.round(capped + jitter), 0);
    }

    public Instant nextAttemptAt(int attempt) {
        return Instant.now().plusMillis(delayMillisFor(attempt));
    }
}

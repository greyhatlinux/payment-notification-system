package com.paymentnotify.delivery;

import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.FailureCategory;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.resilience.MerchantGuard;
import com.paymentnotify.resilience.MerchantGuardRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/**
 * The {@link MerchantClient} that C1 and C2 actually use: wraps
 * {@link HttpMerchantClient} with the per-merchant circuit breaker, rate
 * limit and concurrency limit. Because both C1 and C2 depend on the
 * {@link MerchantClient} interface and this is the sole {@code @Primary}
 * implementation, they transparently share the same {@link MerchantGuard}
 * (and therefore the same circuit breaker state) per merchant.
 *
 * When the circuit is OPEN, or a local rate/concurrency limit is hit, no
 * HTTP call is made at all — the result is a retryable failure, which
 * C1DeliveryService/C2 will route to the scheduled retry queue exactly like
 * a real merchant failure.
 */
@Component
@Primary
public class ResilientMerchantClient implements MerchantClient {

    private final MerchantClient delegate;
    private final MerchantGuardRegistry guardRegistry;

    public ResilientMerchantClient(@Qualifier("httpMerchantClient") MerchantClient delegate,
                                    MerchantGuardRegistry guardRegistry) {
        this.delegate = delegate;
        this.guardRegistry = guardRegistry;
    }

    @Override
    public DeliveryResult deliver(PaymentEvent event) {
        MerchantGuard guard = guardRegistry.forMerchant(event.merchantId());

        if (!guard.circuitBreaker().allowRequest()) {
            return DeliveryResult.failure(null, 0, FailureCategory.RETRYABLE, "circuit_open");
        }
        if (!guard.rateLimiter().tryAcquire()) {
            return DeliveryResult.failure(null, 0, FailureCategory.RETRYABLE, "rate_limited");
        }
        if (!guard.tryAcquireConcurrency()) {
            return DeliveryResult.failure(null, 0, FailureCategory.RETRYABLE, "concurrency_limit_reached");
        }
        try {
            DeliveryResult result = delegate.deliver(event);
            guard.circuitBreaker().recordResult(result.success());
            return result;
        } finally {
            guard.releaseConcurrency();
        }
    }
}

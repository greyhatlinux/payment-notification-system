package com.paymentnotify.resilience;

import com.paymentnotify.config.CircuitBreakerProperties;
import com.paymentnotify.config.ConcurrencyProperties;
import com.paymentnotify.config.RateLimitProperties;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.merchant.MerchantRegistry;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One {@link MerchantGuard} per merchant, created lazily (and bounded — the
 * number of distinct merchants is small and fixed for this demo). Known
 * merchants are pre-created at startup so the dashboard shows CLOSED/healthy
 * state for everyone before any traffic has flowed.
 */
@Component
public class MerchantGuardRegistry {

    private final Map<MerchantId, MerchantGuard> guards = new ConcurrentHashMap<>();
    private final CircuitBreakerProperties circuitBreakerProperties;
    private final RateLimitProperties rateLimitProperties;
    private final ConcurrencyProperties concurrencyProperties;

    public MerchantGuardRegistry(MerchantRegistry merchantRegistry,
                                  CircuitBreakerProperties circuitBreakerProperties,
                                  RateLimitProperties rateLimitProperties,
                                  ConcurrencyProperties concurrencyProperties) {
        this.circuitBreakerProperties = circuitBreakerProperties;
        this.rateLimitProperties = rateLimitProperties;
        this.concurrencyProperties = concurrencyProperties;
        for (MerchantId merchantId : merchantRegistry.all()) {
            guards.put(merchantId, newGuard(merchantId));
        }
    }

    public MerchantGuard forMerchant(MerchantId merchantId) {
        return guards.computeIfAbsent(merchantId, this::newGuard);
    }

    public Map<MerchantId, MerchantGuard> all() {
        return Map.copyOf(guards);
    }

    private MerchantGuard newGuard(MerchantId merchantId) {
        return new MerchantGuard(
                new CircuitBreaker(merchantId.value(), circuitBreakerProperties),
                new RateLimiter(rateLimitProperties.perMerchantPermitsPerSecond()),
                concurrencyProperties.perMerchantMaxConcurrency()
        );
    }
}

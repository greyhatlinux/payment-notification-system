package com.paymentnotify.delivery;

import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.PaymentEvent;

/**
 * Calls a merchant to deliver a payment notification. Shared, unchanged,
 * between C1 (synchronous first attempt) and C2 (retry worker) — both sit
 * on top of the same MerchantClient -> CircuitBreaker -> merchant path, per
 * AGENTS.md's "shared circuit breaker state" requirement.
 *
 * The call is synchronous from the caller's point of view (it returns a
 * result, not a future) because the caller — an S2 partition consumer or a
 * C2 worker — needs the outcome before deciding what to do next (commit
 * offset / reschedule). Internally, implementations should still use
 * non-blocking I/O and bounded connection pools rather than a
 * thread-per-request model.
 */
public interface MerchantClient {

    DeliveryResult deliver(PaymentEvent event);
}

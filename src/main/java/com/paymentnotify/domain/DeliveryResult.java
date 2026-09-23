package com.paymentnotify.domain;

/**
 * Outcome of a single attempt to call a merchant, made by either C1 (the
 * synchronous first attempt) or C2 (a retry worker). Both paths share this
 * shape so the circuit breaker / metrics / retry-scheduling code can treat
 * them uniformly.
 */
public record DeliveryResult(
        boolean success,
        Integer httpStatus,
        long latencyMs,
        FailureCategory failureCategory,
        String failureDetail
) {

    public static DeliveryResult success(int httpStatus, long latencyMs) {
        return new DeliveryResult(true, httpStatus, latencyMs, null, null);
    }

    public static DeliveryResult failure(Integer httpStatus, long latencyMs,
                                          FailureCategory category, String detail) {
        return new DeliveryResult(false, httpStatus, latencyMs, category, detail);
    }

    public boolean isRetryable() {
        return !success && failureCategory == FailureCategory.RETRYABLE;
    }
}

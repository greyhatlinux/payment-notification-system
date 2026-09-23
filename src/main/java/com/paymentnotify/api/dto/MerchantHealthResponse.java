package com.paymentnotify.api.dto;

import com.paymentnotify.merchant.FailureInjectionConfig;
import com.paymentnotify.resilience.CircuitState;

public record MerchantHealthResponse(
        String merchantId,
        String status,
        double requestRatePerSecond,
        double successRatePercent,
        double failureRatePercent,
        double averageLatencyMs,
        CircuitState circuitBreakerState,
        int activeRequests,
        int concurrencyLimit,
        FailureInjectionConfig failureInjection
) {
}

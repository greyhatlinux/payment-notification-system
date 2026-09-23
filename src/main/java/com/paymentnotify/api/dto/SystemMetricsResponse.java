package com.paymentnotify.api.dto;

public record SystemMetricsResponse(
        double ingestionPerSecond,
        double processingPerSecond,
        double deliveryPerSecond,
        double successRatePercent,
        double retryRatePerSecond,
        long dlqTotalCount,
        int dlqCurrentSize,
        int scheduledQueueDepth,
        long consumerLagTotal,
        int activeC2Workers,
        long totalProcessed,
        long totalDelivered,
        long totalRetryScheduled
) {
}

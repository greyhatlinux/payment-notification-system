package com.paymentnotify.metrics;

import com.paymentnotify.api.dto.CircuitBreakerStatusResponse;
import com.paymentnotify.api.dto.MerchantHealthResponse;
import com.paymentnotify.api.dto.SystemMetricsResponse;
import com.paymentnotify.c2.C2WorkerPool;
import com.paymentnotify.dlq.DeadLetterStore;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.ingestion.EventSource;
import com.paymentnotify.ingestion.TrafficGenerator;
import com.paymentnotify.merchant.MerchantRegistry;
import com.paymentnotify.merchant.MerchantSimulatorConfigService;
import com.paymentnotify.resilience.CircuitState;
import com.paymentnotify.resilience.MerchantGuard;
import com.paymentnotify.resilience.MerchantGuardRegistry;
import com.paymentnotify.retry.ScheduledMessageQueue;
import org.springframework.stereotype.Service;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Assembles the dashboard/API-facing metrics views by pulling live state
 * out of the various components (no separate storage of its own).
 */
@Service
public class MetricsService {

    private final MetricsRegistry metrics;
    private final EventSource eventSource;
    private final ScheduledMessageQueue retryQueue;
    private final DeadLetterStore deadLetterStore;
    private final C2WorkerPool c2WorkerPool;
    private final MerchantGuardRegistry guardRegistry;
    private final MerchantSimulatorConfigService failureInjectionConfig;
    private final MerchantRegistry merchantRegistry;
    private final TrafficGenerator trafficGenerator;

    public MetricsService(MetricsRegistry metrics,
                           EventSource eventSource,
                           ScheduledMessageQueue retryQueue,
                           DeadLetterStore deadLetterStore,
                           C2WorkerPool c2WorkerPool,
                           MerchantGuardRegistry guardRegistry,
                           MerchantSimulatorConfigService failureInjectionConfig,
                           MerchantRegistry merchantRegistry,
                           TrafficGenerator trafficGenerator) {
        this.metrics = metrics;
        this.eventSource = eventSource;
        this.retryQueue = retryQueue;
        this.deadLetterStore = deadLetterStore;
        this.c2WorkerPool = c2WorkerPool;
        this.guardRegistry = guardRegistry;
        this.failureInjectionConfig = failureInjectionConfig;
        this.merchantRegistry = merchantRegistry;
        this.trafficGenerator = trafficGenerator;
    }

    public SystemMetricsResponse systemMetrics() {
        long totalRequests = 0;
        long totalSuccesses = 0;
        for (MerchantStats stats : metrics.allMerchantStats().values()) {
            totalRequests += stats.requests();
            totalSuccesses += stats.successes();
        }
        double successRatePercent = totalRequests == 0 ? 100.0 : (100.0 * totalSuccesses / totalRequests);

        long consumerLag = 0;
        for (int p = 0; p < eventSource.partitionCount(); p++) {
            consumerLag += eventSource.endOffset(p) - (eventSource.committedOffset(p) + 1);
        }

        return new SystemMetricsResponse(
                trafficGenerator.status().actualEventsPerSecond(),
                metrics.processedRate.ratePerSecond(),
                metrics.deliveredRate.ratePerSecond(),
                successRatePercent,
                metrics.retryScheduledRate.ratePerSecond(),
                deadLetterStore.totalCount(),
                deadLetterStore.currentSize(),
                retryQueue.size(),
                consumerLag,
                c2WorkerPool.activeWorkers(),
                metrics.processedRate.total(),
                metrics.deliveredRate.total(),
                metrics.retryScheduledRate.total()
        );
    }

    public List<MerchantHealthResponse> merchantHealth() {
        // Union of known/registered merchants and any merchant that has ever had traffic
        // (e.g. custom merchant ids used via the traffic-generation API).
        Set<MerchantId> ids = new LinkedHashSet<>(merchantRegistry.all());
        ids.addAll(metrics.allMerchantStats().keySet());

        return ids.stream().map(this::merchantHealth).toList();
    }

    public MerchantHealthResponse merchantHealth(MerchantId merchantId) {
        MerchantStats stats = metrics.statsFor(merchantId);
        MerchantGuard guard = guardRegistry.forMerchant(merchantId);
        CircuitState state = guard.circuitBreaker().state();

        return new MerchantHealthResponse(
                merchantId.value(),
                statusFor(state),
                stats.requestRate.ratePerSecond(),
                stats.successRate() * 100.0,
                (1 - stats.successRate()) * 100.0,
                stats.averageLatencyMs(),
                state,
                guard.activeConcurrency(),
                guard.concurrencyLimit(),
                failureInjectionConfig.configFor(merchantId)
        );
    }

    public List<CircuitBreakerStatusResponse> circuitBreakerStates() {
        Set<MerchantId> ids = new LinkedHashSet<>(merchantRegistry.all());
        ids.addAll(metrics.allMerchantStats().keySet());
        return ids.stream()
                .map(id -> {
                    var breaker = guardRegistry.forMerchant(id).circuitBreaker();
                    return new CircuitBreakerStatusResponse(id.value(), breaker.state(), breaker.currentFailureRate());
                })
                .toList();
    }

    private String statusFor(CircuitState state) {
        return switch (state) {
            case CLOSED -> "HEALTHY";
            case HALF_OPEN -> "DEGRADED";
            case OPEN -> "DOWN";
        };
    }
}

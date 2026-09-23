package com.paymentnotify.api;

import com.paymentnotify.api.dto.CircuitBreakerStatusResponse;
import com.paymentnotify.api.dto.MerchantHealthResponse;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.merchant.FailureInjectionConfig;
import com.paymentnotify.merchant.MerchantRegistry;
import com.paymentnotify.merchant.MerchantSimulatorConfigService;
import com.paymentnotify.metrics.MetricsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/merchants")
public class MerchantController {

    private final MerchantRegistry merchantRegistry;
    private final MerchantSimulatorConfigService failureInjectionConfig;
    private final MetricsService metricsService;

    public MerchantController(MerchantRegistry merchantRegistry,
                               MerchantSimulatorConfigService failureInjectionConfig,
                               MetricsService metricsService) {
        this.merchantRegistry = merchantRegistry;
        this.failureInjectionConfig = failureInjectionConfig;
        this.metricsService = metricsService;
    }

    @GetMapping
    public List<String> merchantIds() {
        return merchantRegistry.all().stream().map(MerchantId::value).toList();
    }

    /** "Merchant Health" dashboard section: status, rates, latency, circuit breaker, active requests. */
    @GetMapping("/health")
    public List<MerchantHealthResponse> health() {
        return metricsService.merchantHealth();
    }

    @GetMapping("/{merchantId}/health")
    public MerchantHealthResponse health(@PathVariable String merchantId) {
        return metricsService.merchantHealth(MerchantId.of(merchantId));
    }

    @GetMapping("/{merchantId}/failure-injection")
    public FailureInjectionConfig getFailureInjection(@PathVariable String merchantId) {
        return failureInjectionConfig.configFor(MerchantId.of(merchantId));
    }

    @PutMapping("/{merchantId}/failure-injection")
    public FailureInjectionConfig updateFailureInjection(@PathVariable String merchantId,
                                                           @RequestBody FailureInjectionConfig config) {
        MerchantId id = MerchantId.of(merchantId);
        failureInjectionConfig.updateConfig(id, config);
        return failureInjectionConfig.configFor(id);
    }

    @GetMapping("/circuit-breakers")
    public List<CircuitBreakerStatusResponse> circuitBreakers() {
        return metricsService.circuitBreakerStates();
    }

    @GetMapping("/{merchantId}/circuit-breaker")
    public CircuitBreakerStatusResponse circuitBreaker(@PathVariable String merchantId) {
        return metricsService.circuitBreakerStates().stream()
                .filter(cb -> cb.merchantId().equals(merchantId))
                .findFirst()
                .orElseGet(() -> {
                    var state = metricsService.merchantHealth(MerchantId.of(merchantId)).circuitBreakerState();
                    return new CircuitBreakerStatusResponse(merchantId, state, 0.0);
                });
    }
}

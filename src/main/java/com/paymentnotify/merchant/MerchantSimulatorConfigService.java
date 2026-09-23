package com.paymentnotify.merchant;

import com.paymentnotify.domain.MerchantId;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the current failure-injection configuration for every merchant.
 * Read on every simulated request; written by the failure-injection API.
 * Bounded by merchant count (never grows unboundedly).
 */
@Component
public class MerchantSimulatorConfigService {

    private final Map<MerchantId, FailureInjectionConfig> configs = new ConcurrentHashMap<>();

    public MerchantSimulatorConfigService(MerchantRegistry registry) {
        for (MerchantId merchantId : registry.all()) {
            configs.put(merchantId, FailureInjectionConfig.HEALTHY_DEFAULT);
        }
    }

    public FailureInjectionConfig configFor(MerchantId merchantId) {
        return configs.getOrDefault(merchantId, FailureInjectionConfig.HEALTHY_DEFAULT);
    }

    public void updateConfig(MerchantId merchantId, FailureInjectionConfig config) {
        configs.put(merchantId, config);
    }

    public Map<MerchantId, FailureInjectionConfig> allConfigs() {
        return Map.copyOf(configs);
    }
}

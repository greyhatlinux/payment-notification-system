package com.paymentnotify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.merchant-simulator")
public record MerchantSimulatorProperties(
        int port,
        int workerThreads
) {
    public MerchantSimulatorProperties {
        if (workerThreads <= 0) {
            workerThreads = 200;
        }
    }
}

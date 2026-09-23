package com.paymentnotify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.concurrency")
public record ConcurrencyProperties(
        int s2ConsumerThreadsPerPartition,
        int c2WorkerPoolSize,
        int perMerchantMaxConcurrency
) {
}

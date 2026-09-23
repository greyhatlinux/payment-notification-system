package com.paymentnotify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.http-client")
public record HttpClientProperties(
        long connectTimeoutMs,
        long requestTimeoutMs,
        int maxConnectionsPerMerchant
) {
}

package com.paymentnotify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public record KafkaProperties(
        int partitionCount,
        int partitionBufferCapacity
) {
    public KafkaProperties {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0");
        }
        if (partitionBufferCapacity <= 0) {
            throw new IllegalArgumentException("partitionBufferCapacity must be > 0");
        }
    }
}

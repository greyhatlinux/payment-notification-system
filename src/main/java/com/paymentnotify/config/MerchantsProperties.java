package com.paymentnotify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "app.merchants")
public record MerchantsProperties(
        List<String> ids
) {
    public MerchantsProperties {
        if (ids == null || ids.isEmpty()) {
            ids = List.of("amazon", "flipkart", "swiggy", "zomato", "myntra");
        }
    }
}

package com.paymentnotify.domain;

/**
 * Identifies a merchant. A thin wrapper around a String so merchant identity
 * is type-safe wherever it's used as a map key (circuit breakers, rate
 * limiters, metrics registries, etc).
 */
public record MerchantId(String value) {

    public MerchantId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("MerchantId must not be blank");
        }
    }

    public static MerchantId of(String value) {
        return new MerchantId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}

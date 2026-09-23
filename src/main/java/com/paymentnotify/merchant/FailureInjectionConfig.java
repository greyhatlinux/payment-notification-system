package com.paymentnotify.merchant;

/**
 * Per-merchant behavior for the simulator, mutable at runtime via the API/UI.
 *
 * @param mode              which failure to return when the failure roll hits (ignored if failurePercentage is 0)
 * @param failurePercentage 0-100, chance any given request is failed with {@code mode} instead of succeeding
 * @param latencyMs         base latency applied to every request, success or failure, to simulate real network/processing time
 * @param specific4xxStatus HTTP status returned when mode == HTTP_4XX (e.g. 400, 401, 403, 404)
 */
public record FailureInjectionConfig(
        FailureMode mode,
        int failurePercentage,
        long latencyMs,
        int specific4xxStatus
) {
    public static final FailureInjectionConfig HEALTHY_DEFAULT =
            new FailureInjectionConfig(FailureMode.SUCCESS, 0, 20, 400);

    public FailureInjectionConfig {
        if (failurePercentage < 0 || failurePercentage > 100) {
            throw new IllegalArgumentException("failurePercentage must be between 0 and 100");
        }
        if (latencyMs < 0) {
            throw new IllegalArgumentException("latencyMs must be >= 0");
        }
    }
}

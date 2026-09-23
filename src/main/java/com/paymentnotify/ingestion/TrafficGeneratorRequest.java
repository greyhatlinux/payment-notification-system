package com.paymentnotify.ingestion;

import java.util.List;

/**
 * API input to start the synthetic traffic generator.
 *
 * @param eventsPerSecond target sustained publish rate
 * @param durationSeconds run for this long then auto-stop; null/0 = run until explicitly stopped
 * @param merchantIds     merchants to distribute events across (round-robin/random); null/empty = all known merchants
 */
public record TrafficGeneratorRequest(
        int eventsPerSecond,
        Integer durationSeconds,
        List<String> merchantIds
) {
}

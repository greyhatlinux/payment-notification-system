package com.paymentnotify.ingestion;

import java.time.Instant;
import java.util.List;

public record TrafficGeneratorStatus(
        boolean running,
        int targetEventsPerSecond,
        double actualEventsPerSecond,
        long totalPublished,
        Instant startedAt,
        Instant willStopAt,
        List<String> merchantIds
) {
}

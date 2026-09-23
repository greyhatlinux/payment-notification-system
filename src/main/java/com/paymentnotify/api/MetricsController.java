package com.paymentnotify.api;

import com.paymentnotify.api.dto.SystemMetricsResponse;
import com.paymentnotify.domain.NotificationHistoryEntry;
import com.paymentnotify.metrics.MetricsRegistry;
import com.paymentnotify.metrics.MetricsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final MetricsService metricsService;
    private final MetricsRegistry metricsRegistry;

    public MetricsController(MetricsService metricsService, MetricsRegistry metricsRegistry) {
        this.metricsService = metricsService;
        this.metricsRegistry = metricsRegistry;
    }

    @GetMapping("/system")
    public SystemMetricsResponse system() {
        return metricsService.systemMetrics();
    }

    /** Bounded recent notification history (success + failure), for a live feed in the UI. */
    @GetMapping("/history")
    public List<NotificationHistoryEntry> history() {
        return metricsRegistry.recentHistory();
    }
}

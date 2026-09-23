package com.paymentnotify.api;

import com.paymentnotify.ingestion.TrafficGenerator;
import com.paymentnotify.ingestion.TrafficGeneratorRequest;
import com.paymentnotify.ingestion.TrafficGeneratorStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/traffic")
public class TrafficController {

    private final TrafficGenerator trafficGenerator;

    public TrafficController(TrafficGenerator trafficGenerator) {
        this.trafficGenerator = trafficGenerator;
    }

    @PostMapping("/start")
    public TrafficGeneratorStatus start(@RequestBody TrafficGeneratorRequest request) {
        return trafficGenerator.start(request);
    }

    @PostMapping("/stop")
    public TrafficGeneratorStatus stop() {
        return trafficGenerator.stop();
    }

    @GetMapping("/status")
    public TrafficGeneratorStatus status() {
        return trafficGenerator.status();
    }
}

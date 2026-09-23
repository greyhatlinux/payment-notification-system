package com.paymentnotify;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Confirms the whole Spring context wires up (every bean across S2, C1,
 * resilience, retry, C2, DLQ, metrics and the API layer resolves) and the
 * key REST endpoints respond, without depending on Kafka or any external
 * infrastructure.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApplicationSmokeTest {

    @LocalServerPort
    private int port;

    private final TestRestTemplate rest = new TestRestTemplate();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void contextLoadsAndHealthIsUp() {
        ResponseEntity<String> response = rest.getForEntity(url("/actuator/health"), String.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).contains("UP");
    }

    @Test
    void merchantListAndHealthEndpointsRespond() {
        ResponseEntity<String> merchants = rest.getForEntity(url("/api/merchants"), String.class);
        assertThat(merchants.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(merchants.getBody()).contains("amazon");

        ResponseEntity<String> health = rest.getForEntity(url("/api/merchants/health"), String.class);
        assertThat(health.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(health.getBody()).contains("circuitBreakerState");
    }

    @Test
    void systemMetricsEndpointResponds() {
        ResponseEntity<String> response = rest.getForEntity(url("/api/metrics/system"), String.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).contains("scheduledQueueDepth");
    }

    @Test
    void retryQueueAndDlqEndpointsRespond() {
        assertThat(rest.getForEntity(url("/api/retry-queue"), String.class).getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(rest.getForEntity(url("/api/dlq"), String.class).getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(rest.getForEntity(url("/api/merchants/circuit-breakers"), String.class).getStatusCode().is2xxSuccessful()).isTrue();
    }
}

package com.paymentnotify.merchant;

import com.paymentnotify.config.MerchantSimulatorProperties;
import com.paymentnotify.config.MerchantsProperties;
import com.paymentnotify.domain.MerchantId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MerchantSimulatorServerTest {

    private MerchantSimulatorServer simulator;
    private MerchantSimulatorConfigService configService;
    private HttpClient httpClient;
    private int port;

    @BeforeEach
    void setUp() {
        MerchantRegistry registry = new MerchantRegistry(new MerchantsProperties(List.of("amazon")));
        configService = new MerchantSimulatorConfigService(registry);
        simulator = new MerchantSimulatorServer(configService, new MerchantSimulatorProperties(0, 8));
        simulator.start();
        port = simulator.boundPort();
        httpClient = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        simulator.stop();
    }

    private HttpResponse<String> post(String merchantId, Duration timeout) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/merchants/" + merchantId + "/notify"))
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void defaultConfigAlwaysSucceeds() throws Exception {
        HttpResponse<String> response = post("amazon", Duration.ofSeconds(2));
        assertThat(response.statusCode()).isEqualTo(200);
    }

    @Test
    void canInjectAlways4xx() throws Exception {
        configService.updateConfig(MerchantId.of("amazon"),
                new FailureInjectionConfig(FailureMode.HTTP_4XX, 100, 0, 404));

        HttpResponse<String> response = post("amazon", Duration.ofSeconds(2));
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void canInject503() throws Exception {
        configService.updateConfig(MerchantId.of("amazon"),
                new FailureInjectionConfig(FailureMode.HTTP_503, 100, 0, 400));

        HttpResponse<String> response = post("amazon", Duration.ofSeconds(2));
        assertThat(response.statusCode()).isEqualTo(503);
    }

    @Test
    void canInject429() throws Exception {
        configService.updateConfig(MerchantId.of("amazon"),
                new FailureInjectionConfig(FailureMode.HTTP_429, 100, 0, 400));

        HttpResponse<String> response = post("amazon", Duration.ofSeconds(2));
        assertThat(response.statusCode()).isEqualTo(429);
    }

    @Test
    void canInjectTimeout() {
        configService.updateConfig(MerchantId.of("amazon"),
                new FailureInjectionConfig(FailureMode.TIMEOUT, 100, 0, 400));

        assertThatThrownBy(() -> post("amazon", Duration.ofMillis(300)))
                .isInstanceOf(HttpTimeoutException.class);
    }

    @Test
    void appliesConfiguredLatency() throws Exception {
        configService.updateConfig(MerchantId.of("amazon"),
                new FailureInjectionConfig(FailureMode.SUCCESS, 0, 300, 400));

        long start = System.nanoTime();
        post("amazon", Duration.ofSeconds(2));
        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        assertThat(elapsedMs).isGreaterThanOrEqualTo(280);
    }

    @Test
    void failurePercentageIsRoughlyRespected() throws Exception {
        configService.updateConfig(MerchantId.of("amazon"),
                new FailureInjectionConfig(FailureMode.HTTP_500, 50, 0, 400));

        int failures = 0;
        int total = 200;
        for (int i = 0; i < total; i++) {
            HttpResponse<String> response = post("amazon", Duration.ofSeconds(2));
            if (response.statusCode() == 500) {
                failures++;
            }
        }
        // Statistical check with generous bounds to avoid flakiness.
        assertThat(failures).isBetween((int) (total * 0.3), (int) (total * 0.7));
    }

    @Test
    void unknownPathReturns404() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/nope"))
                .timeout(Duration.ofSeconds(2))
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(404);
    }
}

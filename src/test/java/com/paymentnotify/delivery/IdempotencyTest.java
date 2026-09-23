package com.paymentnotify.delivery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentnotify.config.FailureClassificationProperties;
import com.paymentnotify.config.HttpClientProperties;
import com.paymentnotify.config.MerchantSimulatorProperties;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.merchant.MerchantEndpointResolver;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka's at-least-once delivery means S2 may see (and C1/C2 may deliver)
 * the same eventId more than once. This system deliberately does not
 * suppress duplicates itself (see AGENTS.md #3/#15) — it just carries a
 * stable Idempotency-Key so a real merchant can dedupe, and handles being
 * called twice for the same event without any internal state corruption.
 */
class IdempotencyTest {

    private HttpServer server;
    private final List<String> capturedIdempotencyKeys = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    private HttpMerchantClient startClientAgainstCapturingServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/merchants/", exchange -> {
            capturedIdempotencyKeys.add(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            exchange.getRequestBody().readAllBytes();
            byte[] body = "{}".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();

        MerchantEndpointResolver resolver = new MerchantEndpointResolver(new MerchantSimulatorProperties(port, 8));
        FailureClassifier classifier = new FailureClassifier(
                new FailureClassificationProperties(List.of(408, 429), List.of(400, 401, 403, 404)));
        return new HttpMerchantClient(HttpClient.newHttpClient(), resolver, classifier, new ObjectMapper(),
                new HttpClientProperties(500, 2000, 20));
    }

    @Test
    void duplicateEventIsDeliveredTwiceWithTheSameIdempotencyKey() throws Exception {
        HttpMerchantClient client = startClientAgainstCapturingServer();
        PaymentEvent event = new PaymentEvent("evt-duplicate-1", "pay-1", MerchantId.of("amazon"),
                BigDecimal.TEN, "USD", Instant.now());

        // Simulate Kafka at-least-once redelivery: the same event is processed twice.
        DeliveryResult first = client.deliver(event);
        DeliveryResult second = client.deliver(event);

        assertThat(first.success()).isTrue();
        assertThat(second.success()).isTrue();
        assertThat(capturedIdempotencyKeys).containsExactly("evt-duplicate-1", "evt-duplicate-1");
    }

    @Test
    void c1DeliveryServiceHandlesTheSameEventTwiceWithoutError() {
        MerchantClient client = e -> DeliveryResult.success(200, 5);
        C1DeliveryService c1 = new C1DeliveryService(client, (e, r) -> { }, (e, a, r) -> { },
                new com.paymentnotify.metrics.MetricsRegistry());
        PaymentEvent event = new PaymentEvent("evt-dup-2", "pay-2", MerchantId.of("amazon"),
                BigDecimal.TEN, "USD", Instant.now());

        DeliveryResult first = c1.deliver(event);
        DeliveryResult second = c1.deliver(event);

        assertThat(first.success()).isTrue();
        assertThat(second.success()).isTrue();
    }
}

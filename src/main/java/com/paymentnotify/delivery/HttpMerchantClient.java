package com.paymentnotify.delivery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentnotify.config.HttpClientProperties;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.FailureCategory;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.merchant.MerchantEndpointResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Map;

/**
 * Default {@link MerchantClient}: a real (loopback) synchronous HTTP POST
 * to the merchant simulator, so timeouts/connection failures/status codes
 * are exercised for real rather than faked.
 *
 * The eventId is sent as an Idempotency-Key header — this system does not
 * suppress duplicate deliveries itself (Kafka's at-least-once guarantee
 * means a merchant may legitimately be notified more than once); it just
 * gives the receiver everything it needs to dedupe.
 */
@Component
public class HttpMerchantClient implements MerchantClient {

    private static final Logger log = LoggerFactory.getLogger(HttpMerchantClient.class);

    private final HttpClient httpClient;
    private final MerchantEndpointResolver endpointResolver;
    private final FailureClassifier failureClassifier;
    private final ObjectMapper objectMapper;
    private final Duration requestTimeout;

    public HttpMerchantClient(HttpClient httpClient,
                               MerchantEndpointResolver endpointResolver,
                               FailureClassifier failureClassifier,
                               ObjectMapper objectMapper,
                               HttpClientProperties props) {
        this.httpClient = httpClient;
        this.endpointResolver = endpointResolver;
        this.failureClassifier = failureClassifier;
        this.objectMapper = objectMapper;
        this.requestTimeout = Duration.ofMillis(props.requestTimeoutMs());
    }

    @Override
    public DeliveryResult deliver(PaymentEvent event) {
        String url = endpointResolver.resolve(event.merchantId());
        String body = toJson(event);

        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", event.eventId())
                .timeout(requestTimeout)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        long start = System.nanoTime();
        try {
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            long latencyMs = elapsedMs(start);
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return DeliveryResult.success(status, latencyMs);
            }
            FailureCategory category = failureClassifier.classify(status);
            return DeliveryResult.failure(status, latencyMs, category, "HTTP " + status);
        } catch (HttpTimeoutException e) {
            return DeliveryResult.failure(null, elapsedMs(start), FailureCategory.RETRYABLE, "timeout");
        } catch (IOException e) {
            return DeliveryResult.failure(null, elapsedMs(start), FailureCategory.RETRYABLE,
                    "connection failure: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DeliveryResult.failure(null, elapsedMs(start), FailureCategory.RETRYABLE, "interrupted");
        }
    }

    private long elapsedMs(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
    }

    private String toJson(PaymentEvent event) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "eventId", event.eventId(),
                    "paymentId", event.paymentId(),
                    "merchantId", event.merchantId().value(),
                    "amount", event.amount(),
                    "currency", event.currency(),
                    "createdAt", event.createdAt().toString()
            ));
        } catch (IOException e) {
            // Serialization of our own well-formed record should never fail.
            log.error("Failed to serialize PaymentEvent {}", event.eventId(), e);
            return "{}";
        }
    }
}

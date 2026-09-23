package com.paymentnotify.delivery;

import com.paymentnotify.dlq.DeadLetterSink;
import com.paymentnotify.domain.DeliveryResult;
import com.paymentnotify.domain.FailureCategory;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.metrics.MetricsRegistry;
import com.paymentnotify.retry.RetryEnqueuer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class C1DeliveryServiceTest {

    private MerchantClient merchantClient;
    private RetryEnqueuer retryEnqueuer;
    private DeadLetterSink deadLetterSink;
    private C1DeliveryService service;

    private final PaymentEvent event = new PaymentEvent(
            "evt-1", "pay-1", MerchantId.of("amazon"), BigDecimal.TEN, "USD", Instant.now());

    @BeforeEach
    void setUp() {
        merchantClient = mock(MerchantClient.class);
        retryEnqueuer = mock(RetryEnqueuer.class);
        deadLetterSink = mock(DeadLetterSink.class);
        service = new C1DeliveryService(merchantClient, retryEnqueuer, deadLetterSink, new MetricsRegistry());
    }

    @Test
    void successfulDeliveryDoesNotRetryOrDlq() {
        when(merchantClient.deliver(event)).thenReturn(DeliveryResult.success(200, 15));

        DeliveryResult result = service.deliver(event);

        assertThat(result.success()).isTrue();
        verify(retryEnqueuer, never()).enqueue(any(), any());
        verify(deadLetterSink, never()).send(any(), anyInt(), anyString());
    }

    @Test
    void retryableFailureSchedulesRetryNotDlq() {
        DeliveryResult failure = DeliveryResult.failure(503, 20, FailureCategory.RETRYABLE, "HTTP 503");
        when(merchantClient.deliver(event)).thenReturn(failure);

        DeliveryResult result = service.deliver(event);

        assertThat(result.success()).isFalse();
        verify(retryEnqueuer, times(1)).enqueue(event, failure);
        verify(deadLetterSink, never()).send(any(), anyInt(), anyString());
    }

    @Test
    void permanentFailureGoesStraightToDlqNotRetry() {
        DeliveryResult failure = DeliveryResult.failure(404, 10, FailureCategory.PERMANENT, "HTTP 404");
        when(merchantClient.deliver(event)).thenReturn(failure);

        DeliveryResult result = service.deliver(event);

        assertThat(result.success()).isFalse();
        verify(deadLetterSink, times(1)).send(event, 1, "HTTP 404");
        verify(retryEnqueuer, never()).enqueue(any(), any());
    }
}

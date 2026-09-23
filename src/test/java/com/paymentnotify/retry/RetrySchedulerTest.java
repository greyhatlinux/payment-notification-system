package com.paymentnotify.retry;

import com.paymentnotify.config.RetryProperties;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.domain.RetryMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RetrySchedulerTest {

    private RetryScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    @Test
    void dispatchesDueMessagesAndLeavesNotYetDueOnesInPlace() {
        RetryProperties props = new RetryProperties(6, 5000, 4.5, 7_200_000, 0.2, 1000, 50, 20);
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(props);
        CopyOnWriteArrayList<String> dispatched = new CopyOnWriteArrayList<>();
        RetryDispatcher dispatcher = message -> dispatched.add(message.eventId());

        scheduler = new RetryScheduler(queue, dispatcher, props);
        scheduler.start();

        PaymentEvent dueEvent = new PaymentEvent("evt-due", "pay-due", MerchantId.of("amazon"),
                BigDecimal.TEN, "USD", Instant.now());
        PaymentEvent notDueEvent = new PaymentEvent("evt-not-due", "pay-not-due", MerchantId.of("amazon"),
                BigDecimal.TEN, "USD", Instant.now());

        queue.schedule(RetryMessage.firstRetry(dueEvent, Instant.now(), "fail"));
        queue.schedule(RetryMessage.firstRetry(notDueEvent, Instant.now().plusSeconds(60), "fail"));

        await().atMost(Duration.ofSeconds(2)).until(() -> dispatched.contains("evt-due"));
        assertThat(dispatched).doesNotContain("evt-not-due");
        assertThat(queue.size()).isEqualTo(1);
    }
}

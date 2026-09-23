package com.paymentnotify.retry;

import com.paymentnotify.config.RetryProperties;
import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.domain.RetryMessage;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryScheduledQueueTest {

    private RetryMessage messageDueIn(long millis, String eventId) {
        PaymentEvent event = new PaymentEvent(eventId, "pay-" + eventId, MerchantId.of("amazon"),
                BigDecimal.TEN, "USD", Instant.now());
        return RetryMessage.firstRetry(event, Instant.now().plusMillis(millis), "some failure");
    }

    @Test
    void pollDueOnlyReturnsMessagesWhoseTimeHasArrived() throws Exception {
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(new RetryProperties(6, 5000, 4.5, 7_200_000, 0.2, 100, 10, 100));

        queue.schedule(messageDueIn(0, "due-now"));
        queue.schedule(messageDueIn(60_000, "due-later"));

        Thread.sleep(20);
        var due = queue.pollDue(10);

        assertThat(due).extracting(RetryMessage::eventId).containsExactly("due-now");
        assertThat(queue.size()).isEqualTo(1);
    }

    @Test
    void pollDueReturnsInNextAttemptOrder() {
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(new RetryProperties(6, 5000, 4.5, 7_200_000, 0.2, 100, 10, 100));

        queue.schedule(messageDueIn(-30, "third")); // already due, latest of the three
        queue.schedule(messageDueIn(-90, "first"));
        queue.schedule(messageDueIn(-60, "second"));

        var due = queue.pollDue(10);
        assertThat(due).extracting(RetryMessage::eventId).containsExactly("first", "second", "third");
    }

    @Test
    void schedulingBeyondCapacityIsRejected() {
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(new RetryProperties(6, 5000, 4.5, 7_200_000, 0.2, 2, 10, 100));

        assertThat(queue.schedule(messageDueIn(60_000, "one"))).isTrue();
        assertThat(queue.schedule(messageDueIn(60_000, "two"))).isTrue();
        assertThat(queue.schedule(messageDueIn(60_000, "three"))).isFalse();
        assertThat(queue.size()).isEqualTo(2);
    }

    @Test
    void peekAllDoesNotRemoveMessages() {
        InMemoryScheduledQueue queue = new InMemoryScheduledQueue(new RetryProperties(6, 5000, 4.5, 7_200_000, 0.2, 100, 10, 100));
        queue.schedule(messageDueIn(-10, "a"));
        queue.schedule(messageDueIn(60_000, "b"));

        assertThat(queue.peekAll()).hasSize(2);
        assertThat(queue.size()).isEqualTo(2);
    }
}

package com.paymentnotify.retry;

import com.paymentnotify.config.RetryProperties;
import com.paymentnotify.domain.RetryMessage;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Default {@link ScheduledMessageQueue}: an in-memory, time-ordered queue.
 * Requires no external infrastructure — this is what makes the retry path
 * work out of the box without Redis.
 *
 * Capacity is enforced explicitly (checked in {@link #schedule}) rather
 * than relying on a fixed-size collection, since a priority queue ordered
 * by due time doesn't map onto a simple bounded array; the effect is the
 * same: {@code schedule()} refuses new work past the configured capacity.
 */
@Component
public class InMemoryScheduledQueue implements ScheduledMessageQueue {

    private final PriorityBlockingQueue<RetryMessage> queue =
            new PriorityBlockingQueue<>(1024, Comparator.comparing(RetryMessage::nextAttemptAt));
    private final int capacity;

    public InMemoryScheduledQueue(RetryProperties props) {
        this.capacity = props.queueCapacity();
    }

    @Override
    public boolean schedule(RetryMessage message) {
        if (queue.size() >= capacity) {
            return false;
        }
        return queue.offer(message);
    }

    @Override
    public List<RetryMessage> pollDue(int maxBatchSize) {
        List<RetryMessage> due = new ArrayList<>(Math.min(maxBatchSize, 64));
        Instant now = Instant.now();
        while (due.size() < maxBatchSize) {
            RetryMessage head = queue.peek();
            if (head == null || head.nextAttemptAt().isAfter(now)) {
                break;
            }
            RetryMessage polled = queue.poll();
            if (polled == null) {
                break;
            }
            if (polled.nextAttemptAt().isAfter(now)) {
                // Lost a race with another poller; not actually due — put it back.
                queue.offer(polled);
                break;
            }
            due.add(polled);
        }
        return due;
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public List<RetryMessage> peekAll() {
        List<RetryMessage> snapshot = new ArrayList<>(queue);
        snapshot.sort(Comparator.comparing(RetryMessage::nextAttemptAt));
        return snapshot;
    }
}

package com.paymentnotify.api;

import com.paymentnotify.domain.RetryMessage;
import com.paymentnotify.retry.ScheduledMessageQueue;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/retry-queue")
public class RetryQueueController {

    private final ScheduledMessageQueue queue;

    public RetryQueueController(ScheduledMessageQueue queue) {
        this.queue = queue;
    }

    /** Pending messages, ordered by next attempt time — attempt number, merchant, paymentId all included. */
    @GetMapping
    public List<RetryMessage> pending() {
        return queue.peekAll();
    }

    @GetMapping("/size")
    public int size() {
        return queue.size();
    }
}

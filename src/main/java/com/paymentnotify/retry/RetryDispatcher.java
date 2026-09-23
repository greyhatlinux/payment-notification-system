package com.paymentnotify.retry;

import com.paymentnotify.domain.RetryMessage;

/**
 * Where {@link RetryScheduler} hands off a due message. Implemented by the
 * C2 worker pool from Phase 7 onward — kept as its own interface so the
 * scheduler (polling loop) is decoupled from how retries are actually
 * executed.
 */
public interface RetryDispatcher {

    void dispatch(RetryMessage message);
}

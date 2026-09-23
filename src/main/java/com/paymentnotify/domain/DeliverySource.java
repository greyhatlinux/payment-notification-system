package com.paymentnotify.domain;

/** Which code path made a delivery attempt. Used for metrics/history only. */
public enum DeliverySource {
    /** Synchronous first attempt, run inline on the S2 consumer thread. */
    C1,
    /** Asynchronous retry attempt, run on a C2 worker. */
    C2
}

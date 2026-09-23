package com.paymentnotify.ingestion;

/**
 * Assigns a partition for a given paymentId, the same way a real Kafka
 * producer would key on paymentId. All events for the same payment land on
 * the same partition and are therefore delivered to S2 in publish order.
 */
public final class Partitioner {

    private Partitioner() {
    }

    public static int partitionFor(String paymentId, int partitionCount) {
        return Math.floorMod(paymentId.hashCode(), partitionCount);
    }
}

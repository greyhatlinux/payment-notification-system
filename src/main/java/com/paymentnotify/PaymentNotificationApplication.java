package com.paymentnotify;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Payment Notification System — demo entry point.
 *
 * Pipeline (see AGENTS.md for full architecture):
 *   MockKafka -> S2 consumer -> C1 (sync delivery) -> merchant
 *   on failure -> Scheduled Retry Queue -> C2 worker -> merchant
 *   on repeated failure -> DLQ
 *
 * Runs with zero external infrastructure by default: Kafka is mocked
 * in-process, the retry queue is in-memory, and "merchants" are simulated
 * over loopback HTTP so the resilience code (timeouts, pooling, circuit
 * breakers) is exercised realistically.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PaymentNotificationApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentNotificationApplication.class, args);
    }
}

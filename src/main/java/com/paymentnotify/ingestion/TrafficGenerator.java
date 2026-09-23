package com.paymentnotify.ingestion;

import com.paymentnotify.domain.MerchantId;
import com.paymentnotify.domain.PaymentEvent;
import com.paymentnotify.merchant.MerchantRegistry;
import com.paymentnotify.metrics.RateCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Configurable synthetic producer for the mock Kafka topic. Exactly one
 * generator run is active at a time (matches the single "Traffic Generator"
 * control panel in the UI); starting a new run stops any previous one.
 *
 * Uses a single dedicated background thread that paces itself against a
 * target events/sec using a catch-up loop, rather than one thread per event
 * or an unbounded scheduler — this is the only thread this component ever
 * creates.
 */
@Component
public class TrafficGenerator {

    private static final Logger log = LoggerFactory.getLogger(TrafficGenerator.class);
    private static final long TICK_MILLIS = 20;

    private final MockKafkaBroker broker;
    private final MerchantRegistry merchantRegistry;
    private final RateCounter rateCounter = new RateCounter();

    private final AtomicReference<Run> currentRun = new AtomicReference<>();

    public TrafficGenerator(MockKafkaBroker broker, MerchantRegistry merchantRegistry) {
        this.broker = broker;
        this.merchantRegistry = merchantRegistry;
    }

    public synchronized TrafficGeneratorStatus start(TrafficGeneratorRequest request) {
        if (request.eventsPerSecond() <= 0) {
            throw new IllegalArgumentException("eventsPerSecond must be > 0");
        }
        stop();

        List<MerchantId> merchants = (request.merchantIds() == null || request.merchantIds().isEmpty())
                ? merchantRegistry.all()
                : request.merchantIds().stream().map(MerchantId::of).collect(Collectors.toList());
        if (merchants.isEmpty()) {
            throw new IllegalArgumentException("no merchants configured");
        }

        Instant startedAt = Instant.now();
        Instant willStopAt = (request.durationSeconds() != null && request.durationSeconds() > 0)
                ? startedAt.plusSeconds(request.durationSeconds())
                : null;

        Run run = new Run(request.eventsPerSecond(), merchants, startedAt, willStopAt);
        Thread thread = new Thread(() -> runLoop(run), "traffic-generator");
        thread.setDaemon(true);
        run.thread = thread;
        currentRun.set(run);
        thread.start();

        log.info("Traffic generator started: {} events/sec, merchants={}, duration={}s",
                request.eventsPerSecond(), merchants, request.durationSeconds());
        return status();
    }

    public synchronized TrafficGeneratorStatus stop() {
        Run run = currentRun.getAndSet(null);
        if (run != null) {
            run.stopFlag = true;
            run.thread.interrupt();
            log.info("Traffic generator stopped (published {} events this run)", run.emitted);
        }
        return status();
    }

    public TrafficGeneratorStatus status() {
        Run run = currentRun.get();
        if (run == null) {
            return new TrafficGeneratorStatus(false, 0, rateCounter.ratePerSecond(),
                    rateCounter.total(), null, null, List.of());
        }
        return new TrafficGeneratorStatus(
                true,
                run.eventsPerSecond,
                rateCounter.ratePerSecond(),
                rateCounter.total(),
                run.startedAt,
                run.willStopAt,
                run.merchants.stream().map(MerchantId::value).collect(Collectors.toList())
        );
    }

    private void runLoop(Run run) {
        try {
            while (!run.stopFlag && (run.willStopAt == null || Instant.now().isBefore(run.willStopAt))) {
                long elapsedMillis = Duration.between(run.startedAt, Instant.now()).toMillis();
                long targetEmitted = (elapsedMillis * run.eventsPerSecond) / 1000;
                long toEmit = targetEmitted - run.emitted;
                for (long i = 0; i < toEmit && !run.stopFlag; i++) {
                    publishOne(run.merchants);
                    run.emitted++;
                }
                Thread.sleep(TICK_MILLIS);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            currentRun.compareAndSet(run, null);
            log.info("Traffic generator run finished: {} events published", run.emitted);
        }
    }

    private void publishOne(List<MerchantId> merchants) throws InterruptedException {
        MerchantId merchant = merchants.get(ThreadLocalRandom.current().nextInt(merchants.size()));
        PaymentEvent event = new PaymentEvent(
                "evt-" + UUID.randomUUID(),
                "pay-" + UUID.randomUUID(),
                merchant,
                BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(10.0, 5000.0)).setScale(2, java.math.RoundingMode.HALF_UP),
                "USD",
                Instant.now()
        );
        broker.publish(event);
        rateCounter.increment();
    }

    private static final class Run {
        final int eventsPerSecond;
        final List<MerchantId> merchants;
        final Instant startedAt;
        final Instant willStopAt;
        volatile boolean stopFlag = false;
        volatile long emitted = 0;
        Thread thread;

        Run(int eventsPerSecond, List<MerchantId> merchants, Instant startedAt, Instant willStopAt) {
            this.eventsPerSecond = eventsPerSecond;
            this.merchants = merchants;
            this.startedAt = startedAt;
            this.willStopAt = willStopAt;
        }
    }
}

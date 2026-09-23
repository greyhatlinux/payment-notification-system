# Implementation Plan — Payment Notification System

## Stack

- **Java 21** + **Spring Boot 3.3.x**, Maven build (`mvn`, wrapper committed as `mvnw`).
- Single Spring Boot module (`payment-notification-service`), package-per-concern. No microservices — one deployable JVM process, matching "avoid premature microservices."
- No database. In-memory state (bounded), matching "no unnecessary infrastructure."
- Concurrency: Java virtual-thread-friendly executors + bounded thread pools (`java.util.concurrent`), `java.net.http.HttpClient` (async, non-blocking) for merchant calls. No reactive framework (WebFlux) — keeps it readable for an interview; async is achieved via `CompletableFuture` + bounded executors + async HttpClient, not blocking-thread-per-request.
- UI: plain server-rendered static HTML + vanilla JS (no build step, no framework) served from `src/main/resources/static`, polling the REST APIs. Deliberately basic, not flashy, per instructions.
- Packaging/run: `docker compose up` builds and runs the single service; no Kafka/Redis containers required by default (Mock Kafka + in-memory queue are the defaults). An optional profile/adapter point is documented for real Kafka/Redis later, but not wired up.

## Module layout

```
src/main/java/com/paymentnotify/
  domain/          PaymentEvent, NotificationAttempt, MerchantId, FailureReason, enums
  ingestion/        EventSource interface, MockKafkaEventSource, KafkaEventSourceStub(not impl'd), partitioning
  s2/               S2Consumer (per-partition consumer loop), ConsumerGroupCoordinator
  delivery/         C1DeliveryService (sync path), MerchantClient (HTTP), NotificationIdempotency
  merchant/         MerchantSimulator (fake HTTP endpoint via in-process handler), FailureInjectionConfig
  resilience/       CircuitBreaker, CircuitBreakerRegistry, RateLimiter, ConcurrencyLimiter (per-merchant)
  retry/            ScheduledMessageQueue interface, InMemoryScheduledQueue, RetryScheduler, BackoffPolicy
  c2/               C2RetryWorkerPool
  dlq/               DeadLetterStore
  metrics/          MetricsRegistry, per-merchant stats, snapshot DTOs
  api/              REST controllers (metrics, merchants, traffic, failure-injection, retry-queue, dlq, circuit-breakers)
  config/           application properties binding, executor beans
resources/
  static/           index.html, app.js, style.css (dashboard)
  application.yml
test/java/...       unit + a few integration tests (bounded scope per AGENTS.md test list)
```

## Key interfaces (extension points called out in AGENTS.md)

- `EventSource` — `MockKafkaEventSource` now; `KafkaEventSource` left as a documented stub/interface only (not implemented, per "Do not implement S1"... actually Kafka consumption itself is S2's job and *is* in scope, but we only ship the mock; real Kafka adapter is future work, described in README).
- `ScheduledMessageQueue` — `InMemoryScheduledQueue` (delay queue) now; Redis ZSET adapter is a documented future implementation point, not built.
- `MerchantClient` talks to `MerchantSimulator` over real loopback HTTP (localhost) using `HttpClient`, so the circuit breaker/timeout/pool code is exercised realistically, while "merchant" behavior (latency, failure %, status code) is fully configurable at runtime via API.

## Phases (compile + test after each)

1. **Project structure & domain models** — Maven project, Spring Boot app skeleton, `PaymentEvent`, `NotificationAttempt`, enums, config properties skeleton. Build passes, app boots.
2. **Mock Kafka** — `EventSource`/`KafkaMessage` abstraction, `MockKafkaEventSource` with N partitions (paymentId hash → partition), per-partition offset counters, ordering guarantee, a configurable synthetic producer (events/sec, merchant distribution), bounded per-partition buffer for backpressure. Unit tests: ordering, partitioning, offset monotonicity, backpressure.
3. **S2 consumer + synchronous C1** — one consumer loop per partition (bounded pool, not unbounded threads), reads events in order, calls `C1DeliveryService` synchronously; `MerchantClient` stub returns success initially. Test: successful C1 delivery, ordering preserved end-to-end.
4. **Merchant simulator + failure injection** — in-process HTTP server (Spring's own embedded server on a second port, or an in-JVM handler) simulating merchants with configurable success/4xx/429/500/503/timeout/latency/failure-%. Test: failure injection produces expected status/latency distribution.
5. **Circuit breakers + rate/concurrency limits** — per-merchant `CircuitBreaker` (CLOSED/OPEN/HALF_OPEN), `RateLimiter`, `ConcurrencyLimiter`, shared registry keyed by merchantId, used by both C1 and C2 via the same `MerchantClient`. Tests: opens on threshold, blocks calls while OPEN, half-open recovery, merchant isolation.
6. **Scheduled retry queue + scheduler + backoff/jitter** — `ScheduledMessageQueue` interface + `InMemoryScheduledQueue` (delay-ordered), `BackoffPolicy` (exponential + jitter, configurable), `RetryScheduler` polling loop. C1 failures (transient only, per classification config) enqueue retry messages. Tests: backoff progression, jitter bounds, failure classification (retryable vs permanent).
7. **C2 retry workers** — bounded worker pool pulling due messages, calling the same `MerchantClient`/circuit breaker path; success removes from queue, failure reschedules (same queue, incremented attempt) or routes to DLQ if attempts exhausted. Tests: C2 success removes retry, C2 failure reschedules, retry limit reached.
8. **DLQ** — bounded `DeadLetterStore` (in-memory, capped with eviction), attempt history, failure reason. Test: retry limit → DLQ.
9. **Metrics/observability APIs** — `MetricsRegistry` aggregating ingestion/processing/delivery rates, per-merchant stats, circuit breaker states, queue depth, consumer lag, DLQ count; REST endpoints. Lightweight counters (atomics + small ring buffers), bounded recent-history list.
10. **UI dashboard** — static HTML/JS pages: System Overview, Merchant Health, Traffic Generator controls, Failure Injection controls, Retry Queue view, DLQ view — polling the REST APIs every ~1–2s. Deliberately plain styling.
11. **Integration tests & failure scenarios** — a handful of end-to-end tests wiring real components (mock Kafka → S2 → C1 → simulated merchant → retry → circuit breaker → DLQ) using small, fast timings (ms-scale backoff config for tests). Covers the numbered list in AGENTS.md without redundant tests.
12. **Docker Compose + README + e2e validation** — Dockerfile (multi-stage Maven build), `docker-compose.yml` (single service, port for app+UI), README with architecture diagram (text), run instructions, and how mock Kafka/in-memory queue map to real Kafka/Redis. Run a real end-to-end demo locally: start app, drive traffic generator toward high events/sec, inject merchant failures, observe retry → circuit breaker → recovery → DLQ via API/UI, capture results in README.

## Scale note

100K events/sec is a *design target* for the mock producer/consumer pipeline (in-memory, partitioned, non-blocking), not a claim that the simulated merchant HTTP layer will sustain 100K real loopback calls/sec on a laptop. The demo will run mock ingestion at high configurable rates and merchant calls at a realistic bounded concurrency, exactly as AGENTS.md specifies.

## Risks / notes (no blocking ambiguity — proceeding)

- Embedded second HTTP server for the merchant simulator adds a bit of Spring wiring; alternative is an in-process method call behind an interface shaped like an HTTP client (same latency/failure injection semantics, no real socket). I'll use a real loopback HTTP call via a lightweight embedded Jetty/Undertow-free approach (`com.sun.net.httpserver.HttpServer`, JDK-builtin) so `MerchantClient` genuinely exercises HTTP timeouts/connection pooling without pulling in a second servlet container. This keeps things simple and dependency-free.
- Java 21 virtual threads are a good fit for the bounded S2 consumer/C2 worker loops but I will still cap concurrency explicitly (semaphores / fixed executor sizes) rather than relying on virtual threads alone for boundedness, per "no unbounded threads."

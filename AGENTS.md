# Payment Notification Service — Agent Instructions

## Mission

Build a production-quality payment notification platform demonstrating a synchronous-first notification architecture with asynchronous retry/recovery.

The system starts AFTER S1 has successfully published payment events to Kafka.

Do NOT implement S1 or bank integration.

Primary target workload: 100,000 payment events/sec.

The application must be runnable locally without requiring Kafka installation.

---

## Core Architecture

Normal path:

Kafka
↓
S2 Kafka Consumer
↓
C1 Immediate Delivery
↓
MerchantClient
↓
Circuit Breaker
↓
Merchant

Failure path:

C1
↓
Scheduled Retry Queue
↓
Retry Scheduler
↓
C2 Worker Pool
↓
MerchantClient
↓
Circuit Breaker
↓
Merchant

If C2 fails:

C2
↓
same Scheduled Retry Queue
↓
future retry

After maximum retry attempts:

Scheduled Retry Queue
↓
DLQ

---

## Critical Design Decisions

### 1. Synchronous-first

Payment confirmation is latency-sensitive.

Successful notifications MUST attempt immediate synchronous delivery.

Do not make asynchronous retry the default path.

Normal flow:

Kafka → S2 → C1 → Merchant

Only failed deliveries enter the retry system.

### 2. Kafka is the source

Assume S1 guarantees that payment events are successfully published to Kafka.

Do not implement an outbox pattern or S1 logic.

S2 begins at Kafka.

### 3. At-least-once delivery

Do not claim exactly-once HTTP delivery.

Use stable eventId/paymentId and idempotency semantics.

A merchant may receive the same notification more than once.

### 4. Merchant isolation

Circuit breakers, rate limits and concurrency limits must be merchant-specific.

One unhealthy merchant must not affect other merchants.

Example:

Amazon DOWN must not stop Flipkart notifications.

### 5. Circuit breaker

C1 and C2 MUST use the same merchant-specific circuit breaker state.

Conceptually:

C1 ─┐
├── MerchantClient → CircuitBreaker → Merchant
C2 ─┘

States:

CLOSED → OPEN → HALF_OPEN → CLOSED

When OPEN, do not make HTTP calls.

Instead schedule the notification for retry.

### 6. Scheduled retry queue

Every retry message has:

* eventId
* paymentId
* merchantId
* attempt
* nextAttemptAt
* payload

nextAttemptAt MUST include jitter.

Example:

baseDelay + randomJitter

Avoid retry storms.

Use exponential backoff.

Example policy:

attempt 1: ~5 sec
attempt 2: ~30 sec
attempt 3: ~2 min
attempt 4: ~10 min
attempt 5: ~30 min
attempt 6: ~2 hours

Make this configurable.

### 7. Retry queue implementation

The local/default implementation should be in-memory and require no external infrastructure.

Use a time-ordered / priority-based queue.

Abstract it:

ScheduledMessageQueue
├── InMemoryScheduledQueue
└── future Redis implementation point

Do not hardcode the rest of S2 to the in-memory implementation.

### 8. Kafka implementation

Abstract event ingestion:

EventSource
├── MockKafkaEventSource
└── KafkaEventSource

Default local mode:

MockKafkaEventSource

Real Kafka should be an optional adapter.

The evaluator should NOT need to install Kafka.

### 9. Mock Kafka

Mock Kafka should model the important Kafka semantics:

* partitions
* ordering within a partition
* consumer group style processing
* offsets
* configurable event generation rate
* backpressure

Partition events using paymentId so events for one payment remain ordered.

Do NOT attempt to implement the Kafka protocol.

This is a behavioral emulator only.

### 10. Scale

Design target:

100,000 events/sec.

Do not claim the developer laptop can necessarily sustain 100K real external HTTP requests/sec.

The simulator should be able to generate a configurable synthetic workload.

Architecture must support horizontal scaling conceptually.

Avoid unbounded threads, queues, memory or HTTP connections.

Use bounded concurrency and asynchronous HTTP I/O where appropriate.

### 11. Backpressure

Never create an unbounded in-memory queue.

Kafka should act as the durable input buffer.

Retry scheduling must also have bounded worker concurrency.

### 12. Merchant HTTP

Merchant calls are synchronous HTTP calls.

Configure:

* connection timeout
* request timeout
* connection pool
* maximum concurrency
* rate limit

Use asynchronous/non-blocking HTTP where practical.

Do not create one dedicated thread per request.

### 13. Failure classification

Retry transient failures:

* timeout
* connection failure
* HTTP 408
* HTTP 429
* HTTP 5xx

Do not automatically retry permanent failures:

* HTTP 400
* HTTP 401
* HTTP 403
* HTTP 404

Make classification configurable.

### 14. DLQ

After maximum attempts, move the notification to a dead-letter representation.

Expose DLQ metrics and UI visibility.

### 15. Observability

Expose:

* ingestion rate
* processing rate
* successful deliveries
* failed deliveries
* retry count
* DLQ count
* retry queue size
* scheduled queue depth
* Kafka/mock consumer lag
* per-merchant success rate
* per-merchant latency
* per-merchant failure rate
* circuit breaker state
* active worker count

Keep metrics lightweight.

For the demo, retain only a bounded recent notification history.

Do not write every successful event to a relational DB.

---

# UI Requirements

Build a simple but polished dashboard.

The UI must allow an evaluator to understand the system without reading source code.

Dashboard sections:

## System Overview

Show:

* current events/sec
* processing/sec
* delivery/sec
* success rate
* retry rate
* DLQ count
* scheduled queue depth
* consumer lag

## Merchant Health

For each merchant:

* status
* request rate
* success rate
* failure rate
* latency
* circuit breaker state
* active requests

## Traffic Generator

Allow:

* events/sec
* duration
* merchant selection
* start
* stop

## Failure Injection

For each merchant allow:

* success
* 4xx
* 429
* 500
* 503
* timeout
* configurable latency
* configurable failure percentage

The evaluator should be able to make a merchant fail and immediately observe:

C1 failure
→ scheduled queue
→ retry
→ circuit breaker
→ recovery

## Retry Queue

Display:

* pending messages
* next attempt time
* attempt number
* merchant
* payment ID

## DLQ

Display:

* event
* merchant
* attempt count
* failure reason

---

# API Requirements

Provide APIs for:

* system metrics
* merchant metrics
* merchant configuration
* traffic generation
* failure injection
* retry queue inspection
* DLQ inspection
* circuit breaker state

Keep APIs simple and RESTful.

---

# Testing Requirements

Write tests for:

1. successful C1 delivery
2. C1 failure schedules retry
3. C2 success removes retry
4. C2 failure reschedules same message
5. exponential backoff
6. jitter
7. circuit breaker opens
8. circuit breaker prevents calls while OPEN
9. HALF_OPEN recovery
10. merchant isolation
11. retry limit → DLQ
12. payment ordering
13. bounded concurrency
14. backpressure
15. duplicate/idempotent event handling

Do not write meaningless tests merely to increase coverage.

---

# Engineering Principles

Prefer:

* simple abstractions
* composition over inheritance
* immutable event models
* bounded resources
* dependency injection
* configuration over constants
* clear failure handling
* thread safety
* deterministic tests

Avoid:

* premature microservices
* unnecessary databases
* unnecessary infrastructure
* over-engineering
* synchronous blocking worker pools
* global circuit breakers
* global rate limits
* unbounded queues
* pretending the mock Kafka is real Kafka

---

# Execution Instructions

Before coding:

1. Inspect the repository.
2. Identify existing technology choices.
3. Create a concise implementation plan.
4. Identify architectural risks.
5. Ask only if a genuinely blocking ambiguity exists.

Then implement incrementally.

After each major phase:

1. compile
2. run tests
3. fix failures
4. continue

Do not stop after creating scaffolding.

The final result must be runnable.

Required local experience:

docker compose up

Then evaluator should be able to open the UI and exercise the complete flow.

---

# Definition of Done

The project is complete only when:

* backend starts successfully
* frontend starts successfully
* no Kafka installation is required
* mock Kafka generates events
* C1 immediately calls merchants
* failures enter scheduled retry queue
* retries execute at nextAttemptAt
* jitter is applied
* C2 failures return to the same queue
* circuit breakers work
* merchant isolation works
* retry limits produce DLQ
* metrics are visible in UI
* failure injection is visible in UI
* automated tests pass
* README explains architecture and how to run
* architecture documentation explains how mock infrastructure maps to production Kafka/Redis
* project can be demonstrated end-to-end locally

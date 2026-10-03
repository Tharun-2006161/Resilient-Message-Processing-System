# 🛰 Galactic Message Relay — Resilient Message Processing System

A production-grade, resilient asynchronous message processing system built with **Node.js**, **RabbitMQ**, and **Redis**. The system implements critical distributed systems patterns including **message queuing**, **exactly-once idempotency**, **automated retries with backoff**, and **dead-letter (poison) queues** — all fully containerized with Docker Compose.

---

## 🏗 Architecture

```
┌──────────────┐    POST /command    ┌──────────────┐    Publish     ┌──────────────────┐
│              │ ──────────────────► │              │ ─────────────► │                  │
│   Client     │    202 Accepted     │   Producer   │                │   RabbitMQ       │
│              │ ◄────────────────── │   (Express)  │   GET /status  │   (Broker)       │
│              │                     │   :3000      │ ◄────────────► │                  │
└──────────────┘                     └──────────────┘                └────────┬─────────┘
                                                                              │
                                                                     Deliver (prefetch=1)
                                                                              │
                                                                     ┌────────▼─────────┐
                                                                     │                  │
                                                                     │   Consumer       │
                                                                     │   (Worker)       │
                                                                     │                  │
                                                                     └────────┬─────────┘
                                                                              │
                                                                     Check / Save ID
                                                                              │
                                                                     ┌────────▼─────────┐
                                                                     │                  │
                                                                     │   Redis          │
                                                                     │   (State Store)  │
                                                                     │                  │
                                                                     └──────────────────┘
```

### Components

| Service       | Technology     | Role                                              |
|---------------|----------------|----------------------------------------------------|
| **Producer**  | Express.js     | HTTP API — validates & enqueues commands            |
| **Consumer**  | Node.js        | Background worker — processes messages              |
| **Broker**    | RabbitMQ 3.12  | Message queue with DLX for poison routing           |
| **State Store** | Redis 7      | Idempotency (SETNX) & retry count tracking (INCR)  |

### Resilience Patterns Implemented

| Pattern                    | Implementation Detail                                      |
|----------------------------|------------------------------------------------------------|
| **Asynchronous Decoupling** | Producer publishes to RabbitMQ, responds `202` immediately |
| **Exactly-Once Processing** | Redis `SETNX` on `message_id` before business logic       |
| **Manual ACK/NACK**        | Auto-ack disabled; ACK only after state store write        |
| **Retry with Backoff**     | Redis `INCR` tracks attempts; 500ms delay between retries |
| **Dead-Letter Queue**      | After 3 retries → message routed to `poison_queue`         |
| **Atomic State Updates**   | `SETNX` prevents race-condition duplicates                 |

---

## 🚀 Quick Start

### Prerequisites

- **Docker** ≥ 20.x and **Docker Compose** ≥ 2.x
- Ports `3000`, `5672`, `15672`, `6379` available on the host

### 1. Clone & Configure

```bash
git clone <repository-url>
cd <project-directory>

# Copy the example env file (defaults work out of the box)
cp .env.example .env
```

### 2. Start the System

```bash
docker-compose up --build
```

> ⏳ On first run, Docker will pull images and build containers. The Producer and Consumer wait for RabbitMQ and Redis health checks to pass before starting.

You'll see output like:
```
galactic-producer  | 🚀  Galactic Producer API listening on port 3000
galactic-consumer  | 🛰  Galactic Consumer Worker online — listening on [primary_queue]
```

### 3. Stop the System

```bash
docker-compose down           # Stop and remove containers
docker-compose down -v        # Also remove data volumes
```

---

## 📡 API Reference

### `POST /command` — Enqueue a Mission Command

**Request:**
```json
{
  "message_id": "mission-alpha-001",
  "payload": "Deploy satellite to orbit L2",
  "simulate_error": false
}
```

| Field            | Type    | Required | Description                              |
|------------------|---------|----------|------------------------------------------|
| `message_id`     | string  | ✅       | Unique identifier for idempotency        |
| `payload`        | string  | ✅       | The mission command data                 |
| `simulate_error` | boolean | ❌       | If `true`, forces a processing failure   |

**Success Response:** `202 Accepted`
```json
{
  "status": "queued",
  "message_id": "mission-alpha-001"
}
```

**Validation Error:** `400 Bad Request`
```json
{
  "error": "Bad Request",
  "detail": "Both 'message_id' (string) and 'payload' (string) are required."
}
```

---

### `GET /status` — Inspect Queue Depths

**Response:** `200 OK`
```json
{
  "primary_queue_count": 0,
  "poison_queue_count": 1
}
```

---

### `GET /health` — Health Check

**Response:** `200 OK`
```json
{
  "status": "ok"
}
```

---

## ✅ Verification Guide

### 1. Normal Message Processing

Send a unique message and verify it's processed exactly once:

```bash
# Enqueue a command
curl -X POST http://localhost:3000/command \
  -H "Content-Type: application/json" \
  -d '{"message_id": "test-001", "payload": "Launch probe to Mars"}'

# Expected: {"status":"queued","message_id":"test-001"}
```

Check the Consumer logs — you should see:
```
[consumer] ✔ Successfully processed message_id=test-001
```

### 2. Idempotency (Duplicate Prevention)

Send the **same** `message_id` again:

```bash
curl -X POST http://localhost:3000/command \
  -H "Content-Type: application/json" \
  -d '{"message_id": "test-001", "payload": "Launch probe to Mars"}'
```

Check the Consumer logs — you should see:
```
[consumer] ⚠ Duplicate ignored (message_id=test-001)
```

The message is acknowledged without re-processing.

### 3. Retry Mechanism & Poison Queue

Send a message with `simulate_error: true`:

```bash
curl -X POST http://localhost:3000/command \
  -H "Content-Type: application/json" \
  -d '{"message_id": "error-001", "payload": "Corrupt data", "simulate_error": true}'
```

Watch the Consumer logs — you'll see 4 attempts (1 initial + 3 retries):
```
[consumer] ▶ Received message_id=error-001  attempt=1/4
[consumer] ✘ Processing failed (attempt 1): Simulated transient failure
[consumer] ↻ Retry 1/3 — requeuing after 500ms delay

[consumer] ▶ Received message_id=error-001  attempt=2/4
[consumer] ✘ Processing failed (attempt 2): Simulated transient failure
[consumer] ↻ Retry 2/3 — requeuing after 500ms delay

[consumer] ▶ Received message_id=error-001  attempt=3/4
[consumer] ✘ Processing failed (attempt 3): Simulated transient failure
[consumer] ↻ Retry 3/3 — requeuing after 500ms delay

[consumer] ▶ Received message_id=error-001  attempt=4/4
[consumer] ✘ Processing failed (attempt 4): Simulated transient failure
[consumer] 💀 Max retries (3) exceeded — routing to poison queue
```

### 4. Verify Queue Depths

```bash
curl http://localhost:3000/status
# Expected: {"primary_queue_count":0,"poison_queue_count":1}
```

### 5. Validation Errors

```bash
# Missing payload
curl -X POST http://localhost:3000/command \
  -H "Content-Type: application/json" \
  -d '{"message_id": "test-bad"}'

# Expected: 400 Bad Request

# Missing message_id
curl -X POST http://localhost:3000/command \
  -H "Content-Type: application/json" \
  -d '{"payload": "some data"}'

# Expected: 400 Bad Request
```

---

## 🔧 Configuration

All configuration is via environment variables (see [`.env.example`](.env.example)):

| Variable           | Default         | Description                        |
|---------------------|-----------------|------------------------------------|
| `RABBITMQ_USER`     | `galaxy`        | RabbitMQ username                  |
| `RABBITMQ_PASS`     | `starfleet`     | RabbitMQ password                  |
| `RABBITMQ_PORT`     | `5672`          | RabbitMQ AMQP port                 |
| `RABBITMQ_MGMT_PORT`| `15672`        | RabbitMQ Management UI port        |
| `REDIS_PORT`        | `6379`          | Redis port                         |
| `PRODUCER_PORT`     | `3000`          | Producer API host port             |
| `PRIMARY_QUEUE`     | `primary_queue` | Name of the primary message queue  |
| `POISON_QUEUE`      | `poison_queue`  | Name of the dead-letter queue      |
| `MAX_RETRIES`       | `3`             | Max retry attempts before DLQ      |

---

## 📁 Project Structure

```
project-root/
├── producer/
│   ├── src/
│   │   ├── index.js          # Express API server (POST /command, GET /status)
│   │   └── broker.js         # RabbitMQ connection & topology declaration
│   ├── Dockerfile
│   ├── .dockerignore
│   └── package.json
├── consumer/
│   ├── src/
│   │   ├── index.js          # Consumer worker (idempotency, retries, DLQ)
│   │   ├── broker.js         # RabbitMQ connection & topology declaration
│   │   └── stateStore.js     # Redis client (SETNX idempotency, INCR retries)
│   ├── Dockerfile
│   ├── .dockerignore
│   └── package.json
├── docker-compose.yml         # Full orchestration (4 services)
├── .env.example               # Environment variable documentation
├── .env                       # Active environment (gitignored)
└── README.md                  # This file
```

---

## 🛡 Resilience Deep Dive

### Why Redis SETNX for Idempotency?

```
Consumer A receives message_id=123  ─┐
                                      ├─► Redis SETNX("123") ─► Only ONE wins
Consumer B receives message_id=123  ─┘
```

`SETNX` (Set if Not eXists) is an **atomic** Redis operation. Even if two consumers race on the same `message_id`, only one will succeed. The loser gets `0` and drops the message — no duplicate processing.

### Why Track Retries in Redis (Not RabbitMQ Headers)?

RabbitMQ's `x-death` headers are only populated when a message is **dead-lettered**, not when it's **requeued** via `nack(msg, false, true)`. Since requeuing is simpler and doesn't require a separate retry exchange, we track attempt counts in Redis using `INCR` — which is also atomic and race-safe.

### Order of Operations (Crash Safety)

```
1. Check idempotency  →  Is message_id already in Redis?
2. Execute business logic
3. SETNX message_id   →  Mark as processed (atomic)
4. ACK to broker      →  Message removed from queue
```

If the consumer crashes between steps 3 and 4:
- The broker re-delivers the message (it was never ACKed)
- Step 1 catches the duplicate (message_id already in Redis)
- No double-processing occurs ✔

---

## 🔗 External Resources

- [RabbitMQ Dead Letter Exchanges](https://www.rabbitmq.com/dlx.html)
- [Redis SETNX Documentation](https://redis.io/commands/setnx/)
- [Microservice Messaging Patterns](https://microservices.io/patterns/communication-style/messaging.html)

---

## 📄 License

MIT

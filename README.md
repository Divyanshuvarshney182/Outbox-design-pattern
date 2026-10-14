# 📦 Transactional Outbox Pattern — Microservices Demo

A production-ready demonstration of the **Transactional Outbox Pattern** using two Spring Boot microservices communicating reliably through Apache Kafka.

---

## 📋 Table of Contents

- [What is the Transactional Outbox Pattern?](#-what-is-the-transactional-outbox-pattern)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Tech Stack](#-tech-stack)
- [Getting Started](#-getting-started)
- [API Reference](#-api-reference)
- [Event Flow Walkthrough](#-event-flow-walkthrough)
- [Key Design Decisions](#-key-design-decisions)
- [Configuration](#-configuration)
- [Monitoring](#-monitoring)
- [Failure Scenarios](#-failure-scenarios)

---

## 💡 What is the Transactional Outbox Pattern?

In a microservices architecture, a common problem arises when you need to **update your database AND publish a message to a broker** (like Kafka) in the same operation. If you do them separately, you risk:

- ✅ Database saved → ❌ Kafka publish crashes → **Event is lost forever**
- ❌ Database rollback → ✅ Kafka message sent → **Ghost event with no matching data**

The **Transactional Outbox Pattern** solves this by writing the event to an `outbox_events` table **in the same database transaction** as your business data. A background scheduler then reads from the outbox and publishes to Kafka — guaranteeing **at-least-once delivery** without distributed transactions.

```
❌ The problem:
   DB.save(order)         ─┐ Two separate operations
   Kafka.publish(event)   ─┘ Either can fail independently

✅ The solution:
   BEGIN TRANSACTION
     DB.save(order)              ─┐ One atomic operation
     DB.save(outbox_event)       ─┘ Both commit or both rollback
   COMMIT

   [Scheduler] poll outbox → publish to Kafka → mark PROCESSED
```

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            CLIENT REQUEST                                   │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │  POST /api/orders
                                     ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                        ORDER SERVICE  :8081                                │
│                                                                            │
│   OrderController                                                          │
│        │                                                                   │
│        ▼                                                                   │
│   OrderService.createOrder()  ◄── @Transactional                           │
│        │                                                                   │
│        ├── 1. INSERT INTO orders (status = PENDING)                        │
│        └── 2. INSERT INTO outbox_events (status = PENDING)  ◄── same TX    │
│                                                                            │
│   OutboxPublisherScheduler  (every 5s)                                     │
│        ├── 3. SELECT outbox_events WHERE status = PENDING  FOR UPDATE      │
│        ├── 4. KafkaPublisher.publish()  ──────────────────────────────┐    │
│        └── 5. UPDATE outbox_events SET status = PROCESSED             │    │
└───────────────────────────────────────────────────────────────────────┼─── ┘
                                                                         │
                                              Kafka Topic: order.created │
                                                                         │
┌─────────────────────────────────────────────────────────────────────── ┼───┐
│                      INVENTORY SERVICE  :8082                          │   │
│                                                                        ▼   │
│   OrderEventConsumer                                                       │
│        │                                                                   │
│        ▼                                                                   │
│   InventoryService.reserveStock()  ◄── @Transactional                      │
│        │                                                                   │
│        ├── 6. SELECT inventory FOR UPDATE (pessimistic lock)               │
│        ├── 7. UPDATE inventory (availableQty -= n, reservedQty += n)       │
│        ├── 8. INSERT INTO inventory_reservations                           │
│        └── 9. INSERT INTO outbox_events (status = PENDING)  ◄── same TX    │
│                                                                            │
│   OutboxPublisherScheduler  (every 5s)                                     │
│        ├── 10. SELECT outbox_events WHERE status = PENDING  FOR UPDATE     │
│        ├── 11. KafkaPublisher.publish()  ─────────────────────────────┐    │
│        └── 12. UPDATE outbox_events SET status = PROCESSED            │    │
└──────────────────────────────────────────────────────────────────────┬┘    │
                                                                        │    │
                                     Kafka Topic: inventory.reserved    │    │
                                                                        │    │
┌───────────────────────────────────────────────────────────────────────┘    │
│                        ORDER SERVICE  :8081  (consumer)                    │
│                                                                            │
│   InventoryEventConsumer                                                   │
│        │                                                                   │
│        ├── 13a. success=true  → UPDATE orders SET status = CONFIRMED       │
│        └── 13b. success=false → UPDATE orders SET status = CANCELLED       │
└────────────────────────────────────────────────────────────────────────────┘
```

### Databases

| Service | Database | Port |
|---|---|---|
| Order Service | `order_db` | `5432` |
| Inventory Service | `inventory_db` | `5433` |

Each service owns its own database — they share **nothing** at the data layer.

---

## 📁 Project Structure

```
transactional-outbox-demo/
│
├── docker-compose.yml                  # PostgreSQL ×2, Kafka, Zookeeper, Kafka UI
│
├── order-service/                      # Microservice 1 — Port 8081
│   ├── pom.xml
│   └── src/main/java/com/example/order/
│       ├── OrderServiceApplication.java
│       ├── config/
│       │   ├── KafkaConfig.java        # Producer (idempotent) + Consumer (manual ack)
│       │   └── JacksonConfig.java
│       ├── model/
│       │   ├── Order.java              # Business entity
│       │   ├── OrderStatus.java        # PENDING → CONFIRMED / CANCELLED
│       │   ├── OutboxEvent.java        # Outbox table entity
│       │   └── OutboxStatus.java       # PENDING → PROCESSING → PROCESSED / FAILED
│       ├── event/
│       │   ├── OrderCreatedEvent.java  # Published to Kafka
│       │   └── InventoryReservedEvent.java  # Consumed from Kafka
│       ├── repository/
│       │   ├── OrderRepository.java
│       │   └── OutboxEventRepository.java   # Pessimistic-lock query
│       ├── service/
│       │   ├── OrderService.java       # createOrder() — core @Transactional method
│       │   ├── OutboxService.java      # MANDATORY propagation — saves event in caller TX
│       │   └── KafkaPublisher.java     # Sends to Kafka with sync confirmation
│       ├── scheduler/
│       │   └── OutboxPublisherScheduler.java  # Polls outbox every 5s
│       ├── consumer/
│       │   └── InventoryEventConsumer.java    # Handles inventory.reserved replies
│       └── controller/
│           ├── OrderController.java
│           └── OutboxController.java    # GET /api/outbox/stats
│
└── inventory-service/                  # Microservice 2 — Port 8082
    ├── pom.xml
    └── src/main/java/com/example/inventory/
        ├── InventoryServiceApplication.java
        ├── config/
        │   ├── KafkaConfig.java
        │   └── JacksonConfig.java
        ├── model/
        │   ├── Inventory.java           # availableQty / reservedQty + domain methods
        │   ├── InventoryReservation.java # Audit record, unique(orderId) = idempotency key
        │   ├── OutboxEvent.java
        │   └── OutboxStatus.java
        ├── event/
        │   ├── OrderCreatedEvent.java   # Consumed from Kafka
        │   └── InventoryReservedEvent.java  # Published to Kafka
        ├── repository/
        │   ├── InventoryRepository.java         # findByProductIdWithLock()
        │   ├── InventoryReservationRepository.java
        │   └── OutboxEventRepository.java
        ├── service/
        │   ├── InventoryService.java    # reserveStock() — core @Transactional method
        │   ├── OutboxService.java
        │   └── KafkaPublisher.java
        ├── scheduler/
        │   └── OutboxPublisherScheduler.java
        ├── consumer/
        │   └── OrderEventConsumer.java  # Handles order.created events
        └── controller/
            ├── InventoryController.java
            └── OutboxController.java
```

---

## 🛠 Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Java | 17 | Language |
| Spring Boot | 3.2.1 | Application framework |
| Spring Data JPA | — | Database access layer |
| Spring Kafka | — | Kafka integration |
| PostgreSQL | 15 | Relational database (one per service) |
| Apache Kafka | 7.5.0 (Confluent) | Message broker |
| Lombok | — | Boilerplate reduction |
| Jackson | — | JSON serialization |
| Docker Compose | — | Local infrastructure |

---

## 🚀 Getting Started

### Prerequisites

- Java 17+
- Maven 3.8+
- Docker & Docker Compose

### 1. Clone the repository

```bash
git clone https://github.com/your-username/transactional-outbox-demo.git
cd transactional-outbox-demo
```

### 2. Start the infrastructure

```bash
docker-compose up -d
```

This starts:
- PostgreSQL for Order Service on port `5432`
- PostgreSQL for Inventory Service on port `5433`
- Zookeeper on port `2181`
- Kafka on port `9092`
- Kafka UI on port `8090` (optional — browse topics at http://localhost:8090)

Wait ~15 seconds for all services to be healthy:

```bash
docker-compose ps
```

### 3. Run the Order Service

```bash
cd order-service
mvn clean spring-boot:run
```

### 4. Run the Inventory Service

Open a new terminal:

```bash
cd inventory-service
mvn clean spring-boot:run
```

### 5. Seed some inventory

```bash
curl -X POST http://localhost:8082/api/inventory \
  -H "Content-Type: application/json" \
  -d '{
    "productId":   "PROD-001",
    "productName": "Laptop",
    "quantity":    10
  }'
```

### 6. Place an order

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId":  "CUST-001",
    "productId":   "PROD-001",
    "productName": "Laptop",
    "quantity":    2,
    "totalAmount": 1999.99
  }'
```

> The order is created with status `PENDING`. Take note of the `id` in the response.

### 7. Watch the pattern work

Wait ~10 seconds, then check the order — it will have moved to `CONFIRMED`:

```bash
curl http://localhost:8081/api/orders/{orderId}
```

Check inventory — available stock decreased, reserved stock increased:

```bash
curl http://localhost:8082/api/inventory/PROD-001
```

---

## 📡 API Reference

### Order Service — `localhost:8081`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/orders` | Place a new order |
| `GET` | `/api/orders` | List all orders |
| `GET` | `/api/orders/{orderId}` | Get a specific order |
| `GET` | `/api/orders/customer/{customerId}` | Get orders by customer |
| `GET` | `/api/outbox/stats` | Outbox event counts by status |
| `GET` | `/actuator/health` | Health check |

**POST /api/orders — Request body:**

```json
{
  "customerId":  "CUST-001",
  "productId":   "PROD-001",
  "productName": "Laptop",
  "quantity":    2,
  "totalAmount": 1999.99
}
```

**Response:**

```json
{
  "id":          "a3f1c2d4-...",
  "customerId":  "CUST-001",
  "productId":   "PROD-001",
  "productName": "Laptop",
  "quantity":    2,
  "totalAmount": 1999.99,
  "status":      "PENDING",
  "createdAt":   "2024-01-15T10:30:00"
}
```

---

### Inventory Service — `localhost:8082`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/inventory` | Add / top-up stock for a product |
| `GET` | `/api/inventory` | List all inventory |
| `GET` | `/api/inventory/{productId}` | Get stock for a product |
| `GET` | `/api/inventory/{productId}/reservations` | Get reservation history |
| `GET` | `/api/outbox/stats` | Outbox event counts by status |
| `GET` | `/actuator/health` | Health check |

**POST /api/inventory — Request body:**

```json
{
  "productId":   "PROD-001",
  "productName": "Laptop",
  "quantity":    10
}
```

**GET /api/inventory/{productId} — Response:**

```json
{
  "id":                "b2e4d6f8-...",
  "productId":         "PROD-001",
  "productName":       "Laptop",
  "availableQuantity": 8,
  "reservedQuantity":  2
}
```

---

## 🔁 Event Flow Walkthrough

### Happy Path — Successful Order

```
1. Client        → POST /api/orders
2. OrderService  → BEGIN TX
                   INSERT orders (status=PENDING)
                   INSERT outbox_events (status=PENDING, topic=order.created)
                   COMMIT
3. Scheduler     → polls outbox (every 5s)
                   UPDATE outbox_events SET status=PROCESSING
                   publish to Kafka: order.created
                   UPDATE outbox_events SET status=PROCESSED

4. Kafka         → delivers to inventory-service-group

5. Consumer      → OrderEventConsumer receives message
6. InventoryService → BEGIN TX
                      SELECT inventory FOR UPDATE
                      UPDATE inventory (available -2, reserved +2)
                      INSERT inventory_reservations (success=true)
                      INSERT outbox_events (status=PENDING, topic=inventory.reserved)
                      COMMIT
                      ACK Kafka offset

7. Scheduler     → polls outbox
                   publish to Kafka: inventory.reserved

8. Consumer      → InventoryEventConsumer receives message
9. OrderService  → UPDATE orders SET status=CONFIRMED
                   ACK Kafka offset
```

### Sad Path — Insufficient Stock

Steps 1–4 are identical. At step 6, the inventory check fails:

```
6. InventoryService → BEGIN TX
                      SELECT inventory FOR UPDATE
                      availableQuantity (3) < requested (10) → FAIL
                      INSERT inventory_reservations (success=false, reason="Insufficient stock")
                      INSERT outbox_events (topic=inventory.reserved, success=false)
                      COMMIT

8. Consumer      → InventoryEventConsumer receives message (success=false)
9. OrderService  → UPDATE orders SET status=CANCELLED
```

---

## 🔑 Key Design Decisions

### 1. `Propagation.MANDATORY` on `OutboxService.save()`

```java
@Transactional(propagation = Propagation.MANDATORY)
public OutboxEvent save(...) { ... }
```

This enforces that `save()` can **only** be called from within an existing transaction. If called without one, Spring throws immediately. This prevents the silent bug where someone calls it outside a transaction and gets zero atomicity guarantees.

### 2. Pessimistic locking on the outbox query

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT e FROM OutboxEvent e WHERE e.status = :status ...")
List<OutboxEvent> findPendingWithLock(...);
```

When running multiple service instances, this ensures two pods cannot pick up the same outbox row simultaneously — no duplicate Kafka messages from the publisher side.

### 3. Pessimistic locking on inventory

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT i FROM Inventory i WHERE i.productId = :productId")
Optional<Inventory> findByProductIdWithLock(String productId);
```

Prevents overselling when two concurrent orders target the same product.

### 4. `unique(orderId)` on `inventory_reservations`

The `orderId` column has a unique constraint. This acts as a **database-level idempotency guard** — if a duplicate `order.created` event is delivered by Kafka, the second reservation attempt fails at the DB constraint before touching inventory.

### 5. Manual Kafka acknowledgment

```java
factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
```

The Kafka offset is only committed **after** the database transaction commits successfully. If the DB write fails, the offset is not committed and Kafka redelivers the message.

### 6. Idempotent Kafka producer

```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.ACKS_CONFIG, "all");
```

Prevents the producer from publishing duplicate messages to Kafka in the event of a network timeout and retry.

---

## ⚙️ Configuration

### Order Service — `application.yml`

```yaml
server:
  port: 8081

spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/order_db

  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: order-service-group

outbox:
  publisher:
    batch-size: 10
    fixed-delay-ms: 5000    # poll every 5 seconds
    initial-delay-ms: 10000 # wait 10s on startup
```

### Inventory Service — `application.yml`

```yaml
server:
  port: 8082

spring:
  datasource:
    url: jdbc:postgresql://localhost:5433/inventory_db  # separate DB

  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: inventory-service-group  # separate consumer group

outbox:
  publisher:
    batch-size: 10
    fixed-delay-ms: 5000
    initial-delay-ms: 10000
```

---

## 📊 Monitoring

### Outbox Statistics

Check how many events are in each state:

```bash
# Order Service outbox
curl http://localhost:8081/api/outbox/stats

# Inventory Service outbox
curl http://localhost:8082/api/outbox/stats
```

```json
{
  "pending":    0,
  "processing": 0,
  "processed":  5,
  "failed":     0
}
```

### Kafka UI

Browse topics, messages, and consumer groups at:

```
http://localhost:8090
```

Topics to watch:
- `order.created` — published by Order Service, consumed by Inventory Service
- `inventory.reserved` — published by Inventory Service, consumed by Order Service

### Actuator Health

```bash
curl http://localhost:8081/actuator/health
curl http://localhost:8082/actuator/health
```

---

## 🔥 Failure Scenarios

### Scenario 1 — Kafka is down when order is placed

The order and outbox event are written to the DB atomically. The outbox scheduler will keep retrying (every 5s, up to 3 times) until Kafka comes back up. No events are lost.

```bash
# Simulate by stopping Kafka
docker stop kafka

# Place an order — it saves to DB successfully
curl -X POST http://localhost:8081/api/orders ...

# Outbox stats show pending = 1
curl http://localhost:8081/api/outbox/stats

# Restart Kafka — event publishes automatically within 5s
docker start kafka
```

### Scenario 2 — Service crashes after DB commit but before Kafka publish

The outbox event row exists in the DB with status `PENDING`. When the service restarts, the scheduler picks it up on the next poll and publishes it. The event is never lost.

### Scenario 3 — Duplicate Kafka delivery (at-least-once)

Kafka may redeliver a message that was already processed (e.g., if the consumer crashed before committing the offset). Both services guard against this:

- **Order Service** keeps an in-memory set of processed `eventId`s
- **Inventory Service** has a `UNIQUE` constraint on `inventory_reservations.order_id`

In production, replace the in-memory set with a database table for durability across restarts.

### Scenario 4 — Insufficient stock

```bash
# Seed only 3 units
curl -X POST http://localhost:8082/api/inventory \
  -d '{"productId":"PROD-002","productName":"GPU","quantity":3}'

# Try to order 10
curl -X POST http://localhost:8081/api/orders \
  -d '{"productId":"PROD-002","quantity":10,...}'

# After ~10s — order status will be CANCELLED
curl http://localhost:8081/api/orders/{orderId}
```

---

## 📝 License

MIT License — feel free to use this as a reference implementation.

---

## 🤝 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

---

> **Note:** The in-memory idempotency set in the Kafka consumers is intentionally simplified for demo purposes. In a production system, replace it with a persistent store (Redis or a `processed_events` DB table) so idempotency survives service restarts.

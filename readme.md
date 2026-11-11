# Event-Driven Payment Processing Microservice

An asynchronous payment processing microservice built with **Spring Boot 4**, **Apache Kafka**, and **PostgreSQL**. Payment records are stored in PostgreSQL, while Kafka messages trigger asynchronous processing and communicate status snapshots. The database stores the current payment state; this implementation does not use event sourcing.

---

## Architecture & Event Flow

The service publishes a `Payment` payload to Kafka after saving a new payment as `PENDING`. A Kafka consumer simulates processing, updates the record to `COMPLETED` in PostgreSQL, and publishes the updated `Payment` payload to the same topic. The database write and Kafka publish are separate operations and are not coordinated by an outbox or a distributed transaction, so the system does not guarantee atomic consistency between them.

```
 [ Client / Postman ]
          │
          ▼
1. POST /api/payments
          │
          ▼
   [ Payment REST API ] ──(Save PENDING)──► [ PostgreSQL DB ]
          │
          └── 2. Publish Payment payload ("PENDING")
          │
          ▼
   [ Kafka Topic: payment-events ]
          │
          ▼
  3. [ PaymentConsumer ]
          │
          ├── (Simulate processing / gateway integration)
          ├── 4. Update status to "COMPLETED" ──► [ PostgreSQL DB ]
          └── 5. Publish updated Payment payload ("COMPLETED")
                  │
                  ▼
         [ Kafka Topic: payment-events ] ──► (Other topic consumers, if configured)
```

1. **Transaction Initiation**: A `POST` request creates a new payment record with a `PENDING` status in PostgreSQL.
2. **Initial Message (`PENDING`)**: The producer sends the saved `Payment` object to the `payment-events` topic.
3. **Message Consumption**: `PaymentConsumer` receives the payment, ignores it if it is already `COMPLETED`, and simulates processing.
4. **State Transition**: The consumer updates the database record status to `COMPLETED`.
5. **Updated Message (`COMPLETED`)**: The consumer sends the updated `Payment` object to the same topic. Since the consumer ignores completed payments, this message does not trigger another state transition in this consumer.

---

## Tech Stack

* **Java**: 17+
* **Framework**: Spring Boot 4.x (Spring Web, Spring Data JPA, Spring for Apache Kafka)
* **Database**: PostgreSQL 15
* **Messaging**: Apache Kafka & Zookeeper
* **Monitoring**: Kafka UI (`provectuslabs/kafka-ui`)
* **Containerization**: Docker & Docker Compose
* **Build Tool**: Maven

---

## Getting Started

### Prerequisites

Ensure you have the following installed locally:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (including Docker Compose)
* [JDK 17+](https://www.oracle.com/java/technologies/downloads/)
* [Maven](https://maven.apache.org/)

---

### Infrastructure Setup

1. **Clone the repository:**
   ```bash
   gh repo clone arieelvc/payment-backend
   cd payment-backend
   ```

2. **Start the environment with Docker Compose:**
   ```bash
   docker compose up -d
   ```

   This spins up the following services:
   * **PostgreSQL** on port `5433` (host; container port `5432`)
   * **Zookeeper** on port `2181`
   * **Kafka Broker** on ports `9092` (host) and `29092` (internal network)
   * **Kafka UI** on port `8081`

3. **Verify running containers:**
   ```bash
   docker ps
   ```

---

### Application Configuration

The application configuration in `src/main/resources/application.yml` uses the following settings:

```yaml
server:
  port: 8080

spring:
  datasource:
    url: jdbc:postgresql://localhost:5433/payments_db
    username: portfolio
    password: portfolio
    driver-class-name: org.postgresql.Driver

  jpa:
    hibernate:
      ddl-auto: update
    show-sql: true
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect

  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: payment-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
```

The database credentials shown above are for local development and match the Docker Compose defaults. Use externally managed credentials in production.

---

### Running the Application

Build and start the Spring Boot service:

```bash
mvn clean spring-boot:run
```

The application will launch on `http://localhost:8080`.

### Swagger UI

Open the interactive API documentation at:

```
http://localhost:8080/swagger-ui/index.html
```

The generated OpenAPI specification is available at:

```
http://localhost:8080/v3/api-docs
```

---

##  API Endpoints & Usage

### 1. Create a Payment

* **Endpoint**: `POST /api/payments`
* **Content-Type**: `application/json`

**Sample Request Body:**
```json
{
  "transactionId": "TXN-10023",
  "amount": 150.50,
  "currency": "USD"
}
```

**Sample Response ( `201 created`):**
```json
{
  "id": 1,
  "transactionId": "TXN-10023",
  "amount": 150.50,
  "currency": "USD",
  "status": "PENDING",
  "createdAt": "2026-10-29T21:09:48.835524600"
}
```

---

## 🔍 Monitoring & Verification

### Kafka UI
Open your browser and navigate to:
```
http://localhost:8081
```
* Go to **Topics** > `payment-events` > **Messages**.
* You will observe two distinct messages per payment:
  1. Status: `"PENDING"`
  2. Status: `"COMPLETED"` (emitted ~2 seconds after processing).

### Kafka CLI Consumer
Alternatively, consume events directly via the Kafka Docker container:

```bash
docker exec -it kafka /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic payment-events --from-beginning
```

---

## Project Structure

```
src/main/java/com/portfolio/payment_backend
├── config/             # Kafka producer and consumer configurations
├── controller/         # REST Controllers handling incoming payments
├── dto/                # Data Transfer Objects
├── model/              # JPA Entities (Payment)
├── kafka/
    ├── producer/           # Kafka Producer publishing payment events
    ├── consumer/           # Kafka Consumer processing async workflows
├── repository/         # Spring Data JPA Repositories
└── service/            # Core business logic
```

---

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request
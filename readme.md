# Event-Driven Payment Processing Microservice

An asynchronous, event-driven payment processing microservice built with **Spring Boot 3**, **Apache Kafka**, and **PostgreSQL**. The service leverages event sourcing principles to track the entire payment lifecycle—from creation (`PENDING`) to final fulfillment (`COMPLETED`)—ensuring full traceability and auditability.

---

## 🏗️ Architecture & Event Flow

The system employs a dual-event lifecycle to maintain continuous alignment between database state and Kafka topics without transactional mismatches.

```
 [ Client / Postman ]
          │
          ▼
1. POST /payments
          │
          ▼
   [ Payment REST API ] ──(Save PENDING)──► [ PostgreSQL DB ]
          │
          ├── 2. Publish PaymentCreatedEvent ("PENDING")
          │
          ▼
   [ Kafka Topic: payment-events ]
          │
          ▼
  3. [ PaymentConsumer ]
          │
          ├── (Simulate processing / gateway integration)
          ├── 4. Update status to "COMPLETED" ──► [ PostgreSQL DB ]
          │
          └── 5. Publish PaymentCompletedEvent ("COMPLETED")
                  │
                  ▼
         [ Kafka Topic: payment-events ] ──► (External Consumers / Audit Services)
```

1. **Transaction Initiation**: A `POST` request creates a new payment record with a `PENDING` status in PostgreSQL.
2. **First Event (`PENDING`)**: The producer emits an initial event to the `payment-events` topic.
3. **Event Consumption**: `PaymentConsumer` picks up the `PENDING` event and simulates transaction processing.
4. **State Transition**: The consumer updates the database record status to `COMPLETED`.
5. **Second Event (`COMPLETED`)**: The consumer publishes a follow-up event to notify downstream consumers that the payment was successfully processed.

---

## 🧰 Tech Stack

* **Java**: 17+
* **Framework**: Spring Boot 3.x (Spring Web, Spring Data JPA, Spring for Apache Kafka)
* **Database**: PostgreSQL 15
* **Messaging**: Apache Kafka & Zookeeper
* **Monitoring**: Kafka UI (`provectuslabs/kafka-ui`)
* **Containerization**: Docker & Docker Compose
* **Build Tool**: Maven

---

## 🚀 Getting Started

### Prerequisites

Ensure you have the following installed locally:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (including Docker Compose)
* [JDK 17+](https://www.oracle.com/java/technologies/downloads/)
* [Maven](https://maven.apache.org/)

---

### 📦 Infrastructure Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/payment-microservice.git
   cd payment-microservice
   ```

2. **Start the environment with Docker Compose:**
   ```bash
   docker compose up -d
   ```

   This spins up the following services:
   * **PostgreSQL** on port `5432`
   * **Zookeeper** on port `2181`
   * **Kafka Broker** on ports `9092` (host) and `29092` (internal network)
   * **Kafka UI** on port `8081`

3. **Verify running containers:**
   ```bash
   docker ps
   ```

---

### ⚙️ Application Configuration

Check your `src/main/resources/application.properties` (or `application.yml`) to ensure connection parameters match your setup:

```properties
server.port=8080

# PostgreSQL Configuration
spring.datasource.url=jdbc:postgresql://localhost:5432/payment_db
spring.datasource.username=portfolio
spring.datasource.password=portfolio
spring.jpa.hibernate.ddl-auto=update
spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.PostgreSQLDialect

# Kafka Configuration
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=payment-group
spring.kafka.consumer.auto-offset-reset=earliest

# JSON Serialization Configuration
spring.jackson.serialization.write-dates-as-timestamps=false
```

---

### 🏃 Running the Application

Build and start the Spring Boot service:

```bash
mvn clean spring-boot:run
```

The application will launch on `http://localhost:8080`.

---

## 📡 API Endpoints & Usage

### 1. Create a Payment

* **Endpoint**: `POST /payments`
* **Content-Type**: `application/json`

**Sample Request Body:**
```json
{
  "transactionId": "TXN-10023",
  "amount": 150.50,
  "currency": "USD"
}
```

**Sample Response (`201 Created` / `200 OK`):**
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

## 🛠️ Project Structure

```
src/main/java/com/example/payment
├── config/             # Kafka producer and consumer configurations
├── controller/         # REST Controllers handling incoming payments
├── dto/                # Data Transfer Objects
├── model/              # JPA Entities (Payment)
├── producer/           # Kafka Producer publishing payment events
├── consumer/           # Kafka Consumer processing async workflows
├── repository/         # Spring Data JPA Repositories
└── service/            # Core business logic
```

---

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request
package com.portfolio.payment_backend;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
public class AbstractIntegrationTest {

    @SuppressWarnings("resource")
    protected static final PostgreSQLContainer postgresContainer = new PostgreSQLContainer(
            DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("test_db")
            .withUsername("test")
            .withPassword("test");

    @SuppressWarnings("resource")
    protected static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.7.0")); // Official Apache Kafka image for the new module

    static {
        postgresContainer.start();
        kafkaContainer.start();
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        // PostgreSQL properties
        registry.add("spring.datasource.url", postgresContainer::getJdbcUrl);
        registry.add("spring.datasource.username", postgresContainer::getUsername);
        registry.add("spring.datasource.password", postgresContainer::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");

        // Kafka properties
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.kafka.admin.fail-fast", () -> "false");
        registry.add("logging.level.org.springframework.kafka", () -> "DEBUG");
        registry.add("logging.level.com.portfolio.payment_backend", () -> "DEBUG");
    }
}

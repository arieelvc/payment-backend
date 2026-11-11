package com.portfolio.payment_backend.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import com.portfolio.payment_backend.AbstractIntegrationTest;
import com.portfolio.payment_backend.model.Payment;

public class PaymentDlqIntegrationTest extends AbstractIntegrationTest {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private KafkaMessageListenerContainer<String, Payment> dltContainer;
    private BlockingQueue<ConsumerRecord<String, Payment>> dltRecords;

    @BeforeEach
    void setUpDltListener() {
        dltRecords = new LinkedBlockingQueue<>();

        // Build consumer properties manually to avoid deprecated KafkaTestUtils methods
        Map<String, Object> consumerProps = Map.of(
                org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaContainer.getBootstrapServers(),
                org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-group",
                org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class,
                org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JsonDeserializer.class,
                org.springframework.kafka.support.serializer.JsonDeserializer.TRUSTED_PACKAGES, "*");

        DefaultKafkaConsumerFactory<String, Payment> cf = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new org.apache.kafka.common.serialization.StringDeserializer(),
                new org.springframework.kafka.support.serializer.JsonDeserializer<>(Payment.class, false));

        ContainerProperties containerProperties = new ContainerProperties("payment-events.DLT");

        dltContainer = new KafkaMessageListenerContainer<>(cf, containerProperties);
        dltContainer.setupMessageListener((MessageListener<String, Payment>) record -> dltRecords.add(record));
        dltContainer.start();
    }

    @AfterEach
    void tearDownDltListener() {
        if (dltContainer != null) {
            dltContainer.stop();
        }
    }

    @Test
    void shouldRouteFailedMessageToDltAfterExhaustingRetries() {
        // Given: A payment with an ID that does not exist in the DB, triggering an
        // exception in the consumer
        Payment invalidPayment = Payment.builder()
                .id(88888L)
                .transactionId("TXN-FAIL-999")
                .amount(new BigDecimal("250.00"))
                .currency("EUR")
                .status("PENDING")
                .build();

        // When: We send the event to the main topic
        kafkaTemplate.send("payment-events", invalidPayment.getTransactionId(), invalidPayment);

        // Then: Verify that the message ends up published in the DLT after retries are
        // exhausted
        ConsumerRecord<String, Payment> dltRecord = null;
        try {
            dltRecord = dltRecords.poll(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(dltRecord).isNotNull();
        assertThat(dltRecord.topic()).isEqualTo("payment-events.DLT");
        assertThat(dltRecord.key()).isEqualTo("TXN-FAIL-999");

        // Validate that the payload matches the failed object
        Payment receivedPayload = dltRecord.value();
        assertThat(receivedPayload).isNotNull();
        assertThat(receivedPayload.getTransactionId()).isEqualTo("TXN-FAIL-999");
    }
}

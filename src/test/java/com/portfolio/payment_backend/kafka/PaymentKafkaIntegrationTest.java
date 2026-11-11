package com.portfolio.payment_backend.kafka;

import com.portfolio.payment_backend.AbstractIntegrationTest;
import com.portfolio.payment_backend.model.Payment;
import com.portfolio.payment_backend.repository.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class PaymentKafkaIntegrationTest extends AbstractIntegrationTest {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate; // Changed to <String, Object> if using JSON serializer

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void shouldConsumePaymentEventAndUpdateStatus() {
        // Given: 1. We create and save the payment in the database beforehand so it has a
        // real ID
        Payment payment = Payment.builder()
                .transactionId("TXN-KAFKA-001")
                .amount(new BigDecimal("150.00"))
                .currency("USD")
                .status("PENDING")
                .build();

        Payment savedPayment = paymentRepository.save(payment);

        // When: 2. We publish the full Payment object to the correct topic that the
        // consumer listens to
        kafkaTemplate.send("payment-events", savedPayment.getTransactionId(), savedPayment);

        // Then: 3. We use Awaitility to wait until the consumer processes the event and
        // changes the status to "COMPLETED"
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Payment found = paymentRepository.findByTransactionId("TXN-KAFKA-001").orElse(null);
            assertThat(found).isNotNull();
            assertThat(found.getStatus()).isEqualTo("COMPLETED");
        });
    }
}

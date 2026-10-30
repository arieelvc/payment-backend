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
    private KafkaTemplate<String, Object> kafkaTemplate; // Cambiado a <String, Object> si usas serializador JSON

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void shouldConsumePaymentEventAndUpdateStatus() {
        // Given: 1. Creamos y guardamos previamente el pago en la BD para que tenga un
        // ID real
        Payment payment = Payment.builder()
                .transactionId("TXN-KAFKA-001")
                .amount(new BigDecimal("150.00"))
                .currency("USD")
                .status("PENDING")
                .build();

        Payment savedPayment = paymentRepository.save(payment);

        // When: 2. Publicamos el objeto Payment completo al tópico correcto que escucha
        // el consumidor
        kafkaTemplate.send("payment-events", savedPayment.getTransactionId(), savedPayment);

        // Then: 3. Usamos Awaitility para esperar a que el consumidor procese el evento
        // y cambie el status a "COMPLETED"
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Payment found = paymentRepository.findByTransactionId("TXN-KAFKA-001").orElse(null);
            assertThat(found).isNotNull();
            assertThat(found.getStatus()).isEqualTo("COMPLETED");
        });
    }
}

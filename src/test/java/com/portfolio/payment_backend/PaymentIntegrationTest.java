package com.portfolio.payment_backend;

import com.portfolio.payment_backend.model.Payment;
import com.portfolio.payment_backend.repository.PaymentRepository;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

public class PaymentIntegrationTest extends AbstractIntegrationTest {
    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void shouldCreatePaymentSuccessfully() {
        // Given
        Payment newPayment = new Payment();
        newPayment.setTransactionId("TXN-TEST-001");
        newPayment.setAmount(new BigDecimal("250.00"));
        newPayment.setCurrency("USD");
        newPayment.setStatus("PENDING");

        // When
        ResponseEntity<Payment> response = restTemplate.postForEntity(
                "/api/payments", newPayment, Payment.class);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getId()).isNotNull();
        assertThat(response.getBody().getTransactionId()).isEqualTo("TXN-TEST-001");

        // Verify database persistence
        boolean exists = paymentRepository.existsByTransactionId("TXN-TEST-001");
        assertThat(exists).isTrue();
    }
}

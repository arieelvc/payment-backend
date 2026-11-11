package com.portfolio.payment_backend.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.portfolio.payment_backend.kafka.producer.PaymentProducer;
import com.portfolio.payment_backend.model.Payment;
import com.portfolio.payment_backend.repository.PaymentRepository;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConsumer {

    private final PaymentProducer paymentProducer;
    public final PaymentRepository paymentRepository;

    @Transactional
    @KafkaListener(topics = "payment-events", groupId = "payment-group")
    public void consumePaymentEvent(Payment paymentEvent) {
        // Ignore if already completed to prevent infinite loops
        if ("COMPLETED".equals(paymentEvent.getStatus())) {
            return;
        }

        log.info("Processing received payment with ID: {}", paymentEvent.getTransactionId());

        // 1. Find the payment; if it does not exist, throw an exception to trigger retries
        // and dead-letter handling
        Payment payment = paymentRepository.findById(paymentEvent.getId())
                .orElseThrow(() -> new IllegalArgumentException("Payment not found with ID: " + paymentEvent.getId()));

        // 2. Simulate a short processing delay (optional)
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 3. Update and save the payment
        payment.setStatus("COMPLETED");
        Payment savedPayment = paymentRepository.save(payment);

        // 4. Send the second event to Kafka with the COMPLETED status
        paymentProducer.sendPaymentEvent(savedPayment);
        log.info("Payment completed and new event sent to Kafka for: {}", savedPayment.getTransactionId());
    }
}

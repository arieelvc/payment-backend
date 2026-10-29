package com.portfolio.payment_backend.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.portfolio.payment_backend.model.Payment;
import com.portfolio.payment_backend.repository.PaymentRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConsumer {

    private final PaymentProducer paymentProducer;
    public final PaymentRepository paymentRepository;

    @KafkaListener(topics = "payment-events", groupId = "payment-group")
    public void consumePaymentEvent(Payment paymentEvent) {
        // Ignore if already completed to prevent infinite loops
        if ("COMPLETED".equals(paymentEvent.getStatus())) {
            return;
        }

        System.out.println("Processing received payment with ID: " + paymentEvent.getTransactionId());

        // 1. Simulate processing (e.g., payment gateway, validations, etc.)
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 2. Find and update the status in the Database
        paymentRepository.findById(paymentEvent.getId()).ifPresent(payment -> {
            payment.setStatus("COMPLETED");
            Payment savedPayment = paymentRepository.save(payment);

            // 3. SEND THE SECOND EVENT TO KAFKA with the COMPLETED status
            paymentProducer.sendPaymentEvent(savedPayment);

            System.out.println("Payment completed and new event sent to Kafka for: " + savedPayment.getTransactionId());
        });
    }
}

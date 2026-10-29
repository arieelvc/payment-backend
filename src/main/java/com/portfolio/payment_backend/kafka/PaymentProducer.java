package com.portfolio.payment_backend.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.portfolio.payment_backend.model.Payment;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentProducer {

    private final KafkaTemplate<String, Payment> kafkaTemplate;
    private static final String TOPIC = "payment-events";

    public void sendPaymentEvent(Payment payment) {
        kafkaTemplate.send(TOPIC, payment.getTransactionId(), payment);
    }
}

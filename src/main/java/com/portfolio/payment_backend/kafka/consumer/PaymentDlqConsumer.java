package com.portfolio.payment_backend.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.portfolio.payment_backend.model.Payment;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class PaymentDlqConsumer {
    @KafkaListener(topics = "payment-events.DLT", groupId = "payment-dlq-group")
    public void consumeDeadLetterEvent(Payment paymentEvent) {
        log.error("DEAD LETTER QUEUE (DLQ) ALERT: Failed to process payment event. " +
                "Transaction ID: {}, Amount: {}, Currency: {}",
                paymentEvent.getTransactionId(),
                paymentEvent.getAmount(),
                paymentEvent.getCurrency());

        // Here you could trigger external notifications, store in an audit log table,
        // etc.
    }
}

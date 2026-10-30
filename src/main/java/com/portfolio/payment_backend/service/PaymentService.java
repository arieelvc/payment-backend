package com.portfolio.payment_backend.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.portfolio.payment_backend.dto.PaymentRequestDTO;
import com.portfolio.payment_backend.kafka.producer.PaymentProducer;
import com.portfolio.payment_backend.model.Payment;
import com.portfolio.payment_backend.repository.PaymentRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentProducer paymentProducer;

    @Transactional
    public Payment processPayment(PaymentRequestDTO requestDTO) {
        log.info("Processing new payment with transaction ID: {}", requestDTO.getTransactionId());

        // 1. Mapear DTO a la Entidad Payment
        Payment payment = Payment.builder()
                .transactionId(requestDTO.getTransactionId())
                .amount(requestDTO.getAmount())
                .currency(requestDTO.getCurrency())
                .status("PENDING")
                .build();

        // 2. Guardar en PostgreSQL
        Payment savedPayment = paymentRepository.save(payment);
        log.info("Payment saved with ID: {} and status: PENDING", savedPayment.getId());

        // 3. Enviar evento a Kafka para procesamiento asíncrono
        paymentProducer.sendPaymentEvent(savedPayment);

        return savedPayment;
    }
}

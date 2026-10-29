package com.portfolio.payment_backend.controller;

import org.springframework.web.bind.annotation.RestController;

import com.portfolio.payment_backend.dto.PaymentRequestDTO;
import com.portfolio.payment_backend.model.Payment;
import com.portfolio.payment_backend.repository.PaymentRepository;
import com.portfolio.payment_backend.service.PaymentService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentRepository paymentRepository;

    // Endpoint POST para iniciar un pago
    @PostMapping
    public ResponseEntity<Payment> createPayment(@Valid @RequestBody PaymentRequestDTO requestDTO) {
        Payment createdPayment = paymentService.processPayment(requestDTO);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdPayment);
    }

    // Endpoint GET para listar todos los pagos y ver sus estados (PENDING ->
    // COMPLETED)
    @GetMapping
    public ResponseEntity<List<Payment>> getAllPayments() {
        List<Payment> payments = paymentRepository.findAll();
        return ResponseEntity.ok(payments);
    }
}

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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Tag(name = "Payments", description = "Payment creation and retrieval operations")
public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentRepository paymentRepository;

    // POST endpoint to initiate a payment
    @PostMapping
    @Operation(summary = "Create a payment", description = "Creates a payment with PENDING status and publishes it for asynchronous processing.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Payment created"),
            @ApiResponse(responseCode = "400", description = "Request validation failed")
    })
    public ResponseEntity<Payment> createPayment(@Valid @RequestBody PaymentRequestDTO requestDTO) {
        Payment createdPayment = paymentService.processPayment(requestDTO);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdPayment);
    }

    // GET endpoint to list all payments and view their statuses (PENDING ->
    // COMPLETED)
    @GetMapping
    @Operation(summary = "List payments", description = "Returns all payments and their current statuses.")
    @ApiResponse(responseCode = "200", description = "Payments retrieved")
    public ResponseEntity<List<Payment>> getAllPayments() {
        List<Payment> payments = paymentRepository.findAll();
        return ResponseEntity.ok(payments);
    }
}

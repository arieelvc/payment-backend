package com.portfolio.payment_backend.dto;

import java.math.BigDecimal;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import lombok.Data;

@Data
public class PaymentRequestDTO {

    @NotBlank(message = "Transaction ID is required")
    private String transactionId;

    @NotNull(message = "Amount cannot be null")
    @Positive(message = "Amount must be greater than zero")
    private BigDecimal amount;

    @NotBlank(message = "Currency cannot be blank")
    private String currency;
}

package com.portfolio.payment_backend.repository;

import org.springframework.stereotype.Repository;

import com.portfolio.payment_backend.model.Payment;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, Long> {

    Optional<Payment> findByTransactionId(String transactionId);

    boolean existsByTransactionId(String transactionId);
}

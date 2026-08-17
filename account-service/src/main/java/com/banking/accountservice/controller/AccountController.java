package com.banking.accountservice.controller;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/v1/accounts")
@Slf4j
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @PostMapping
    public ResponseEntity<AccountResponse>createAccount(@RequestBody @Valid CreateAccountRequest request){
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(accountService.createAccount(request));
    }

    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountResponse>getAccount(@PathVariable String accountNumber){
        return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal>getBalance(@PathVariable String accountNumber){
        return ResponseEntity.ok(accountService.getBalance(accountNumber));
    }

    @PutMapping("/{accountNumber}/block")
    public ResponseEntity<String>blockAccount(@PathVariable String accountNumber){
        accountService.blockAccount(accountNumber);

        return ResponseEntity.ok("Account Blocked Successfully");
    }

    //SAGA Step:1-> Deduct balance
    //called by transaction service when transfer is initiated

    @PutMapping("/{accountNumber}/deduct")
    public ResponseEntity<String>deductBalance(@RequestParam BigDecimal amount, @PathVariable String accountNumber){
        accountService.deductBalance(accountNumber,amount);
        return ResponseEntity.ok("Balance deducted successfully");
    }

    //SAGA Step:4-> Compensating transaction endpoint
    //called by transaction service in two scenarios
    //1. Fraud detected!!!  -> refund sender (undo step 1)
    //2. Transaction Completed  -> credit receiver

    @PutMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance(@RequestParam BigDecimal amount, @PathVariable String accountNumber){
        accountService.creditBalance(accountNumber,amount);
        return ResponseEntity.ok("Balance credited successfully");
    }
}

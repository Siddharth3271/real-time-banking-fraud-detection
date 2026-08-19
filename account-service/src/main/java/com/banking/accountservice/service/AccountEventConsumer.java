package com.banking.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;

    //consume transaction.completed event from kafka
    //means no fraud deducted->credits receiver account
    @KafkaListener(topics="transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object>payload){
        try{
            String receiverAccount=(String) payload.get("receiverAccountNumber");
            BigDecimal amount=new BigDecimal((String)payload.get("amount"));

            log.info("Crediting amount {} to account {}",amount,receiverAccount);

            accountService.creditBalance(receiverAccount,amount);
        }
        catch (Exception e) {
            log.error("Error crediting account: {}",e.getMessage());
        }
    }

    //consume fraud.detected event from kafka
    //blocks the flagged account
    @KafkaListener(topics = "fraud.detected")
    public void fraudDetected(@Payload Map<String,Object>payload){
        try{
            String accountNumber=(String) payload.get("accountNumber");
            log.info("Fraud detected, Blocking account number {}",accountNumber);

            accountService.blockAccount(accountNumber);

        }
        catch (Exception e){
            log.error("Error Blocking Account: {}",e.getMessage());
        }
    }
}

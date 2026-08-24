package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String,Object> kafkaTemplate;

    private static final String VERIFICATION_REQUIRED_TOPIC="verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC="fraud.check.clean";

    public void checkTransaction(Map<String, Object> payload) {
        String transactionId=(String) payload.get("transactionId");
        String accountNumber=(String) payload.get("senderAccountNumber");

        BigDecimal amount=new BigDecimal(payload.get("amount").toString());

        //fetch the real balance from account service
        BigDecimal senderBalance=accountServiceClient.getBalance(accountNumber);

        log.info("Checking transaction: {} account: {} amount: {} balance: {}",transactionId,accountNumber,amount,senderBalance);

        //perform fraud checks
        FraudCheckResult result=performFruadChecks(accountNumber,amount,senderBalance);

        if(result.isFraud()){
            log.info("Suspicious activity detected in account : {}"+" reason : {} - requesting OTP verification",accountNumber,result.getReason());

            Map<String,Object> verificationEvent=new HashMap<>();
            verificationEvent.put("transactionId",transactionId);
            verificationEvent.put("accountNumber",accountNumber);
            verificationEvent.put("amount",amount);
            verificationEvent.put("reason",result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC,transactionId,verificationEvent);
        }
        else{
            //transaction is clean
            log.info("Transaction clean");
            Map<String,Object> transactionCleanEvent=new HashMap<>();

            transactionCleanEvent.put("transactionId",transactionId);
            transactionCleanEvent.put("isFraud",false);
            transactionCleanEvent.put("reason",null);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC,transactionId,transactionCleanEvent);
        }

    }
}

package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final RedisTemplate<String,String> redisTemplate;

    @Value("${fraud.max-transactions-per-minute}")
    private int maxTransactionPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

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
        FraudCheckResult result=performFraudChecks(accountNumber,amount,senderBalance);

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

    private FraudCheckResult performFraudChecks(String accountNumber, BigDecimal amount, BigDecimal senderBalance) {
        //pattern 1: velocity checks
        if(isVelocityExceeded(accountNumber)){
            return new FraudCheckResult(true,"Too many transactions in 60 seconds - "+"Velocity limit exceeded");
        }

        //pattern 2: amount check
        if(isAmountSuspicious(accountNumber,amount)){
            return new FraudCheckResult(true,"Unusual transaction amount - "+"Exceeds 3x your average");
        }

        //pattern 3: balance check
        if(senderBalance.compareTo(BigDecimal.ZERO)>0 && isBalanceCheckFailed(senderBalance,amount)){
            return new FraudCheckResult(true,"Transaction exceeds 90% of your balance amount");
        }

        return new FraudCheckResult(false,null);
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {
        BigDecimal maximumAllowed=senderBalance.multiply(BigDecimal.valueOf(maxBalancePercentage));

        log.info("Balance check - amount : {} maxAllowed : {} suspicious : {}",amount,maximumAllowed,amount.compareTo(maximumAllowed)>0);

        return amount.compareTo(maximumAllowed)>0;
    }

    private boolean isAmountSuspicious(String accountNumber,BigDecimal amount) {
        String key="fraud:stats:"+accountNumber;

        //Get existing statistics
        Object totalAmountObj=redisTemplate.opsForHash().get(key, "totalAmount");
        Object transactionCountObj=redisTemplate.opsForHash().get(key, "transactionCount");

        //First transaction
        if(totalAmountObj == null || transactionCountObj == null){

            redisTemplate.opsForHash().put(key, "totalAmount", amount.toString());
            redisTemplate.opsForHash().put(key, "transactionCount", "1");
            return false;
        }

        BigDecimal totalAmount=new BigDecimal(totalAmountObj.toString());
        long transactionCount=Long.parseLong(transactionCountObj.toString());

        //Calculate current average before adding this transaction
        BigDecimal averageAmount = totalAmount.divide(
                BigDecimal.valueOf(transactionCount),
                2,
                RoundingMode.HALF_UP
        );

        // Calculate suspicious threshold
        BigDecimal threshold = averageAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultiplier));
        boolean suspicious = amount.compareTo(threshold) > 0;

        // Update statistics
        BigDecimal newTotal=totalAmount.add(amount);
        long newCount=transactionCount+1;

        redisTemplate.opsForHash().put(key, "totalAmount", newTotal.toString());
        redisTemplate.opsForHash().put(key, "transactionCount", String.valueOf(newCount));

        log.info("Amount check - account: {} amount: {} average: {} threshold: {} suspicious: {}",
                accountNumber,amount,averageAmount,threshold,suspicious);

        return suspicious;
    }

    private boolean isVelocityExceeded(String accountNumber) {
        String key="fraud:velocity:"+accountNumber;
        Long count=redisTemplate.opsForValue().increment(key);

        if(count!=null && count==1){
            //When this key is created for the first transaction, give it a 60-second lifetime.
            redisTemplate.expire(key,60, TimeUnit.SECONDS);
        }

        log.info("Velocity check - account : {} count : {}/{}",accountNumber,count,maxTransactionPerMinute);

        return count!=null && count>maxTransactionPerMinute;
    }
}

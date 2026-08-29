package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final RedisTemplate<String,String>redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC="transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC="transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC="transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC="fraud.detected";

    //SAGA step-1 : Initiate Transfer
    //deducts from sender via feign client
    //saves transaction as processing
    //publish event to kafka for fraud check
    //returns
    public TransactionResponse transfer(TransferRequest request) {
        log.info("SAGA Starts - Transfer: {} -> {} with amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        //SAGA step-1: deduct from sender
        accountServiceClient.deductBalance(request.getAmount(),request.getSenderAccountNumber());

        Transaction transaction=new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(transaction.getDescription());
        transaction.setReferenceNUmber(UUID.randomUUID().toString());

        Transaction savedTransaction=transactionRepository.save(transaction);

        log.info("Transaction saved as Processing: {}",savedTransaction.getId());

        //SAGA step-2: publish for fraud check
        TransactionInitiatedEvent event=new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId(),event);
        log.info("SAGA step-2 - TransactionInitiatedEvent published: {}",savedTransaction.getId());

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId) {
        return mapToResponse(transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction not found")));
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        log.info("OTP verification for the transaction : {}",transactionId);

        Transaction transaction=transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction not found "+transactionId));

        //get the stored OTP
        String otpKey="verification:otp:"+transactionId;
        String storedOTP=redisTemplate.opsForValue().get(otpKey);

        if(storedOTP==null){
            //OTP expired
            log.warn("OTP expired for transaction : {}",transactionId);
            compensateTransaction(transaction,"OTP expired - transaction expired, amount refunded");
            return mapToResponse(transaction);
        }
        if(!storedOTP.equals(otp)){
            //Block account and refund
            log.warn("Wrong OTP - Blocking account and refunding : {}",transactionId);
            redisTemplate.delete(otpKey);
            blockAccountCompensate(transaction,"Wrong OTP entered " +"Transaction cancelled - Account blocked for security");
            return mapToResponse(transaction);
        }

        //Correct OTP entered - complete transaction
        log.info("OTP verified - completing transaction : {}",transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);

        return mapToResponse(transaction);
    }

    private void compensateTransaction(Transaction transaction,String reason){
        log.warn("SAGA compensation - refunding : {} amount : {}",transaction.getSenderAccountNumber(),transaction.getAmount());

        //credit money back to sender synchronously (use account service - credit balance)
        accountServiceClient.creditBalance(transaction.getAmount(),transaction.getSenderAccountNumber());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason+" - SAGA compensation executed, amount refunded at "+ LocalDateTime.now());

        transactionRepository.save(transaction);

        //publish refund event - Notification will alert user
        Map<String,Object>refundEvent=new HashMap<>();
        refundEvent.put("transactionId",transaction.getId());
        refundEvent.put("senderAccountNumber",transaction.getSenderAccountNumber());
        refundEvent.put("amount",transaction.getAmount());
        refundEvent.put("reason",transaction.getFailureReason());

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,transaction.getId(),refundEvent);

        log.info("SAGA compensation complete - {} refunded to account {}",transaction.getAmount(),transaction.getSenderAccountNumber());
    }

    private void blockAccountCompensate(Transaction transaction,String reason){

        //publish fraud.detected event - account service will block account
        Map<String,Object>fraudEvent=new HashMap<>();
        fraudEvent.put("transactionId",transaction.getId());
        fraudEvent.put("accountNumber",transaction.getSenderAccountNumber());
        fraudEvent.put("reason",transaction.getFailureReason());

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getSenderAccountNumber(),fraudEvent);
        log.warn("fraud.detected published - Account : {} will be blocked - kindly contact to your bank",transaction.getSenderAccountNumber());

        //SAGA compensation - refund sender
        compensateTransaction(transaction,reason);
    }

    private void completeTransaction(Transaction transaction){
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent=new TransactionCompletedEvent(
                transaction.getId(), transaction.getSenderAccountNumber(), transaction.getReceiverAccountNumber(),
                transaction.getAmount(), transaction.getDescription()
        );

        //publish transaction completed event
        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC,transaction.getId(),completedEvent);

        log.info("SAGA complete - Transaction : {} completed",transaction.getId());
    }

    public void processCleanResult(String transactionId) {
        Transaction transaction=transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction not found "+transactionId));

        if(transaction.getStatus()!= TransactionStatus.PROCESSING){
            log.warn("Transaction {} not processing - skipping",transactionId);
            return;
        }

        completeTransaction(transaction);
    }

    private TransactionResponse mapToResponse(Transaction transaction) {
        TransactionResponse transactionResponse=new TransactionResponse();
        transactionResponse.setId(transaction.getId());
        transactionResponse.setAmount(transaction.getAmount());
        transactionResponse.setDescription(transaction.getDescription());
        transactionResponse.setSenderAccountNumber(transaction.getSenderAccountNumber());
        transactionResponse.setType(transaction.getType());
        transactionResponse.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        transactionResponse.setStatus(transaction.getStatus());
        transactionResponse.setCreatedAt(transaction.getCreatedAt());
        transactionResponse.setCompletedAt(transaction.getCompletedAt());
        transactionResponse.setFailureReason(transaction.getFailureReason());
        transactionResponse.setReferenceNUmber(transaction.getReferenceNUmber());

        return transactionResponse;
    }
}

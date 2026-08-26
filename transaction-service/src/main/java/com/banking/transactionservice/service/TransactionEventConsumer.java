package com.banking.transactionservice.service;

import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class TransactionEventConsumer {

    //consume verification.required
    //generate OTP and ask user to verify
    public void consumeVerificationRequired(@Payload Map<String,Object> payload){

        try{

        }
        catch (Exception e) {

        }
    }
}

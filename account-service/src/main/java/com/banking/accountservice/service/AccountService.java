package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    //SecureRandom is a Java class used to generate cryptographically stronger random values than the normal Random class.
    private static SecureRandom secureRandom=new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("Creating Account for: {}",request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException("Account already exists for email: "+request.getEmail());
        }

        Account account= new Account();
        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhone(request.getPhone());
        account.setAccountType(request.getAccountType());
        account.setAccountStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit());

        account.setAccountNumber(generateAccountNumber());

        account.setDailyTransactionLimit(
                request.getAccountType()== AccountType.SAVINGS ? new BigDecimal("100000") : new BigDecimal("500000")
        );

        Account savedAccount=accountRepository.save(account);
        log.info("Account Created: {}",savedAccount.getAccountNumber());

        return mapToResponse(savedAccount);
    }

    //unique, 12-digit account number
    private String generateAccountNumber() {
        String accountNumber;

        do{
            //0 → 999,999,999,999
            long number=secureRandom.nextLong(1_000_000_000_000L);

            accountNumber=String.format("%012d",number);

        }while(accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private AccountResponse mapToResponse(Account account) {
       AccountResponse response=new AccountResponse();
       response.setId(account.getId());
       response.setAccountNumber(account.getAccountNumber());
       response.setAccountHolderName(account.getAccountHolderName());
       response.setPhone(account.getPhone());
       response.setEmail(account.getEmail());
       response.setAccountType(account.getAccountType());
       response.setAccountStatus(account.getAccountStatus());
       response.setBalance(account.getBalance());
       response.setDailyTransactionLimit(account.getDailyTransactionLimit());
       response.setCreatedAt(account.getCreatedAt());

       return response;
    }

    //get account by account number
    public AccountResponse getAccount(String accountNumber) {
        Account account=accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()->new RuntimeException("Account not found"));

        return mapToResponse(account);
    }

    //get account balance
    public BigDecimal getBalance(String accountNumber) {
        Account account=accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()->new RuntimeException("Account not found"));

        return account.getBalance();
    }

    //called by fraud detection service via kafka
    public void blockAccount(String accountNumber) {
        log.info("Blocking Account: {}", accountNumber);

        Account account=accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()->new RuntimeException("Account not found"));

        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account blocked: {}",accountNumber);
    }

    //deduct balance from sender account, called by transaction service via kafka
    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting Balance {} from account: {}",amount,accountNumber);

        Account account=accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()->new RuntimeException("Account not found"));

        if(account.getAccountStatus()!=AccountStatus.ACTIVE){
            throw  new RuntimeException("Account is not active: "+accountNumber);
        }

        if(account.getBalance().compareTo(amount)<0){
            throw  new RuntimeException("Insufficient funds for account: "+accountNumber);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Balance updated to {} ",account.getBalance());
    }

    //credit balance-> called by transaction service via kafka
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("crediting {} amount to account: {}",amount,accountNumber);

        Account account=accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()->new RuntimeException("Account not found"));

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Balance credited. New Balance: {} ",account.getBalance());
    }
}

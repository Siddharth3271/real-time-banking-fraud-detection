package com.banking.transactionservice.entity;

//Transaction Lifecycle
//Spending->Processing->Completed (clean transaction)
//Spending->Processing->Pending_Verification (suspicious activity)-> Completed (Verified)
//Spending->Processing->Pending_Verification (suspicious activity)-> Return Back (Saga refund) (Not Verified)-> Failed (Flagged)
public enum TransactionStatus {
    PENDING, PROCESSING, PENDING_VERIFICATION, COMPLETED, FAILED, FLAGGED
}

package io.goshawkdb.client;

/**
 * Encloses the result of the transaction (assuming it committed) with the {@link TxnId} of the
 * transaction.
 *
 * @param <R> type of result to be returned
 */
public final class TransactionResult<R> {

    public final R result;
    public final TxnId txnid;
    public final Exception cause;

    TransactionResult(R result, TxnId transactionId) {
        this.result = result;
        this.txnid = transactionId;
        this.cause = null;
    }

    TransactionResult(Exception exception, TxnId transactionId) {
        this.result = null;
        this.txnid = transactionId;
        this.cause = exception;
    }

    TransactionResult(R result, Exception exception, TxnId transactionId) {
        this.result = result;
        this.txnid = transactionId;
        this.cause = exception;
    }

    public boolean isSuccessful() {
        return result != null && cause == null;
    }

}

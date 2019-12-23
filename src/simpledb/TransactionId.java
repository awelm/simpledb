package simpledb;

import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId {
    static AtomicLong counter = new AtomicLong(0);
    long myid;

    public TransactionId() {
        myid = counter.getAndIncrement();
    }

    public long getId() {
        return myid;
    }

    public boolean equals(Object tid) {
        return ((TransactionId)tid).myid == myid;
    }

    public int hashCode() {
        return (int) myid;
    }
}

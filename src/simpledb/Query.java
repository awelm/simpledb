package simpledb;

import java.io.*;
import java.util.*;

/** Query is a wrapper class to manage the execution of queries.
    It takes a query plan in the form of a high level DbIterator
    (built by initiating the constructors of query plans)
    and runs it as a part of a specified transaction.

    @author Sam Madden
*/

public class Query {
    DbIterator op;
    TransactionId tid;
    boolean started = false;

    public Query(DbIterator root, TransactionId t) {
        op = root;
        tid = t;
    }

    public void start()
        throws IOException, DbException, TransactionAbortedException {
        op.open();

        started = true;
    }

    /** @return true if there are more tuples remaining. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return op.hasNext();
    }

    /** Returns the next tuple, or throws NoSuchElementException if the
        iterator is closed.
        @return The next tuple in the iterator
        @throws DbException If there is an error in the database system
        @throws NoSuchElementException If the iterator has finished iterating
        @throws TransactionAbortedException If the transaction is aborted (e.g., due to a deadlock)
    */
    public Tuple next() throws DbException, NoSuchElementException, TransactionAbortedException {
        if (!started) throw new DbException("Database not started.");

        return op.next();
    }

    /** Close the iterator */
    public void close() throws IOException {
        op.close();
        started = false;
    }
}

package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {
    private Predicate p;
    private DbIterator childIt;

    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.childIt = child;
    }

    public TupleDesc getTupleDesc() {
        return childIt.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        childIt.open();
    }

    public void close() {
        childIt.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        childIt.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        if(!childIt.hasNext())
            return null;
        Tuple currTuple = childIt.next();
        while(!p.filter(currTuple)) {
            if(!childIt.hasNext())
                return null;
            currTuple = childIt.next();
        }
        return currTuple;
    }
}

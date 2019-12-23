package simpledb;
import java.util.*;

/**
 * OrderBy is an operator that implements a relational ORDER BY.
 */
public class OrderBy extends AbstractDbIterator {
    DbIterator child;
    TupleDesc td;
    ArrayList<Tuple> childTups = new ArrayList<Tuple>();
    int orderByField;
    Iterator<Tuple> it;
    boolean asc;

    /**
     * Creates a new OrderBy node over the tuples from the iterator.
     *
     * @param orderbyField the field to which the sort is applied.
     * @param asc true if the sort order is ascending.
     * @param child the tuples to sort.
     */
    public OrderBy(int orderbyField,  boolean asc, DbIterator child) {
        this.child = child;
        td= child.getTupleDesc();
        this.orderByField = orderbyField;
        this.asc = asc;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        child.open();
        //load all the tuples in a collection, and sort it
        while (child.hasNext())
            childTups.add((Tuple)child.next());
        Collections.sort(childTups, new TupleComparator(orderByField, asc));
        it = childTups.iterator();
    }

    public void close() {
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        it = childTups.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Returns tuples from the child operator in order
     *
     * @return The next tuple in the ordering, or null if there are no more tuples
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }
}

 class TupleComparator implements Comparator<Tuple> {
     int field;
     boolean asc;

     public TupleComparator(int field, boolean asc) {
         this.field = field;
     this.asc = asc;
     }

     public int compare(Tuple o1, Tuple o2) {
         Field t1 = (o1).getField(field);
         Field t2 = (o2).getField(field);
         if (t1.compare(Predicate.Op.EQUALS, t2))
             return 0;
         if (t1.compare(Predicate.Op.GREATER_THAN, t2))
             return asc?1:-1;
         else
             return asc?-1:1;
     }

}

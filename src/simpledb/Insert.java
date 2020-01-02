package simpledb;
import javax.xml.crypto.Data;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private TupleDesc td;
    private boolean readNextCalled;

    /**
     * Constructor.
     * @param tid The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId tid, DbIterator child, int tableid)
        throws DbException {
        this.tid = tid;
        this.child = child;
        this.tableid = tableid;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"affected_rows"});
        this.readNextCalled = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        readNextCalled = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        if(readNextCalled)
            return null;
        readNextCalled = true;

        BufferPool bp = Database.getBufferPool();
        int insertCount = 0;
        while(child.hasNext()) {
            bp.insertTuple(tid, tableid, child.next());
            insertCount++;
        }

        Tuple retTuple = new Tuple(td);
        retTuple.setField(0, new IntField(insertCount));
        return retTuple;
    }
}

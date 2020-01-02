package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
    private TransactionId tid;
    private DbIterator child;
    private TupleDesc td;
    private boolean readNextCalled;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if(readNextCalled)
            return null;
        readNextCalled = true;

        BufferPool bp = Database.getBufferPool();
        int deleteCount = 0;
        while(child.hasNext()) {
            bp.deleteTuple(tid, child.next());
            deleteCount++;
        }

        Tuple retTuple = new Tuple(td);
        retTuple.setField(0, new IntField(deleteCount));
        return retTuple;
    }
}

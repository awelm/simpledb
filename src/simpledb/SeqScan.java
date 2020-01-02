package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
    private TransactionId tid;
    private int tableId;
    private String tableAlias;
    private DbFile tableFile;
    private DbFileIterator fileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias= tableAlias;

        tableFile = Database.getCatalog().getDbFile(tableId);
        if (tableFile == null) {
            System.err.printf("Could not find table file with ID %d", tableFile.getId());
            System.exit(1);
        }
    }

    public void open()
        throws DbException, TransactionAbortedException {
        fileIterator = tableFile.iterator(tid);
        fileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return tableFile.getTupleDesc();
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return fileIterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        if(!this.hasNext())
            throw new NoSuchElementException();
        return fileIterator.next();
    }

    public void close() {
        fileIterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        fileIterator.rewind();
    }
}

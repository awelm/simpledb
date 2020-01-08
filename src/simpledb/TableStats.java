package simpledb;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.HashMap;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    private DbFile dbFile;
    private int ioCostPerPage;
    private ArrayList<Object> histograms;
    private HashMap<Integer, Field> fieldMax;
    private HashMap<Integer, Field> fieldMin;
    private int tupleCount;

    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        tupleCount = 0;
        histograms = new ArrayList<Object>();
        this.ioCostPerPage = ioCostPerPage;
        dbFile = Database.getCatalog().getDbFile(tableid);
        fieldMax = new HashMap<Integer, Field>();
        fieldMin = new HashMap<Integer, Field>();
        TupleDesc td = dbFile.getTupleDesc();
        calculateTableFieldsMinMax();

        // perform table scan to create histograms
        for(int i=0; i<td.numFields(); i++) {
            switch(td.getType(i)) {
                case INT_TYPE:
                    IntField min = (IntField) fieldMin.get(i);
                    IntField max = (IntField) fieldMax.get(i);
                    histograms.add(new IntHistogram(NUM_HIST_BINS, min.getValue(), max.getValue()));
                    break;
                case STRING_TYPE:
                    histograms.add(new StringHistogram(NUM_HIST_BINS));
                    break;
            }
        }

        // perform table scan to populate histogram
        try {
            TransactionId tid = new TransactionId();
            DbFileIterator it = dbFile.iterator(tid);
            it.open();

            while(it.hasNext()) {
                Tuple t = it.next();
                tupleCount++;
                int numFields = t.getTupleDesc().numFields();
                for(int i=0; i<numFields; i++) {
                    Field f = t.getField(i);
                    switch(f.getType()) {
                        case INT_TYPE:
                            IntHistogram intHist = (IntHistogram) histograms.get(i);
                            intHist.addValue(((IntField)f).getValue());
                            break;
                        case STRING_TYPE:
                            StringHistogram stringHist = (StringHistogram) histograms.get(i);
                            stringHist.addValue(((StringField)f).getValue());
                            break;
                    }
                }
            }

            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void calculateTableFieldsMinMax() {
        try {
            TransactionId tid = new TransactionId();
            DbFileIterator it = dbFile.iterator(tid);
            it.open();

            while(it.hasNext()) {
                Tuple t = it.next();
                int numFields = t.getTupleDesc().numFields();
                for(int i=0; i<numFields; i++) {
                    Field f = t.getField(i);
                    if(!fieldMax.containsKey(i)) {
                        fieldMax.put(i, f);
                        fieldMin.put(i, f);
                    } else {
                        fieldMax.put(i, fieldMax.get(i).compare(Predicate.Op.GREATER_THAN, f) ? fieldMax.get(i) : f);
                        fieldMin.put(i, fieldMin.get(i).compare(Predicate.Op.LESS_THAN, f) ? fieldMin.get(i) : f);
                    }
                }
            }

            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
        return ((HeapFile)dbFile).numPages() * ioCostPerPage;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	// some code goes here
        return (int) (tupleCount * selectivityFactor);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        switch(dbFile.getTupleDesc().getType(field)) {
            case INT_TYPE:
                IntHistogram intHist = (IntHistogram) histograms.get(field);
                int constantInt = ((IntField)constant).getValue();
                return intHist.estimateSelectivity(op, constantInt);
            case STRING_TYPE:
                StringHistogram stringHist = (StringHistogram) histograms.get(field);
                String constantString = ((StringField)constant).getValue();
                return stringHist.estimateSelectivity(op, constantString);
        }
        return -1;
    }

}

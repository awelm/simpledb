package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {
    private DbIterator child;
    private int afield;
    private int gfield;
    private Type gbFieldType;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private DbIterator aggregatorIterator;
    private TupleDesc td;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to construct an
     * IntAggregator or StringAggregator to help you with your implementation of readNext().
     *
     * @param child  The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        gbFieldType = gfield != Aggregator.NO_GROUPING ? child.getTupleDesc().getType(gfield) : null;
        Type aggType = child.getTupleDesc().getType(afield);

        switch (aggType) {
            case INT_TYPE:
                aggregator = new IntAggregator(gfield, gbFieldType, afield, this.aop);
                break;
            case STRING_TYPE:
                aggregator = new StringAggregator(gfield, gbFieldType, afield, this.aop);
                break;
            default:
                System.err.printf("Unsupported type %s", aggType);
                System.exit(1);
        }

        aggregatorIterator = aggregator.iterator();
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
            case MIN:
                return "min";
            case MAX:
                return "max";
            case AVG:
                return "avg";
            case SUM:
                return "sum";
            case COUNT:
                return "count";
        }
        return "";
    }

    public static TupleDesc createAggregateTupleDesc(Type gbFieldType, String aggColumnName, String gbColumnName) {
        String[] fieldNames;
        Type[] fieldTypes;

        if(gbFieldType == null) {
            fieldTypes = new Type[] {Type.INT_TYPE};
            fieldNames = new String[] {aggColumnName};
        }
        else {
            fieldTypes = new Type[] {gbFieldType, Type.INT_TYPE};
            fieldNames = new String[] {gbColumnName, aggColumnName};
        }

        return new TupleDesc(fieldTypes, fieldNames);
    }

    public void open()
            throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();

        while (child.hasNext())
            aggregator.merge(child.next());

        aggregatorIterator.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (aggregatorIterator.hasNext())
            return aggregatorIterator.next();
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggregatorIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator.
     */
    public TupleDesc getTupleDesc() {
        if(td != null)
            return td;

        TupleDesc childTd = child.getTupleDesc();
        String aggColumnName = String.format("%s(%s)", aggName(aop), childTd.getFieldName(afield));
        String groupByColumnName = gfield != Aggregator.NO_GROUPING ? childTd.getFieldName(gfield) : null;
        td = createAggregateTupleDesc(gbFieldType, aggColumnName, groupByColumnName);
        return td;
    }

    public void close() {
        child.close();
        aggregatorIterator.close();
        super.close();
    }
}

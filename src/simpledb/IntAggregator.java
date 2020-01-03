package simpledb;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groupValToAgg;
    private HashMap<Field, Integer> groupValToCount;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupValToAgg = new HashMap<Field, Integer>();
        if (what == Op.AVG)
            this.groupValToCount = new HashMap<Field, Integer>();
    }

    private int getInitValueForOp(Op what) {
        switch(what) {
            case MIN:
                return Integer.MAX_VALUE;
            case MAX:
                return Integer.MIN_VALUE;
            case SUM:
            case AVG:
            case COUNT:
                return 0;
            default:
                System.err.printf("Unsupported operation type %s", what);
                System.exit(1);
        }
        // should never happen
        return 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field groupVal = gbfield != NO_GROUPING ? tup.getField(gbfield) : new IntField(NO_GROUPING);
        int aggFieldVal = ((IntField) tup.getField(afield)).getValue();

        if (!groupValToAgg.containsKey(groupVal)) {
            groupValToAgg.put(groupVal, getInitValueForOp(what));
            if(what == Op.AVG)
                groupValToCount.put(groupVal, 0);
        }

        int updatedAggVal = groupValToAgg.get(groupVal);

        switch(what) {
            case MIN:
                updatedAggVal = Math.min(updatedAggVal, aggFieldVal);
                break;
            case MAX:
                updatedAggVal = Math.max(updatedAggVal, aggFieldVal);
                break;
            case SUM:
                updatedAggVal += aggFieldVal;
                break;
            case AVG:
                updatedAggVal += aggFieldVal;
                groupValToCount.put(groupVal, groupValToCount.get(groupVal) + 1);
                break;
            case COUNT:
                updatedAggVal++;
                break;
            default:
                System.err.printf("Unsupported operation type %s", what);
                System.exit(1);
        }

        groupValToAgg.put(groupVal, updatedAggVal);
    }


    public class IntAggregatorIterator extends AbstractDbIterator {
        private IntAggregator intAgg;
        private TupleDesc td;
        private Iterator<HashMap.Entry<Field, Integer>> it;

        public IntAggregatorIterator(IntAggregator intAgg) {
            this.intAgg = intAgg;
            this.td = Aggregate.createAggregateTupleDesc(intAgg.gbfieldtype, null, null);
        }

        public TupleDesc getTupleDesc() {
            return this.td;
        }

        public void open() {
            it = intAgg.groupValToAgg.entrySet().iterator();
        }

        public void rewind() {
            it = intAgg.groupValToAgg.entrySet().iterator();
        }

        protected Tuple readNext() {
            if(!it.hasNext())
                return null;

            HashMap.Entry<Field, Integer> groupAndAggVal = it.next();
            Field groupValue = groupAndAggVal.getKey();
            IntField aggValue = new IntField(groupAndAggVal.getValue());
            if(this.intAgg.what == Op.AVG)
                aggValue = new IntField(aggValue.getValue() / this.intAgg.groupValToCount.get(groupValue));

            Tuple nextTuple = new Tuple(this.td);

            if(intAgg.gbfield == NO_GROUPING) {
                nextTuple.setField(0, aggValue);
                return nextTuple;
            } else {
                nextTuple.setField(0, groupValue);
                nextTuple.setField(1, aggValue);
                return nextTuple;
            }
        }

        public void close() {
            intAgg = null;
            super.close();
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new IntAggregatorIterator(this);
    }

}

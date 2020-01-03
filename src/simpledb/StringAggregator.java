package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groupValToCount;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            System.err.println("The only string aggregation operator supported is COUNT");
            System.exit(1);
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupValToCount = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field groupVal = tup.getField(gbfield);
        if(!groupValToCount.containsKey(groupVal))
            groupValToCount.put(groupVal, 0);

        groupValToCount.put(groupVal, groupValToCount.get(groupVal) + 1);
    }

    public class StringAggregatorIterator extends AbstractDbIterator {
        private StringAggregator stringAgg;
        private TupleDesc td;
        private Iterator<HashMap.Entry<Field, Integer>> it;

        public StringAggregatorIterator(StringAggregator stringAgg) {
            this.stringAgg = stringAgg;
            this.td = Aggregate.createAggregateTupleDesc(stringAgg.gbfieldtype, null, null);
        }

        public TupleDesc getTupleDesc() {
            return this.td;
        }

        public void open() {
            it = stringAgg.groupValToCount.entrySet().iterator();
        }

        public void rewind() {
            it = stringAgg.groupValToCount.entrySet().iterator();
        }

        protected Tuple readNext() {
            if(!it.hasNext())
                return null;

            HashMap.Entry<Field, Integer> groupAndAggVal = it.next();
            Field groupValue = groupAndAggVal.getKey();
            IntField aggValue = new IntField(groupAndAggVal.getValue());

            Tuple nextTuple = new Tuple(this.td);

            if(stringAgg.gbfield == NO_GROUPING) {
                nextTuple.setField(0, aggValue);
                return nextTuple;
            } else {
                nextTuple.setField(0, groupValue);
                nextTuple.setField(1, aggValue);
                return nextTuple;
            }
        }

        public void close() {
            stringAgg = null;
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
        // some code goes here
        return new StringAggregatorIterator(this);
    }
}

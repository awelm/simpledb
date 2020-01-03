package simpledb;

/**
 * JoinPredicate compares fields of two tuples using a predicate.
 * JoinPredicate is most likely used by the Join operator.
 */
public class JoinPredicate {
    private int field1;
    private Predicate.Op op;
    private int field2;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op The operation to apply (as defined in Predicate.Op); either
     *   Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN, Predicate.Op.EQUAL,
     *   Predicate.Op.GREATER_THAN_OR_EQ, or Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.field1 = field1;
        this.op = op;
        this.field2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples.
     * The comparison can be made through Field's compare method.
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        return t1.getField(field1).compare(op, t2.getField(field2));
    }

    public Predicate.Op getOp() {
        return op;
    }

    public int getFieldOne() {
        return field1;
    }

    public int getFieldTwo() {
        return field2;
    }
}

package simpledb;

/** A LogicalSubplanJoinNode represens the state needed of a join of a
 * table to a subplan in a LogicalQueryPlan -- inherits state from
 * {@link LogicalJoinNode}; t2 and f2 should always be null
 */
public class LogicalSubplanJoinNode extends LogicalJoinNode {
    
    /** The subplan (used on the inner) of the join */
    DbIterator subPlan;
    
    public LogicalSubplanJoinNode(String table1, String joinField1, DbIterator sp, Predicate.Op pred) {
        t1 = table1;
        f1 = joinField1;
        subPlan = sp;
        p = pred;
    }
    
    @Override public int hashCode() {
        return t1.hashCode() + f1.hashCode() + subPlan.hashCode();
    }
    
    @Override public boolean equals(Object o) {
        LogicalJoinNode j2 =(LogicalJoinNode)o;
        if (!(o instanceof LogicalSubplanJoinNode))
            return false;
        
        return (j2.t1.equals(t1)  && j2.f1.equals(f1) && ((LogicalSubplanJoinNode)o).subPlan.equals(subPlan));
    }
    
    public LogicalSubplanJoinNode swapInnerOuter() {
        LogicalSubplanJoinNode j2 = new LogicalSubplanJoinNode(t1,f1,subPlan, p);
        return j2;
    }

}

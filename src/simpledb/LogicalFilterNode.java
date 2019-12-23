package simpledb;

/** A LogicalFilterNode represents the parameters of a filter in the WHERE clause of a query. 
    <p>
    Filter is of the form t.f p c
    <p>
    Where t is a table, f is a file in t, p is a predicate, and c is a constant
*/
public class LogicalFilterNode {
    /** The table (or alias) over which the filter ranges */
    public String t;

    /** The predicate in the filter */
    public Predicate.Op p;
    
    /* The constant on the right side of the filter */
    public String c;
    
    /** The field from t which is in the filter */
    public String f;
    
    public LogicalFilterNode(String table, String field, Predicate.Op pred, String constant) {
        t = table;
        p = pred;
        c = constant;
        f = field;
    }
}
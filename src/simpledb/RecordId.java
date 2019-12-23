package simpledb;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {
	private PageId pid;
	private int tupleNum;

    /** Creates a new RecordId refering to the specified PageId and tuple number.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
    	this.pid = pid;
    	this.tupleNum = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
    	return tupleNum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
    	return pid;
    }
    
    /**
     * Two RecordId objects are considered equal if they represent the same tuple.
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
    	if(o==null || ! (o instanceof RecordId))
    		return false;
    	RecordId other = (RecordId) o;
    	return pid.equals(other.pid) && tupleNum == other.tupleNum;
    }
    
    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	return new Integer(String.valueOf(pid.hashCode()) + String.valueOf(tupleNum));
    }
    
}

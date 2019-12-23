package simpledb;

/** PageId is an interface to a specific page of a specific table. */
public interface PageId {

    /** Return a representation of this page id object as a collection of
        integers (used for logging)

        This class MUST have a constructor that accepts n integer parameters,
        where n is the number of integers returned in the array from serialize.
    */
    public int[] serialize();

    /** @return the unique tableid hashcode with this PageId */
    public int getTableId();

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode();

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o);

    public int pageno();
}


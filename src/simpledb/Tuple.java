package simpledb;

import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {

	private TupleDesc td;
	private RecordId rId;
	private Field[] fields;

    public static Tuple combine(Tuple t1, Tuple t2) {
        TupleDesc newTd = TupleDesc.combine(t1.getTupleDesc(), t2.getTupleDesc());
        Tuple newT = new Tuple(newTd);
        int fieldOneCount = t1.getTupleDesc().numFields();
        int fieldTwoCount = t2.getTupleDesc().numFields();

        for(int x=0; x<fieldOneCount; x++)
            newT.setField(x, t1.getField(x));
        for(int x=fieldOneCount; x < fieldOneCount + fieldTwoCount; x++)
            newT.setField(x, t2.getField(x-fieldOneCount));

        return newT;
    }

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	this.td = td;
    	this.fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() {
        return rId;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
    	this.rId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) throws NoSuchElementException {
    	if(i >= td.numFields())
    		throw new NoSuchElementException(String.format("Trying to set field %d that doesnt exist in schema", i));
    	Type t = td.getType(i);
    	if(f.getType() != t)
    		throw new NoSuchElementException("Specified field type conflicts with type specified in schema");
    	fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) throws NoSuchElementException {
    	if(i >= td.numFields())
    		throw new NoSuchElementException("Trying to access a field that is out of bounds");
    	return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
    	StringBuilder retVal = new StringBuilder(); 
    	for(Field f: fields) {
    		retVal.append(f.toString());
    		retVal.append("\t");
    	}
    	retVal.append("\n");
    	return retVal.toString();
    }
}

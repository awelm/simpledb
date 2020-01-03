package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
	
	private Type[] typeAr;
	private String[] fieldAr;
	

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     * @throws Exception 
     */	
	
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
    	int newTdNumFields = td1.numFields() + td2.numFields();
    	Type[] newTdTypeAr = new Type[newTdNumFields];
    	
    	// concatenate both type arrays
    	for(int x=0; x<td1.numFields(); x++)
    		newTdTypeAr[x] = td1.typeAr[x];
    	int startIndexTd2 = td1.numFields();
    	for(int x=startIndexTd2; x<newTdNumFields; x++)
    		newTdTypeAr[x] = td2.typeAr[x-startIndexTd2];
    	
    	// dont specify field name array if neither td has a specified field name array
    	if(td1.fieldAr == null && td2.fieldAr == null)
    		return new TupleDesc(newTdTypeAr);
    	
    	String[] newTdFieldAr = new String[newTdNumFields];
    	// concatenate both field name arrays
    	for(int x=0; x<td1.numFields(); x++)
    		newTdFieldAr[x] = td1.fieldAr == null ? null : td1.fieldAr[x];
    	for(int x=startIndexTd2; x<newTdNumFields; x++)
    		newTdFieldAr[x] = td2.fieldAr == null ? null : td2.fieldAr[x-startIndexTd2];
    	
     	return new TupleDesc(newTdTypeAr, newTdFieldAr);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	try {
			if(typeAr.length < 1)
				throw new Exception("Must provide at least one field type");
			this.typeAr = typeAr.clone();
			this.fieldAr = fieldAr.clone();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	if(typeAr.length < 1) {
			Exception e = new Exception("Must provide at least one field type");
			e.printStackTrace();
			System.exit(1);
		}
		this.typeAr = typeAr.clone();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	return this.typeAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if (i >= this.numFields())
    		throw new NoSuchElementException();
    	if(fieldAr == null)
    		return null;
    	return this.fieldAr[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
    	if(name == null || fieldAr == null)
    		throw new NoSuchElementException();

    	// adding this to remove table name prefixing that parser adds to the fields
        int dotIndex = name.indexOf('.');
        if(dotIndex != -1)
        	name = name.substring(dotIndex+1);

    	for(int x=0; x<fieldAr.length; x++)
    		if(name.equals(fieldAr[x]))
    			return x;
    	throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
    	if (i >= this.numFields())
    		throw new NoSuchElementException();
        return this.typeAr[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int tupleSize = 0;
		for(Type t: typeAr) {
			tupleSize += t.getLen();
		}
		return tupleSize;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (o==null || !(o instanceof TupleDesc))
    		return false;
    	TupleDesc other_td = (TupleDesc) o;
    	if(other_td.getSize() != this.getSize())
    		return false;
    	for(int x=0; x<typeAr.length; x++) {
    		if(typeAr[x] != other_td.typeAr[x])
    			return false;
    	}
    	return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder retVal = new StringBuilder(); 
    	for(int x=0; x<typeAr.length; x++) {
    		String fieldName = fieldAr == null ? null : fieldAr[x];
    		retVal.append(String.format("%s(%s)",typeAr[x].toString(), fieldName));
    		if(x < typeAr.length -1)
    			retVal.append(",");
    	}
    	return retVal.toString();
    }
}

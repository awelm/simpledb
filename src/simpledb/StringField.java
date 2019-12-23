package simpledb;

import java.io.*;

/**
 * Instance of Field that stores a single String of a fixed length.
 */
public class StringField implements Field {
    private String value;
    private int maxSize;

    public String getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param s The value of this field.
     * @param maxSize The maximum size of this string

     */
    public StringField(String s, int maxSize) {
    this.maxSize = maxSize;

    if (s.length() > maxSize)
        value = s.substring(0,maxSize);
    else
        value = s;
    }

    public String toString() {
        return value;
    }

    public int hashCode() {
        return value.hashCode();
    }

    public boolean equals(Object field) {
        return ((StringField) field).value.equals(value);
    }

    /** Write this string to dos.  Always writes maxSize + 4 bytes to the
    passed in dos.  First four bytes are string length, next bytes are
    string, with remainder padded with 0 to maxSize.
    @param dos Where the string is written
    */
    public void serialize(DataOutputStream dos) throws IOException {
    String s = value;
    int overflow = maxSize - s.length();
    if (overflow < 0) {
        String news = s.substring(0,maxSize);
        s  = news;
    }
    dos.writeInt(s.length());
    dos.writeBytes(s);
    while (overflow-- > 0)
        dos.write((byte)0);
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     *
     * @throws IllegalCastException if val is not a StringField
     * @see Field#compare
     */
    public boolean compare(Predicate.Op op, Field val) {

        StringField iVal = (StringField) val;
        int cmpVal = value.compareTo(iVal.value);

        switch (op) {
        case EQUALS:
            return cmpVal == 0;

        case NOT_EQUALS:
            return cmpVal != 0;

        case GREATER_THAN:
            return cmpVal > 0;

        case GREATER_THAN_OR_EQ:
            return cmpVal >= 0;

        case LESS_THAN:
            return cmpVal < 0;

        case LESS_THAN_OR_EQ:
            return cmpVal <= 0;
           
        case LIKE:
            return value.indexOf(iVal.value) >= 0;
        }

        return false;
    }
    /**
     * @return the Type for this Field
     */
	public Type getType() {
		
		return Type.STRING_TYPE;
	}
}

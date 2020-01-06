// These are not the droids you are looking for.
// This file has been moved to src/simpledb/Parser.java,
// and into the "simpledb" Java package, so that
// unit tests can invoke it.

import java.io.IOException;

public class Parser {
    public static void main(String[] argv) throws IOException {
	System.err.println("Error: You should be using simpledb.Parser, not just plain 'Parser'!");
	System.err.println("Executing simpledb.Parser for you, with the specified arguments...");
	simpledb.Parser.main(argv);
    }
}
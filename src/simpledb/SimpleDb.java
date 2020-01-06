package simpledb;
import java.util.*;
import java.io.*;

public class SimpleDb {
    public static void main (String args[])
            throws DbException, TransactionAbortedException, IOException {
        // convert a file
        if(args[0].equals("convert")) {
        try {
        if (args.length == 3) {
            HeapFileEncoder.convert(new File(args[1]),
                        new File(args[1].replaceAll(".txt", ".dat")),
                        BufferPool.PAGE_SIZE,
                        Integer.parseInt(args[2]));
        }
        else if (args.length == 4) {
            ArrayList<Type> ts = new ArrayList<Type>();
            String[] typeStringAr = args[3].split(",");
            for (String s: typeStringAr) {
            if (s.toLowerCase().equals("int"))
                ts.add(Type.INT_TYPE);
            else if (s.toLowerCase().equals("string"))
                ts.add(Type.STRING_TYPE);
            else {
                System.out.println("Unknown type " + s);
                return;
            }
            }
            HeapFileEncoder.convert(new File(args[1]),
                        new File(args[1].replaceAll(".txt", ".dat")),
                        BufferPool.PAGE_SIZE,
                        Integer.parseInt(args[2]), ts.toArray(new Type[0]));

        } else {
            System.out.println("Unexpected number of arguments to convert ");
        }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        } else if (args[0].equals("print")) {
            File tableFile = new File(args[1]);
            int columns = Integer.parseInt(args[2]);
            DbFile table = Utility.openHeapFile(columns, tableFile);
            TransactionId tid = new TransactionId();
            DbFileIterator it = table.iterator(tid);
            
            if(null == it){
               System.out.println("Error: method HeapFile.iterator(TransactionId tid) not yet implemented!");
            } else {
               it.open();
               while (it.hasNext()) {
                  Tuple t = it.next();
                  System.out.println(t);
               }
               it.close();
            }
        }
        else if (args[0].equals("parser")) {
            // Strip the first argument and call the parser
            String[] newargs = new String[args.length-1];
            for (int i = 1; i < args.length; ++i) {
                newargs[i-1] = args[i];
            }
            
            try {
                //dynamically load Parser -- if it doesn't exist, print error message
                Class<?> c = Class.forName("simpledb.Parser");
                Class<?> s = String[].class;
                
                java.lang.reflect.Method m = c.getMethod("main", s);
                m.invoke(null, (java.lang.Object)newargs);
            } catch (ClassNotFoundException cne) {
                System.out.println("Class Parser not found -- perhaps you are trying to run the parser as a part of lab1?");
            }
            catch (Exception e) {
                System.out.println("Error in parser.");
                e.printStackTrace();
            }

        }
        else {
            System.err.println("Unknown command: " + args[0]);
            System.exit(1);
        }
    }

}

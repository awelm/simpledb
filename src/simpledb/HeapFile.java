package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
	private File f;
	private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
    	return f;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	if(pid == null)
    		return null;
    	int pageNum = pid.pageno();
    	int fileOffset = pageNum * BufferPool.PAGE_SIZE;
    	byte[] pageData = new byte[BufferPool.PAGE_SIZE];
    	try {
    		FileInputStream fis = new FileInputStream(f.getAbsoluteFile());
			fis.skip(fileOffset);
			fis.read(pageData);
    		fis.close();
    		return new HeapPage((HeapPageId)pid, pageData);
    	} catch(Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	}
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	return (int) Math.ceil(f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }
    
    public class HeapFileIterator implements DbFileIterator {
    	private HeapPage hp;
    	private Iterator<Tuple> it;
    	private TransactionId tid;
    	private int tableId;
    	private HeapFile hf;
    	
    	public HeapFileIterator(TransactionId tid, HeapFile hf) {
    		this.tableId = hf.getId();
    		this.tid = tid;
    		this.hf = hf;
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			try {
				HeapPageId pId = new HeapPageId(tableId, 0);
				hp = (HeapPage) Database.getBufferPool().getPage(tid, pId, null);
				it = hp.iterator();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			if(hp == null)
				return false;
			return it.hasNext() || hp.pid.pageno() < hf.numPages() - 1;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if(this.hasNext() ==  false)
				throw new NoSuchElementException();

			if(it.hasNext())
				return it.next();

			try {
				HeapPageId pId = new HeapPageId(tableId, hp.pid.pageno()+1);
				hp = (HeapPage) Database.getBufferPool().getPage(tid, pId, null);
				it = hp.iterator();
				return it.next();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
			return null;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			open();
		}

		@Override
		public void close() {
			hp = null;
			it = null;
			tid = null;
			hf = null;
		}
    	
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }
    
}

package simpledb;

import java.io.*;
import java.nio.Buffer;
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
        int fileOffset = page.getId().pageno() * BufferPool.PAGE_SIZE;
        try {
			RandomAccessFile raf = new RandomAccessFile(f.getAbsoluteFile(), "rw");
			raf.seek(fileOffset);
			raf.write(page.getPageData());
			raf.close();
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
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
        int numPages = this.numPages();
        int tableId = getId();
        BufferPool bp = Database.getBufferPool();
        for(int p=0; p<numPages; p++) {
			HeapPageId pId = new HeapPageId(tableId, p);
			//TODO: maintain a free space list in order to avoid iterating through all pages
        	HeapPage hp = (HeapPage) bp.getPage(tid, pId, Permissions.READ_ONLY);
        	if(hp.getNumEmptySlots() > 0) {
        		// if there is space on this page, acquire write lock before inserting tuple into the page
				hp = (HeapPage) bp.getPage(tid, pId, Permissions.READ_WRITE);
				hp.addTuple(t);
				hp.markDirty(true, tid);
        		return new ArrayList<Page>(Arrays.asList(new Page[] {hp}));
			}
			// optimization: we can release page lock early since we did not actually look at the page's data
			bp.releasePage(tid, pId);
		}

        // create new page if no space in previous pages. Prevent race condition where 2 new pages with the same
		// page number are created, resulting in the first new page being overwritten by the second page
        synchronized (Database.getCatalog()) {
        	int synchronizedNumPages = this.numPages();
			HeapPageId newPageId = new HeapPageId(tableId, synchronizedNumPages);
			HeapPage newPage = new HeapPage(newPageId, new byte[BufferPool.PAGE_SIZE]);
			newPage.addTuple(t);
			writePage(newPage);
			return new ArrayList<Page>(Arrays.asList(new Page[] {newPage}));
		}
    }


    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
		HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
		//TODO: Figure out how to garbage collect empty pages and how to defragment data in heap files
		hp.deleteTuple(t);
		hp.markDirty(true, tid);
		return hp;
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
    		HeapPageId pId = new HeapPageId(tableId, 0);
    		hp = (HeapPage) Database.getBufferPool().getPage(tid, pId, null);
    		it = hp.iterator();
		}

		@Override
		public boolean hasNext() {
			if(hp == null)
				return false;

			if(it.hasNext())
				return true;

			while(hp.pid.pageno() < hf.numPages() - 1) {
				try {
					HeapPageId pId = new HeapPageId(tableId, hp.pid.pageno()+1);
					hp = (HeapPage) Database.getBufferPool().getPage(tid, pId, Permissions.READ_ONLY);
					it = hp.iterator();
					if(it.hasNext())
						return true;
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
			}

			return false;
		}

		@Override
		public Tuple next() throws NoSuchElementException {
			if(this.hasNext() ==  false)
				throw new NoSuchElementException();

			if(it.hasNext())
				return it.next();
			else
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

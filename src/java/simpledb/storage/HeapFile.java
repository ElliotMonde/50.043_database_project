package simpledb.storage;

import java.io.*;
import java.util.*;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        // some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return getFile().getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        Page heapPage;
        try (RandomAccessFile f = new RandomAccessFile(getFile(), "r")) {
            int pageSize = BufferPool.getPageSize();
            int offset = pid.getPageNumber() * pageSize;
            byte[] b = new byte[pageSize];
            f.readFully(b, offset, pageSize);
            HeapPageId hpid = new HeapPageId(getId(), offset);
            heapPage = new HeapPage(hpid, b);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read page", e);
        }
        return heapPage;
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
        try (RandomAccessFile f = new RandomAccessFile(getFile(), "r")) {
            long fullPageSize = f.length();
            int pageSize = BufferPool.getPageSize();
            return (int) (fullPageSize / pageSize);
        } catch (IOException e) {
            throw new RuntimeException("Unable to get page number", e);
        }
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

    class HeapFileIterator extends AbstractDbFileIterator {

        TransactionId tid;
        HeapFile hf;
        int currentPage;
        Iterator<Tuple> iter;

        public HeapFileIterator(TransactionId tid, HeapFile hf) {
            this.tid = tid;
            this.hf = hf;
            this.currentPage = -1;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.currentPage = 0;
            loadPage(currentPage);
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            while ((iter == null) || (!iter.hasNext())){
                if(currentPage == -1)   //Not opened yet
                { 
                    return false;
                }
                else if (currentPage + 1 >= hf.numPages()){
                    return false;
                }
                else{
                    currentPage += 1;
                    loadPage(currentPage);
                }
            }
            return iter.hasNext();
        }
        
        @Override
        public void close() {
            iter = null;
            this.currentPage = -1;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            return hasNext() ? iter.next() : null;
        }

        public void loadPage(int pageNo) throws DbException, TransactionAbortedException{
            if ((pageNo<0) || (pageNo > hf.numPages() - 1)){
                iter = null;
                return ;
            }
                HeapPageId hpid = new HeapPageId(getId(), pageNo);
                HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                iter = p.iterator();
        }

    }

}

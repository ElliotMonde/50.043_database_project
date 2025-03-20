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
        int pageSize = BufferPool.getPageSize();
        byte[] data = new byte[pageSize];

        try (RandomAccessFile f = new RandomAccessFile(getFile(), "r")) {
            int pageNo = pid.getPageNumber();
            int offset = pageNo * pageSize;

            if (offset >= f.length()) {
                throw new IOException("Attempting to read beyond file length: " + offset);
            }

            f.seek(offset);
            int bytesRead = f.read(data, 0, pageSize);
            if (bytesRead < pageSize) {
                throw new IOException("Incomplete page read: expected " + pageSize + ", got " + bytesRead);
            }

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new RuntimeException("HeapFile.readPage error: " + e.getMessage(), e);
        }
    }

    // see DbFile.java for javadocs
    //1. validate pageid
    //2. calculate offset
    //3. write to file
    //4. catch n throw ioexception
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPageId pid = (HeapPageId) page.getId();
        if (pid.getTableId() != getId()) {
            throw new IllegalArgumentException("Page does not belong to this HeapFile");
        }
        int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber()*pageSize;
        try (RandomAccessFile f = new RandomAccessFile(getFile(), "rw")){
            f.seek(offset);
            f.write(page.getPageData());
        } catch (IOException e){
            throw new IOException("failed to write message to disk");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        try (RandomAccessFile f = new RandomAccessFile(getFile(), "r")) {
            return (int) (f.length() / BufferPool.getPageSize());
        } catch (IOException e) {
            throw new RuntimeException("Unable to get page count", e);
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
            currentPage = 0;
            iter = loadPage(currentPage);
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (currentPage == -1) {
                return false;
            }

            while ((iter == null || !iter.hasNext()) && currentPage < hf.numPages() - 1) {
                currentPage++;
                iter = loadPage(currentPage);
            }

            return iter != null && iter.hasNext();
        }

        @Override
        public void close() {
            this.currentPage = -1;
            iter = null;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            return iter != null && hasNext() ? iter.next() : null;
        }

        public Iterator<Tuple> loadPage(int pageNo) throws DbException, TransactionAbortedException {
            HeapPageId hpid = new HeapPageId(getId(), pageNo);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
            return p.iterator();
        }

    }

}

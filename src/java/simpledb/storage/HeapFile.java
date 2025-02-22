package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

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
     *            the file that stores the on-disk backing store for this heap
     *            file.
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
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile file = new RandomAccessFile(getFile(),"r");
            int pageSize = BufferPool.getPageSize();
            int offset = pageSize * pid.getPageNumber();
            file.seek(offset);
            byte[] pageContent = new byte[pageSize];
            file.readFully(pageContent);
            Page heapPage = new HeapPage((HeapPageId)pid, pageContent);
            file.close();
            return heapPage;
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to read page", e);
        }
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
        try{
        RandomAccessFile file = new RandomAccessFile(getFile(),"r");
        long fullPageSize = file.length();
        int pageSize = BufferPool.getPageSize();
        return (int) (fullPageSize / pageSize);
        }catch (IOException e){
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
        return new DbFileIterator() {
            private int currentPageIndex = 0;
            private Iterator<Tuple> currentPageIterator = null;

            @Override
            public void open(){
                currentPageIndex = 0;
                
            }
            @Override
            public boolean hasNext(){

            }
            @Override
            public Tuple next(){

            }
            @Override
            public void rewind() throws DbException, TransactionAbortedException{

            }
            @Override
            public void close(){

            }

            private boolean loadPage(){
                while (currentPageIndex <= numPages()){
                    PageId pid = new HeapPageId(getId(),currentPageIndex);
                    Page page = BufferPool.getPage(tid,pid,Permissions.READ_ONLY);
                    currentPageIterator = page.iterator();
                    currentPageIndex += 1;
                    if (currentPageIterator.hasNext()){
                        return true;
                    }
                    currentPageIndex += 1;
                }
                return false;
            }
        };
    }

}


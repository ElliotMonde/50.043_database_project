package simpledb.storage;

import java.io.*;
import java.util.*;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.RWLock;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private List<PageId> LRUList;
    private List<Page> pagesList;
    private int size = DEFAULT_PAGE_SIZE;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        size = numPages;
        LRUList = new ArrayList<>(size);
        pagesList = new ArrayList<>(size);
        lockManager = new LockManager();

    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire
     * a lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is
     * present, it should be returned. If it is not present, it should be added
     * to the buffer pool and returned. If there is insufficient space in the
     * buffer pool, a page should be evicted and the new page should be added in
     * its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        Page page;
        // acquire lock
        lockManager.acquireLock(pid, tid, perm);
        synchronized (this) {
            if (LRUList.contains(pid)) {
                int pageIndex = LRUList.indexOf(pid);
                page = pagesList.get(pageIndex);
            } else {
                if (pagesList.size() == size) {
                    evictPage();
                }
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                page = dbFile.readPage(pid);
            }
            if (perm == Permissions.READ_WRITE) {
                page.markDirty(true, tid);
            }
            if (size <= pagesList.size()) {
                evictPage();
            }
            putPage(page);
            // release lock in transaction complete
            return page;
        }
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result
     * in wrong behavior. Think hard about who needs to call this and why, and
     * why they can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2

        // int index = LRUList.indexOf(pid);
        // if (index != -1) {
        //     Page page = pagesList.get(index);
        //     if (page.isDirty() != null && page.isDirty().equals(tid)) {
        //         page.markDirty(false, null);
        //     }
        // }
        lockManager.releaseLock(pid, tid, Permissions.READ_ONLY);
        lockManager.releaseLock(pid, tid, Permissions.READ_WRITE);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pgSet = lockManager.getTransactionPIDs(tid);
        for (PageId pid : pgSet) {
            RWLock lock = lockManager.getLock(pid);
            if (holdsLock(tid, pid)) {
                if (lock.canReadWrite(tid)) {
                    lockManager.releaseLock(pid, tid, Permissions.READ_WRITE);
                } else if (lock.canRead(tid)) {
                    lockManager.releaseLock(pid, tid, Permissions.READ_ONLY);
                }
            }
            int ind = LRUList.indexOf(pid);
            pagesList.remove(ind);
            LRUList.remove(pid);
        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.hasPageLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2

        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush pages during commit", e);
            }
        }
        transactionComplete(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2). May
     * block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have been
     * dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtiedPages = f.insertTuple(tid, t);
        for (Page p : dirtiedPages) {
            p.markDirty(true, tid);
            putPage(p);
        }
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Helper method to assist putting page into bufferpool. Does LRU swap if
     * bufferpool full.
     *
     * @param p the page to put into bufferpool
     */
    public void putPage(Page p) throws DbException {
        PageId pid = p.getId();
        for (int i = 0; i < LRUList.size(); i++) {
            if (LRUList.get(i).equals(pid)) { // if page is in bufferpool, remove it and add the dirtied/updated one to the tail of bufferpool (LRU is start of list)
                pagesList.remove(i);
                LRUList.remove(i);
                pagesList.add(p);
                LRUList.add(pid);
                return;
            }
        }

        if (pagesList.size() >= this.size) { // if bufferpool is full, evict the LRU (head of list) using evictPage, then add the dirtied page to bufferpool
            evictPage();
        }
        LRUList.add(pid);
        pagesList.add(p);
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write
     * lock on the page the tuple is removed from and any other pages that are
     * updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have been
     * dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        List<Page> dirtiedPages = f.deleteTuple(tid, t);
        for (Page p : dirtiedPages) {
            p.markDirty(true, tid);
            putPage(p);
        }
        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it
     * writes dirty data to disk so will break simpledb if running in NO STEAL
     * mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Page p : pagesList) {
            if (p.isDirty() != null) {
                flushPage(p.getId());
            }
        }

    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in
     * its cache.
     *
     * Also used by B+ tree files to ensure that deleted pages are removed from
     * the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        int ind = LRUList.indexOf(pid);
        if (ind != -1) {
            LRUList.remove(pid);
            pagesList.remove(ind);
        }
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        int index = LRUList.indexOf(pid);
        if (index != -1) {
            Page page = pagesList.get(index);
            if (page.isDirty() != null) {
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : LRUList) {
            Page page = pagesList.get(LRUList.indexOf(pid));
            if (page.isDirty() != null && page.isDirty().equals(tid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!pagesList.isEmpty()) {
            Page p = pagesList.get(0);
            PageId pid = p.getId();
            if (p.isDirty() != null) {
                try {
                    flushPage(pid);
                } catch (IOException e) {
                    throw new DbException("Failed to evict page from bufferpool.");
                }
            }
            discardPage(pid);
        }
    }

}

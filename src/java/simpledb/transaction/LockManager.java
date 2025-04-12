package simpledb.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class LockManager {

    private HashMap<PageId, RWLock> pageLockMap;
    private HashMap<TransactionId, Set<PageId>> tidMap;

    public LockManager() {
        pageLockMap = new HashMap<>();
        tidMap = new HashMap<>();
    }

    public boolean hasLock(PageId pid) {
        return pageLockMap.containsKey(pid);
    }

    public void acquireLock(PageId pid, TransactionId tid, Permissions perm) {
        switch (perm) {
            case READ_ONLY:
                acquireReadLock(pid, tid);
                break;
            case READ_WRITE:
                acquireReadWriteLock(pid, tid);
                break;
        }
    }

    public void releaseLock(PageId pid, TransactionId tid, Permissions perm) {
        switch (perm) {
            case READ_ONLY:
                releaseReadLock(pid, tid);
                break;
            case READ_WRITE:
                releaseReadWriteLock(pid, tid);
                break;
        }
    }

    public void acquireReadLock(PageId pid, TransactionId tid) {
        if (tidMap.containsKey(tid) && tidMap.get(tid).contains(pid)) {
            return;
        }
        if (!hasPageLock(pid, tid)) {
            RWLock lock = getLockOrDefault(pid, tid);
            lock.acquireReadLock(tid);
        } else {
            RWLock lock = pageLockMap.get(pid);
            if (!lock.canRead(tid)) {
                lock.acquireReadLock(tid);
            }
        }
        addToTIDMap(pid, tid);
    }

    public void acquireReadWriteLock(PageId pid, TransactionId tid) {
        if (!hasPageLock(pid, tid)) {
            RWLock lock = getLockOrDefault(pid, tid);
            lock.acquireReadWriteLock(tid);
        } else {
            RWLock lock = pageLockMap.get(pid);
            if (!lock.canReadWrite(tid)) {
                lock.acquireReadWriteLock(tid);
            }
        }
        addToTIDMap(pid, tid);
    }

    public void releaseReadLock(PageId pid, TransactionId tid) {
            RWLock lock = pageLockMap.get(pid);
            lock.readUnlock(tid);
            removeFromTIDMap(pid, tid);
    }

    public void releaseReadWriteLock(PageId pid, TransactionId tid) {
        RWLock lock = pageLockMap.get(pid);
        lock.readWriteUnlock(tid);
        removeFromTIDMap(pid, tid);
    }

    public RWLock getLockOrDefault(PageId pid, TransactionId tid) {
        return pageLockMap.computeIfAbsent(pid, k -> new RWLock());
    }

    public RWLock getLock(PageId pid) {
        return pageLockMap.get(pid);
    }

    public void addToTIDMap(PageId pid, TransactionId tid) {
        Set<PageId> pgSet = getTransactionPIDs(tid);
        if (!pgSet.contains(pid)) {
            pgSet.add(pid);
        }
    }

    public void removeFromTIDMap(PageId pid, TransactionId tid) {
        if (tidMap.containsKey(tid)) {
            Set<PageId> pgSet = tidMap.get(tid);
            pgSet.remove(pid);
            if (pgSet.isEmpty()) {
                tidMap.remove(tid);
            }
        }
    }

    public Set<PageId> getTransactionPIDs(TransactionId tid) {
        return tidMap.computeIfAbsent(tid, k -> new HashSet<>());
    }

    public boolean hasPageLock(PageId pid, TransactionId tid) {
        return getTransactionPIDs(tid).contains(pid);
    }

    public HashSet<TransactionId> getLockHoldersTID(PageId pid) {
        HashSet<TransactionId> holders = new HashSet<>();
        for (HashMap.Entry<TransactionId, Set<PageId>> e : tidMap.entrySet()) {
            if (e.getValue().contains(pid)) {
                holders.add(e.getKey());
            }
        }
        return holders;
    }
}

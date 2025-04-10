package simpledb.transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class LockManager {

    private ConcurrentHashMap<PageId, RWLock> pageLockMap;
    private ConcurrentHashMap<TransactionId, Set<PageId>> tidMap;
    private ConcurrentHashMap<TransactionId, Set<TransactionId>> waitForGraph;

    public LockManager() {
        pageLockMap = new ConcurrentHashMap<>();
        tidMap = new ConcurrentHashMap<>();
        waitForGraph = new ConcurrentHashMap<>();
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
        RWLock lock = getLockOrDefault(pid, tid);
        if (!lock.canRead(tid)) {
            if (lock.heldByOtherTransaction(tid)) {
                for (TransactionId holder : lock.lockHolders()) {
                    if (!holder.equals(tid)) {
                        addToWaitForGraph(tid, holder);
                    }
                }
                try {
                    if (hasCycleInGraph()) {
                        throw new TransactionAbortedException();
                    }
                } catch (TransactionAbortedException e) {
                    handleDeadlock(tid);
                    return;
                }
            }
            lock.acquireReadLock(tid);
        }
        addToTIDMap(pid, tid);
        for (TransactionId holder : lock.lockHolders()) {
            if (!holder.equals(tid)) {
                removeFromWaitForGraph(tid, holder);
            }
        }
    }


    public void acquireReadWriteLock(PageId pid, TransactionId tid) {
        RWLock lock = getLockOrDefault(pid, tid);
        if (!lock.canReadWrite(tid)) {
            if (lock.heldByOtherTransaction(tid)) {
                for (TransactionId holder : lock.lockHolders()) {
                    if (!holder.equals(tid)) {
                        addToWaitForGraph(tid, holder);
                    }
                }
                try {
                    if (hasCycleInGraph()) {
                        throw new TransactionAbortedException();
                    }
                } catch (TransactionAbortedException e) {
                    handleDeadlock(tid);
                    return;
                }
            }
            lock.acquireReadWriteLock(tid);
        }
        addToTIDMap(pid, tid);
        for (TransactionId holder : lock.lockHolders()) {
            if (!holder.equals(tid)) {
                removeFromWaitForGraph(tid, holder);
            }
        }
    }

    public void releaseReadLock(PageId pid, TransactionId tid) {
        RWLock lock = pageLockMap.get(pid);
        lock.readUnlock(tid);
        removeFromTIDMap(pid, tid);
        for (TransactionId holder : lock.lockHolders()) {
            if (!holder.equals(tid)) {
                removeFromWaitForGraph(tid, holder);
            }
        }
    }

    public void releaseReadWriteLock(PageId pid, TransactionId tid) {
        RWLock lock = pageLockMap.get(pid);
        lock.readWriteUnlock(tid);
        removeFromTIDMap(pid, tid);
        for (TransactionId holder : lock.lockHolders()) {
            if (!holder.equals(tid)) {
                removeFromWaitForGraph(tid, holder);
            }
        }
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

    // waitForGraph Operations and Deadlock Resolving

    private void addToWaitForGraph(TransactionId tid, TransactionId tidWithResource) {
        if (!waitForGraph.containsKey(tid)) {
            waitForGraph.put(tid, new HashSet<>());
        }
        waitForGraph.get(tid).add(tidWithResource);
    }

    private boolean hasCycleInGraph() {
        Set<TransactionId> visited = new HashSet<>();
        Set<TransactionId> stack = new HashSet<>();
        for (TransactionId tid : waitForGraph.keySet()) {
            if (!visited.contains(tid)) {
                if (detectCycleDFS(tid, visited, stack)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean detectCycleDFS(TransactionId tid, Set<TransactionId> visited, Set<TransactionId> stack) {
        visited.add(tid);
        stack.add(tid);
        for (TransactionId neighbour : waitForGraph.getOrDefault(tid, new HashSet<>())) {
            if (!visited.contains(neighbour)) {
                if (detectCycleDFS(neighbour, visited, stack)) {
                    return true;
                }
            } else if (stack.contains(neighbour)) {
                return true;
            }
        }
        stack.remove(tid);
        return false;
    }

    private void removeFromWaitForGraph(TransactionId tid, TransactionId otherTid) {
        if (waitForGraph.containsKey(tid)) {
            waitForGraph.get(tid).remove(otherTid);
            if (waitForGraph.get(tid).isEmpty()) {
                waitForGraph.remove(tid);
            }
        }
    }

    private void handleDeadlock(TransactionId tid) {
        System.err.println("Deadlock detected for transaction" + tid);
        Database.getBufferPool().transactionComplete(tid, false);
        System.err.println("Latest Transaction Aborted " + tid);
    }
}

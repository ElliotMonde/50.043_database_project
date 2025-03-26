package simpledb.transaction;

import java.util.HashMap;
import java.util.Set;

public class RWLock {

    private boolean exclusive; // current state: exclusive when 1 holders and read_write, exclusive is always read_write permissions, if read_only --> shared
    private HashMap<TransactionId, Boolean> holderTIDs; // tid, isholder, also queue
    // lock: knows who acquired it, who is waiting for lock, knows whether it's exclusive

    public RWLock() {
        exclusive = false;
        holderTIDs = new HashMap<>();
    }

    public void acquireReadLock(TransactionId tid) {
        synchronized (this) {
            if (holderTIDs.containsKey(tid)){
                return;
            }
            try {
                while (exclusive) {
                    this.wait();
                }
                holderTIDs.put(tid, false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void acquireReadWriteLock(TransactionId tid) {
        synchronized (this) {
            try {
                if (exclusive && holderTIDs.containsKey(tid)){ // if already holding rw lock
                    return;
                }
                while (exclusive || holderTIDs.size() > (holderTIDs.containsKey(tid) ? 1 : 0)) {
                    this.wait();
                }
                exclusive = true;
                holderTIDs.put(tid, true); // upgrade or put new
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void readUnlock(TransactionId tid) {
        synchronized (this) {
            if (holderTIDs.containsKey(tid) && !holderTIDs.get(tid)) { // if holder of non-exclusive lock
                holderTIDs.remove(tid);
                if (holderTIDs.isEmpty()) { // only notify if empty, for threads waiting for write lock
                    notifyAll();
                }
            }
        }
    }

    public void readWriteUnlock(TransactionId tid) {
        synchronized (this) {
            if (holderTIDs.containsKey(tid) && holderTIDs.get(tid)) {
                holderTIDs.remove(tid);
                exclusive = false;
                notifyAll();
            }
        }
    }

    public synchronized Set<TransactionId> lockHolders() {
        return holderTIDs.keySet();
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public synchronized boolean canRead(TransactionId tid) {
        return holderTIDs.containsKey(tid);
    }

    public synchronized boolean canReadWrite(TransactionId tid) {
        return holderTIDs.containsKey(tid) && holderTIDs.get(tid);
    }
}

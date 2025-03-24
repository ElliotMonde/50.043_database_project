package simpledb.transaction;

import java.util.HashMap;
import java.util.Set;

public class RWLock {

    private boolean exclusive; // current state: exclusive when 1 holders and read_write, exclusive is always read_write permissions, if read_only --> shared
    private HashMap<TransactionId, Boolean> holderTIDs; // tid, isholder, also queue
    private int waitingWriters;
    // lock: knows who acquired it, who is waiting for lock, knows whether it's exclusive

    public RWLock() {
        exclusive = false;
        holderTIDs = new HashMap<>();
        waitingWriters = 0;
    }

    public void acquireReadLock(TransactionId tid) {
        synchronized (this) {
            try {
                while (exclusive || waitingWriters > 0) {
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
                ++waitingWriters;
                while (exclusive || !holderTIDs.isEmpty()) {
                    this.wait();
                }
                --waitingWriters;
                exclusive = true;
                holderTIDs.put(tid, true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void readUnlock(TransactionId tid) {
        synchronized (this) {
            if (holderTIDs.containsKey(tid) && !holderTIDs.get(tid)) {
                holderTIDs.remove(tid);
                if (holderTIDs.isEmpty()) {
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

    public synchronized boolean isExclusive() {
        return exclusive;
    }

    public synchronized boolean canRead(TransactionId tid) {
        return holderTIDs.containsKey(tid);
    }

    public synchronized boolean canReadWrite(TransactionId tid) {
        return holderTIDs.containsKey(tid) && holderTIDs.get(tid);
    }
}

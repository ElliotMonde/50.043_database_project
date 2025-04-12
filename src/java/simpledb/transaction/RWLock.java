package simpledb.transaction;

import java.util.HashMap;
import java.util.Set;

public class RWLock {

    private boolean exclusive; // current state: exclusive when 1 holders and read_write, exclusive is always read_write permissions, if read_only --> shared
    private HashMap<TransactionId, Boolean> holderTIDs; // tid, isholder
    private int waitingWriters;
    
    public RWLock() {
        exclusive = false;
        holderTIDs = new HashMap<>();
        waitingWriters = 0;
    }

    public void acquireReadLock(TransactionId tid) {
        synchronized (this) {
            if (holderTIDs.containsKey(tid)){
                return;
            }
            try {
                while (exclusive || waitingWriters > 0) {
                    this.wait();
                }
                holderTIDs.put(tid, false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Thread interrupted while acquiring RW lock", e);
            }
        }
    }

    public void acquireReadWriteLock(TransactionId tid) {
        synchronized (this) {
            try {
                if (holderTIDs.getOrDefault(tid, false)) { // if already holding rw lock
                    return;
                }
                waitingWriters++;
                while (exclusive || holderTIDs.size() > (holderTIDs.containsKey(tid) ? 1 : 0)) {
                    this.wait();
                }
                waitingWriters--;
                if (waitingWriters < 0) {
                    waitingWriters = 0;
                }
                exclusive = true;
                holderTIDs.put(tid, true); // upgrade or put new
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); 
                throw new RuntimeException("Thread interrupted while acquiring RW lock", e);
            }
        }
    }

    public void readUnlock(TransactionId tid) {
        synchronized (this) {
            if (holderTIDs.containsKey(tid) && !holderTIDs.get(tid)) { // if holder of non-exclusive lock
                holderTIDs.remove(tid);
            }
            this.notifyAll();
        }
    }

    public void readWriteUnlock(TransactionId tid) {
        synchronized (this) {
            if (holderTIDs.containsKey(tid) && holderTIDs.get(tid)) {
                holderTIDs.remove(tid);
                exclusive = false;
            }
            this.notifyAll();
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

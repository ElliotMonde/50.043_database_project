package simpledb.execution;

import java.io.IOException;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    OpIterator child;
    TransactionId tid;
    TupleDesc td;
    boolean isOpen;
    boolean fetched;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        isOpen = false;
        fetched = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        isOpen = true;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        isOpen = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (!isOpen) {
            throw new DbException("Delete operator is not open");
        }
        close();
        open();
        fetched = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!isOpen || fetched) {
            return null;
        }
        int count = 0;
        fetched = true;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(tid, child.next());
                count++;
            } catch (IOException e) {
                throw new DbException("Failed to Delete.");
            }
        }
        Tuple numberDeletedTuple = new Tuple(td);
        numberDeletedTuple.setField(0, new IntField(count));
        return numberDeletedTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}

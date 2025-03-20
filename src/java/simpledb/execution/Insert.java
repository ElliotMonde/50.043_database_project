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
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    OpIterator child;
    TransactionId tid;
    TupleDesc td;
    int tableId;
    boolean isOpen;
    boolean fetched;

    /**
     * Constructor.
     *
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which
     * we are to insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.tableId = tableId;
        this.child = child;
        this.isOpen = false;
        this.fetched = false;

        TupleDesc childTD = child.getTupleDesc();
        TupleDesc tableTD = Database.getCatalog().getTupleDesc(tableId);
        if (!tableTD.equals(childTD)) {
            throw new DbException("OpIterator child's tupleDesc is different from table's tupleDesc in Insert.");
        }

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        this.isOpen = true;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        this.isOpen = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (!isOpen) {
            throw new DbException("Insert operator is not open");
        }
        child.rewind();
        fetched = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!isOpen || fetched) {
            return null;
        }
        fetched = true;

        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, t);
                count++;
            } catch (IOException e) {
                throw new DbException("Failed to Insert.");
            }
        }
        Tuple numInsertedTuple = new Tuple(this.td);
        numInsertedTuple.setField(0, new IntField(count));
        return numInsertedTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}

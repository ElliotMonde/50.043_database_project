package simpledb.execution;

import java.util.*;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private int tableId;
    private String tableAlias;
    private DbFileIterator tupleIterator;
    private DbFile file;
    private TransactionId tid;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *                   The transaction this scan is running as a part of.
     * @param tableid
     *                   the table to scan.
     * @param tableAlias
     *                   the alias of this table (needed by the parser); the
     *                   returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case
     *                   where
     *                   tableAlias or fieldName are null. It shouldn't crash if
     *                   they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias == null ? "null" : tableAlias;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
    }

    /**
     * @return
     *         return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        // some code goes here
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * 
     * @param tableid
     *                   the table to scan.
     * @param tableAlias
     *                   the alias of this table (needed by the parser); the
     *                   returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case
     *                   where
     *                   tableAlias or fieldName are null. It shouldn't crash if
     *                   they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        if (this.tupleIterator != null) {
            this.tupleIterator.close();
        }
        this.tableAlias = tableAlias;
        this.tableId = tableid;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.tupleIterator = this.file.iterator(tid);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.tupleIterator = this.file.iterator(tid);
        tupleIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name. The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = this.file.getTupleDesc();
        Iterator<TupleDesc.TDItem> iter = td.iterator();
        int numFields = td.numFields();
        List<String> fieldNames = new ArrayList<>(numFields);
        List<Type> fieldTypes = new ArrayList<>(numFields);

        tableAlias = tableAlias == null ? this.getTableName() : tableAlias;
        while (iter.hasNext()) {
            TupleDesc.TDItem next = iter.next();
            fieldNames.add(String.format("%s.%s", tableAlias,
                    next.fieldName == null ? "null" : next.fieldName));
            fieldTypes.add(next.fieldType);
        }
        return new TupleDesc(fieldTypes, fieldNames);
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.tupleIterator != null && this.tupleIterator.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        Tuple next_tuple = this.tupleIterator.next();
        if (next_tuple == null) {
            throw new NoSuchElementException("No tuple found.");
        }
        return next_tuple;
    }

    @Override
    public void close() {
        // some code goes here
        if (this.tupleIterator != null) {
            this.tupleIterator.close();
        }
        this.tupleIterator = null;
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.tupleIterator.close();
        this.tupleIterator.open();
    }
}

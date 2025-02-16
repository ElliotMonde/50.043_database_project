package simpledb.storage;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private RecordId recordId;
<<<<<<< Updated upstream
=======
    private List<Field> entries; // Contains all items within a tuple

>>>>>>> Stashed changes
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *           the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
<<<<<<< Updated upstream
        this.td = td;
        this.recordId = null;
=======
        this.entries = new ArrayList<>(td.getSize());
        this.td = td;
        this.recordId = null;

>>>>>>> Stashed changes
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        // return null;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        // return null;
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *          index of the field to change. It must be a valid index.
     * @param f
     *          new value for the field.
     */
<<<<<<< Updated upstream
    public void setField(int i, Field f) {
        // some code goes here
=======
    public void setField(int i, Field f){
        if (i < this.td.getSize()) {
            this.entries.set(i, f);
        }
>>>>>>> Stashed changes
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *          field index to return. Must be a valid index.
     */
    public Field getField(int i) {
<<<<<<< Updated upstream
        // some code goes here
=======
        if (i < this.td.getSize()) {
            return this.entries.get(i);
        }
>>>>>>> Stashed changes
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    @Override
    public String toString() {
<<<<<<< Updated upstream
        // some code goes here
        throw new UnsupportedOperationException("Implement this");
=======
        String result = "";
        for (Field entry : entries) {
            result += entry.toString();
            result += "\t";
        }
        return result.strip();
>>>>>>> Stashed changes
    }

    /**
     * @return
<<<<<<< Updated upstream
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return null;
=======
     *         An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        return entries.iterator();
>>>>>>> Stashed changes
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
<<<<<<< Updated upstream
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
=======
     */
    public void resetTupleDesc(TupleDesc td) {
        this.td = td;
>>>>>>> Stashed changes
    }
}

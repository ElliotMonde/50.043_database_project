package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */

public class RecordId implements Serializable {
    private PageId pid;
    private int tupleno;
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return this.tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return this.pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        //equal if a. refer to same page id and same tupleno
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecordId recordId = (RecordId) o;

        if (tupleno != recordId.tupleno) return false;
        return pid != null ? pid.equals(recordId.pid) : recordId.pid == null;
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        //recordIDs must have same hash code
        //recordIDs cannot be null
        int result;
        if (pid != null) {
            result = pid.hashCode();
        } else {
            result = 0;
        }
        result = 31 * result + tupleno;
        return result;
        //throw new UnsupportedOperationException("implement this");
    }

}

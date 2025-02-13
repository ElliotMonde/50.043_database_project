package simpledb.storage;

import java.io.Serializable;
import java.util.*;
import simpledb.common.Type;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<String> fieldNamesList;
    private List<Type> typesList;

    /*** A help class to facilitate organizing the information of each field * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /*** The type of the field * */
        public final Type fieldType;

        /*** The name of the field * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *         An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        List<TDItem> l = new ArrayList<>();
        for (int i = 0; i < typesList.size(); i++) {
            l.add(new TDItem(typesList.get(i), fieldNamesList.get(i)));
        }
        Iterator<TDItem> iterator = l.iterator();
        return iterator;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *                array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *                array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) { // field is a column, fieldAr is the array holding names of
                                                        // columns
        // some code goes here
        fieldNamesList = Arrays.asList(fieldAr);
        typesList = Arrays.asList(typeAr);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *               array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        typesList = Arrays.asList(typeAr);
        fieldNamesList = new ArrayList<>();
        for (Type t : typesList) {
            fieldNamesList.add(null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.fieldNamesList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *          index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return this.fieldNamesList.get(i);
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *          The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return this.typesList.get(i);
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *             name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *                                if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        return this.fieldNamesList.indexOf(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (Type t : this.typesList) {
            totalSize += t.getLen();
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<Type> newTypesList = new ArrayList<>();
        List<String> newFieldNamesList = new ArrayList<>();

        newTypesList.addAll(td1.typesList);
        newTypesList.addAll(td2.typesList);

        newFieldNamesList.addAll(td1.fieldNamesList);
        newFieldNamesList.addAll(td2.fieldNamesList);

        return new TupleDesc((Type[]) newTypesList.toArray(), (String[]) newFieldNamesList.toArray());
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *          the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(Object o) {
        // some code goes here

        // check if o is an instance of TupleDesc class
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        // check if items are same sizes
        if (other.typesList.size() == this.typesList.size()
                || other.numFields() != this.numFields()) {
            return false;
        }
        for (int i = 0; i < other.numFields(); i++) {
            // check if ith-items are the same
            if (other.typesList.get(i) != this.typesList.get(i)
                    || !(other.fieldNamesList.get(i)).equals(this.fieldNamesList.get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        // some code goes here
        String s = "";
        for (int i = 0; i < typesList.size(); i++) {
            s += typesList.get(i) + "(" + fieldNamesList.get(i) + ")";
        }
        return s;
    }
}

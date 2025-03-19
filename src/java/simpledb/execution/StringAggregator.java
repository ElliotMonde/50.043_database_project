package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int fieldIndexToGB;
    private Type typeOfField;
    private int fieldIndexOfAggregate;
    private Op operation;
    private HashMap <Field , Integer> resultantTable; //First Field represents the key, the second integer represent the value requested of number of tuples with the same key
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.fieldIndexToGB = gbfield;
        this.typeOfField = gbfieldtype;
        this.fieldIndexOfAggregate = afield;
        this.operation = what;
        this.resultantTable = new HashMap<Field , Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key;
        if (this.fieldIndexToGB == Aggregator.NO_GROUPING){
            key = null;
        }
        else{
            key = tup.getField(fieldIndexToGB);
        }
        
        if (!resultantTable.containsKey(key)){
            switch(operation){
                case COUNT:
                    resultantTable.put(key,1);
                    break;
            }
        }
        else{
            switch(operation){
                case COUNT:
                    int count = 1 + resultantTable.get(key);
                    resultantTable.put(key,count);
                    break;
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
        Type[] typeAr;
        String[] fieldAr;
        if (this.fieldIndexToGB == Aggregator.NO_GROUPING){
            typeAr = new Type[] {Type.INT_TYPE};
            fieldAr = new String[] {"aggregateVal"};
        }
        else{
            typeAr = new Type[] {typeOfField,Type.INT_TYPE};
            fieldAr = new String[] {"groupVal","aggregateVal"};
        }
        TupleDesc descriptor = new TupleDesc(typeAr, fieldAr);
        for (Field key : resultantTable.keySet()){
            Tuple entry = new Tuple(descriptor);
            int value = resultantTable.get(key);
            if (this.fieldIndexToGB == Aggregator.NO_GROUPING){
                entry.setField(0, new IntField(value));
            }
            else{
                entry.setField(0, key);
                entry.setField(1, new IntField(value));
            }
            tupleList.add(entry);
        }
        return new TupleIterator(descriptor, tupleList);
    }

}

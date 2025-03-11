package simpledb.execution;
import java.util.*;
import simpledb.common.Type;
import simpledb.storage.Field;// Imported to work with Field
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int fieldIndexToGB;
    private Type typeOfField;
    private int fieldIndexOfAggregate;
    private Op operation;
    private HashMap <Field , Integer> resultantTable; //First Field represents the key, the second integer represent the value requested of number of tuples with the same key
    private HashMap <Field , Integer> countTable; //First Field represents the key, the second integer represent the count of the number of tuples with the same key

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.fieldIndexToGB = gbfield;
        this.typeOfField = gbfieldtype;
        this.fieldIndexOfAggregate = afield;
        this.operation = what;
        this.resultantTable = new HashMap<Field , Integer>();
        this.countTable = new HashMap<Field , Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key;
        Integer value;
        if (this.fieldIndexToGB == Aggregator.NO_GROUPING){
            key = null;
        }
        else{
            key = tup.getField(fieldIndexToGB);
        }
        
        int tupleValue = ((IntField)tup.getField(fieldIndexOfAggregate)).getValue();
        if (!resultantTable.containsKey(key)){
            switch(operation){
                case MIN:
                    resultantTable.put(key,tupleValue);
                    break;
                case MAX:
                    resultantTable.put(key,tupleValue);
                    break;
                case SUM:
                    resultantTable.put(key,tupleValue);
                    break;
                case AVG:
                    resultantTable.put(key,tupleValue);
                    break;
                case COUNT:
                    resultantTable.put(key,1);
                    break;
            }
            countTable.put(key,1);
        }
        else{
            switch(operation){
                case MIN:
                    if (tupleValue<resultantTable.get(key)){
                        resultantTable.put(key,tupleValue);
                    }
                    break;
                case MAX:
                    if (tupleValue>resultantTable.get(key)){
                        resultantTable.put(key,tupleValue);
                    }
                    break;
                case SUM:
                    int newValue = tupleValue + resultantTable.get(key);
                    resultantTable.put(key,newValue);
                    break;
                case AVG:
                    int sum = tupleValue + resultantTable.get(key);
                    resultantTable.put(key,sum);
                    break;
                case COUNT:
                    int count = 1 + resultantTable.get(key);
                    resultantTable.put(key,count);
                    break;
            }
            countTable.put(key,1+countTable.get(key));
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
                entry.setField(0, new IntField(resultantTable.get(key)));
            }
            else{
                entry.setField(0, key);
                if (operation == Op.AVG){
                    entry.setField(1, new IntField(resultantTable.get(key)/countTable.get(key)));
                }
                else{
                    entry.setField(1, new IntField(resultantTable.get(key)));
                }
                
            }
            tupleList.add(entry);
        }
        return new TupleIterator(descriptor, tupleList);
    }

}

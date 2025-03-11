package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
    private OpIterator source;
    private int fieldIndexToGB;
    private int fieldIndexOfAggregate;
    private Aggregator.Op operation;
    private Aggregator aggregator;
    private OpIterator aggIterator;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.source = child;
        this.fieldIndexOfAggregate = afield;
        this.fieldIndexToGB = gfield;
        this.operation = aop;
        Type typeOfAgg = source.getTupleDesc().getFieldType(fieldIndexOfAggregate);
        if (typeOfAgg == Type.INT_TYPE){
            if (fieldIndexToGB == -1){
                aggregator = new IntegerAggregator(Aggregator.NO_GROUPING, null, fieldIndexOfAggregate, operation);
            }
            else{
                Type typeOfGB = source.getTupleDesc().getFieldType(fieldIndexToGB);
                aggregator = new IntegerAggregator(fieldIndexToGB, typeOfGB, fieldIndexOfAggregate, operation);
            }
        }
        else{
            if (fieldIndexToGB == -1){
                aggregator = new StringAggregator(Aggregator.NO_GROUPING, null, fieldIndexOfAggregate, operation);
            }
            else{
                Type typeOfGB = source.getTupleDesc().getFieldType(fieldIndexToGB);
                aggregator = new StringAggregator(fieldIndexToGB, typeOfGB, fieldIndexOfAggregate, operation);
            }
        }
        
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return fieldIndexToGB;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if (fieldIndexToGB == -1){
            return null;
        }
        return source.getTupleDesc().getFieldName(fieldIndexToGB);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return fieldIndexOfAggregate;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return source.getTupleDesc().getFieldName(fieldIndexOfAggregate);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return operation;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        source.open();
        while (source.hasNext()){
            aggregator.mergeTupleIntoGroup(source.next());
        }
        aggIterator = aggregator.iterator();
        aggIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggIterator.hasNext()){
            return aggIterator.next();
        }
        else{
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc childTd = source.getTupleDesc();
        Type aggType = childTd.getFieldType(fieldIndexOfAggregate);
        String aggFieldName = nameOfAggregatorOp(operation) + " (" + childTd.getFieldName(fieldIndexOfAggregate) + ")";

        Type groupType = childTd.getFieldType(fieldIndexToGB);
        String groupFieldName = childTd.getFieldName(fieldIndexToGB);
        if (fieldIndexToGB == -1){
            return new TupleDesc(new Type[]{aggType}, new String[]{aggFieldName});
        }
        else{
            return new TupleDesc(new Type[]{groupType, aggType}, new String[]{groupFieldName, aggFieldName});
        }
    }

    public void close() {
        super.close();
        source.close();
        aggIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{aggIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        aggIterator = children[0];
    }

}

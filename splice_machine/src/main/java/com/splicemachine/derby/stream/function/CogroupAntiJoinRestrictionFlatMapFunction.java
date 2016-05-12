package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.sparkproject.guava.collect.Iterables;
import org.sparkproject.guava.collect.Sets;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Set;

/**
 * Created by jleach on 4/30/15.
 */
public class CogroupAntiJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, Tuple2<Iterable<LocatedRow>,Iterable<LocatedRow>>,LocatedRow> {
    protected AntiJoinRestrictionFlatMapFunction antiJoinRestrictionFlatMapFunction;
    protected LocatedRow leftRow;
    public CogroupAntiJoinRestrictionFlatMapFunction() {
        super();
    }

    public CogroupAntiJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<LocatedRow> call(Tuple2<Iterable<LocatedRow>, Iterable<LocatedRow>> tuple) throws Exception {
        checkInit();
        Set<LocatedRow> rightSide = Sets.newHashSet(tuple._2); // Memory Issue, HashSet ?
        Iterable<LocatedRow> returnRows = new ArrayList<>();
        for(LocatedRow a_1 : tuple._1){
            returnRows= Iterables.concat(returnRows, antiJoinRestrictionFlatMapFunction.call(new Tuple2<>(a_1, rightSide)));
        }
        return returnRows;
    }

    @Override
    protected void checkInit() {
        if (!initialized)
            antiJoinRestrictionFlatMapFunction = new AntiJoinRestrictionFlatMapFunction(operationContext);
        super.checkInit();
    }
}
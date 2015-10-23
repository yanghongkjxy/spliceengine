package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.tools.IteratorRegionScanner;
import com.splicemachine.derby.tools.TestDataEncoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.CellType;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.Filter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 10/12/15
 */
public class CheckpointReaderTest{
    private static final KryoPool kp = new KryoPool(10);
    @Test
    public void insertVacuumCommitted() throws Exception{
        /*
         * Insert a single row -> vacuum read -> correct
         */
        ExecRow template = new ValueRow(3);
        template.setColumn(1,new SQLInteger(1));
        template.setColumn(2,new SQLVarchar("hello"));
        template.setColumn(3,new SQLBoolean(false));
        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,template,kp);
        tde.encoder().insert(2,template).commit(2,3).reset();

        long mat = 4;
        CheckpointReader.ReadHandler handler = new CheckpointReader.ReadHandler(){
            @Override
            public void onDelete(ByteSlice rowKey){
                Assert.fail("Should not call delete!");
            }
        };
        SDataLib dataLib = new HDataLib();
        RowAccumulator<Cell> acc= new HRowAccumulator<>(dataLib,EntryPredicateFilter.EMPTY_PREDICATE,
                new EntryDecoder(),false);
        SIFilter filter = buildSIFilter(acc);
        MeasuredRegionScanner mrs = this.buildScanner(buildRowResults(10,tde));

        CheckpointReader cr = new CheckpointReader(mat,handler,acc,filter,dataLib,mrs,Metrics.noOpMetricFactory());

        Assert.assertTrue("Did not read a row!",cr.readNext());
        //we don't know anything about the row key(no pks), so we just make sure it's there
        Assert.assertNotNull("No row key found!",cr.currentRowKey());

    }

    @Test
    public void insertVacuumUncommitted() throws Exception{
        /*
         * Insert a single row -> vacuum read -> correct
         */
        ExecRow template = new ValueRow(3);
        template.setColumn(1,new SQLInteger(1));
        template.setColumn(2,new SQLVarchar("hello"));
        template.setColumn(3,new SQLBoolean(false));
        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,template,kp);
        tde.encoder().insert(2,template).reset();

        long mat = 4;
        CheckpointReader.ReadHandler handler = new CheckpointReader.ReadHandler(){
            @Override
            public void onDelete(ByteSlice rowKey){
                Assert.fail("Should not call delete!");
            }
        };
    }

    @Test
    public void insertManyVacuum() throws Exception{
        /*
         * Insert many rows -> vacuum read -> correct
         */

    }

    @Test
    public void insertUpdateVacuum() throws Exception{
        /*
         * Insert -> Update -> Vacuum read -> correct
         */

    }

    @Test
    public void insertDeleteVacuum() throws Exception{
        /*
         * Insert -> Delete -> Vacuum read -> no rows
         */

    }

    @Test
    public void insertDeleteInsertVacuum() throws Exception{
        /*
         * Insert -> Delete -> Insert -> Vacuum read -> correct
         */

    }

    @Test
    public void manyRowsDeleteOneVacuum() throws Exception{
        /*
         * Insert 3 rows -> Delete first row -> Vacuum read -> correct
         */

    }

    @Test
    public void manyRowsDeleteMiddleVacuum() throws Exception{
        /*
         * Insert 3 rows -> Delete second row -> Vacuum read -> correct
         */

    }

    @Test
    public void manyRowsDeleteLastVacuum() throws Exception{
        /*
         * Insert 3 rows -> Delete last row -> Vacuum read -> correct
         */

    }

    @Test
    public void manyRowsUpdateOneVacuum() throws Exception{
        /*
         * Insert 3 rows -> Update first row -> Vacuum read -> correct
         */

    }

    @Test
    public void manyRowsUpdateMiddleVacuum() throws Exception{
        /*
         * Insert 3 rows -> Update second row -> Vacuum read -> correct
         */

    }

    @Test
    public void manyRowsUpdateLastVacuum() throws Exception{
        /*
         * Insert 3 rows -> Update last row -> Vacuum read -> correct
         */

    }

    /* **************************************************************************************************/
    /*private helper methods*/
    private SIFilter<Cell> buildSIFilter(final RowAccumulator<Cell> accumulator){
        final Set<Long> tombstoneTimestamps = new TreeSet<>();
        final Set<Long> antiTombstoneTimestamps = new TreeSet<>();
        return new SIFilter<Cell>(){
            @Override public void nextRow(){
                accumulator.reset();
                tombstoneTimestamps.clear();
            }
            @Override public RowAccumulator getAccumulator(){ return accumulator; }

            @Override
            public Filter.ReturnCode filterKeyValue(Cell kv) throws IOException{
                CellType type=getKeyValueType(kv);
                if(type==CellType.COMMIT_TIMESTAMP){
                    return Filter.ReturnCode.SKIP;
                }
                if(type==CellType.FOREIGN_KEY_COUNTER){
                        /* Transactional reads always ignore this column, no exceptions. */
                    return Filter.ReturnCode.SKIP;
                }

                switch(type){
                    case TOMBSTONE:
                        tombstoneTimestamps.add(kv.getTimestamp());
                        return Filter.ReturnCode.SKIP;
                    case ANTI_TOMBSTONE:
                        antiTombstoneTimestamps.add(kv.getTimestamp());
                        return Filter.ReturnCode.SKIP;
                    case USER_DATA:
                        long thisTs = kv.getTimestamp();
                        for(Long tt : tombstoneTimestamps){
                            if(thisTs < tt) return Filter.ReturnCode.SKIP;
                        }
                        for(Long at : antiTombstoneTimestamps){
                            if(thisTs < at) return Filter.ReturnCode.SKIP;
                        }
                        if (!accumulator.isFinished() && accumulator.isOfInterest(kv)) {
                            if (!accumulator.accumulate(kv)) {
                                return Filter.ReturnCode.NEXT_ROW;
                            }
                        }
                        return Filter.ReturnCode.INCLUDE;
                    default:
                        //TODO -sf- do better with this?
                        throw new AssertionError("Unexpected Data type: "+type);
                }
            }

            @Override
            public TxnFilter<Cell> unwrapFilter(){
                return null;
            }
        };
    }
    private MeasuredRegionScanner<Cell> buildScanner(List<List<Cell>> testData) throws IOException{
        return new IteratorRegionScanner(testData);
    }

    private CellType getKeyValueType(Cell keyValue) {
        if (CellUtils.singleMatchingQualifier(keyValue,FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES)) {
            return CellType.COMMIT_TIMESTAMP;
        } else if (CellUtils.singleMatchingQualifier(keyValue, FixedSpliceConstants.PACKED_COLUMN_BYTES)) {
            return CellType.USER_DATA;
        } else if (CellUtils.singleMatchingQualifier(keyValue, FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES)) {
            if (CellUtils.matchingValue(keyValue, FixedSIConstants.EMPTY_BYTE_ARRAY)) {
                return CellType.TOMBSTONE;
            } else if (CellUtils.matchingValue(keyValue,FixedSIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES)) {
                return CellType.ANTI_TOMBSTONE;
            }
        } else if (CellUtils.singleMatchingQualifier(keyValue,FixedSIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return CellType.FOREIGN_KEY_COUNTER;
        }
        return CellType.OTHER;
    }

    private List<List<Cell>> buildRowResults(int batchSize,TestDataEncoder tde){
        Map<byte[], List<Cell>> listMap=tde.encodedData();
        List<List<Cell>> data = new ArrayList<>(listMap.size());
        for(List<Cell> values : listMap.values()){
            List<List<Cell>> partitions =Lists.partition(values,batchSize);
            data.addAll(partitions);
        }
        return data;
    }
}
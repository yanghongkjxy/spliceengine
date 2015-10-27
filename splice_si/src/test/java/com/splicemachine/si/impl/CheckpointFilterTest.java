package com.splicemachine.si.impl;

import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 10/8/15
 */
public class CheckpointFilterTest{

    @Test
    public void testReturnsIncludeNoCheckpointCell() throws Exception{
        byte[] rowKey =Encoding.encode("rowKey");
        byte[] value = Encoding.encode("value");
        Cell data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSpliceConstants.PACKED_COLUMN_BYTES,1l, value);

        TxnFilter<Cell> txnFilter=alwaysIncludeTxnFilter();
        CheckpointFilter checkpointFilter=new CheckpointFilter(txnFilter,100);
        Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.INCLUDE,returnCode);
    }

    @Test
    public void testReturnsSkipForCheckpointCell() throws Exception{
        byte[] rowKey =Encoding.encode("rowKey");
        byte[] value = Encoding.encode(10);
        Cell data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,1l, value);

        TxnFilter<Cell> txnFilter=alwaysIncludeTxnFilter();
        CheckpointFilter checkpointFilter=new CheckpointFilter(txnFilter,100);
        Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
    }

    @Test
    public void testManyCheckpointsSwitchToSeekMode() throws Exception{
        byte[] rowKey =Encoding.encode("rowKey");

        TxnFilter<Cell> txnFilter=alwaysIncludeTxnFilter();
        CheckpointFilter checkpointFilter=new CheckpointFilter(txnFilter,3);
        for(int i=6;i>=3;i--){
            Cell data=new KeyValue(rowKey,
                    FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,i,Encoding.encode(10));
            Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
            Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
        }
        //this should trigger a switch to seek mode, and so should return NEXT_COL
        Cell data=new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,2,Encoding.encode(10));
        Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.NEXT_COL,returnCode);
    }

    @Test
    public void testReturnsSkipForCellWithTimestampLessThanCheckpointWhenBelowSeekThreshold() throws Exception{
        byte[] rowKey =Encoding.encode("rowKey");
        byte[] value = Encoding.encode(10);
        Cell data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,10l, value);

        TxnFilter<Cell> txnFilter=alwaysIncludeTxnFilter();
        CheckpointFilter checkpointFilter=new CheckpointFilter(txnFilter,1);
        Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,8l,Encoding.encode(9l));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,8l,Encoding.encode(9l));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,8l,Encoding.encode(9l));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSpliceConstants.PACKED_COLUMN_BYTES,8l,Encoding.encode(9l));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
    }

    @Test
    public void testSwitchesToSeekModeWhenLotsOfNonCheckpointCellsAreFound() throws Exception{
        byte[] rowKey =Encoding.encode("rowKey");
        byte[] value = Encoding.encode(10);
        Cell data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,10l, value);

        TxnFilter<Cell> txnFilter=alwaysIncludeTxnFilter();
        CheckpointFilter checkpointFilter=new CheckpointFilter(txnFilter,5);
        Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
        for(int i=8;i>=2;i--){
            data = new KeyValue(rowKey,
                    FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,i,Encoding.encode(9l));
            returnCode=checkpointFilter.filterKeyValue(data);
            Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
        }

        //should seek now
        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,1,Encoding.encode(9l));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.NEXT_COL,returnCode);
    }

    @Test
    public void testDoesNotSwitchToSeekModeWhenOnlyAFewCellsOfTheSameTimeAreSeen() throws Exception{
        byte[] rowKey =Encoding.encode("rowKey");
        byte[] value = Encoding.encode(10);
        Cell data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,10l, value);

        TxnFilter<Cell> txnFilter=alwaysIncludeTxnFilter();
        CheckpointFilter checkpointFilter=new CheckpointFilter(txnFilter,5);
        Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
        for(int i=8;i>=3;i--){
            data = new KeyValue(rowKey,
                    FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,i,Encoding.encode(9l));
            returnCode=checkpointFilter.filterKeyValue(data);
            Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
        }

        for(int i=8;i>=3;i--){
            data = new KeyValue(rowKey,
                    FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,i,Encoding.encode(9l));
            returnCode=checkpointFilter.filterKeyValue(data);
            Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
        }

        for(int i=8;i>=3;i--){
            data = new KeyValue(rowKey,
                    FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,i,Encoding.encode(9l));
            returnCode=checkpointFilter.filterKeyValue(data);
            Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
        }

        for(int i=8;i>=3;i--){
            data = new KeyValue(rowKey,
                    FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                    FixedSpliceConstants.PACKED_COLUMN_BYTES,i,Encoding.encode(9l));
            returnCode=checkpointFilter.filterKeyValue(data);
            Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
        }
    }

    @Test
    public void testIncludeForCellWithTimestampGreaterThanCheckpoint() throws Exception{
        byte[] rowKey =Encoding.encode("rowKey");
        byte[] value = Encoding.encode(10);
        Cell data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,10l, value);

        TxnFilter<Cell> txnFilter=alwaysIncludeTxnFilter();
        CheckpointFilter checkpointFilter=new CheckpointFilter(txnFilter,-1);
        Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,12l,Encoding.encode(9l));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.INCLUDE,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,12,Encoding.encode(9l));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.INCLUDE,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,12l,Encoding.encode(9l));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.INCLUDE,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSpliceConstants.PACKED_COLUMN_BYTES,12l,Encoding.encode(9l));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.INCLUDE,returnCode);
    }

    @Test
    public void testSkipsCheckpointsAfterHighestWithSkip() throws Exception{
        byte[] rowKey =Encoding.encode("rowKey");
        byte[] value = Encoding.encode(10);
        Cell data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,4l, value);

        TxnView txn = new ReadOnlyTxn(1l,1l,Txn.IsolationLevel.READ_UNCOMMITTED,Txn.ROOT_TRANSACTION,null,null,true);
        TxnFilter<Cell> txnFilter=new BaseTestTxnFilter(txn){
            @Override
            public Filter.ReturnCode filterKeyValue(Cell keyValue) throws IOException{
                Assert.assertEquals("Should not check transactionality!",4l,keyValue.getTimestamp());
                return Filter.ReturnCode.INCLUDE;
            }
        };

        CheckpointFilter checkpointFilter=new CheckpointFilter(txnFilter,100);
        Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,1l, Encoding.encode(9));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
    }

    @Test
    public void testDoesNotSkipCheckpointAfterReset() throws Exception{
        byte[] rowKey =Encoding.encode("rowKey");
        byte[] value = Encoding.encode(10);
        Cell data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,2l, value);

        final long[] correctTs = new long[]{2l};
        TxnView txn = new ReadOnlyTxn(1l,1l,Txn.IsolationLevel.READ_UNCOMMITTED,Txn.ROOT_TRANSACTION,null,null,true);
        TxnFilter<Cell> txnFilter=new BaseTestTxnFilter(txn){
            @Override
            public Filter.ReturnCode filterKeyValue(Cell keyValue) throws IOException{
                Assert.assertEquals("Should not check transactionality!",correctTs[0],keyValue.getTimestamp());
                return Filter.ReturnCode.INCLUDE;
            }
        };

        CheckpointFilter checkpointFilter=new CheckpointFilter(txnFilter,100);
        Filter.ReturnCode returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);

        data = new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,1l, Encoding.encode(9));
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);

        checkpointFilter.reset();
        correctTs[0] = data.getTimestamp();
        returnCode=checkpointFilter.filterKeyValue(data);
        Assert.assertEquals("Incorrect return code!",Filter.ReturnCode.SKIP,returnCode);
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/
    private TxnFilter<Cell> alwaysIncludeTxnFilter(){
        TxnView txn = new ReadOnlyTxn(1l,1l,Txn.IsolationLevel.READ_UNCOMMITTED,Txn.ROOT_TRANSACTION,null,null,true);
        return new BaseTestTxnFilter(txn){
            @Override
            public Filter.ReturnCode filterKeyValue(Cell keyValue) throws IOException{
                return Filter.ReturnCode.INCLUDE;
            }
        };
    }

    private static CellType getKeyValueType(Cell keyValue){
        if(CellUtil.matchingQualifier(keyValue,
                FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES)){
            return CellType.COMMIT_TIMESTAMP;
        }else if(CellUtil.matchingQualifier(keyValue,
                FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES)){
            return CellType.TOMBSTONE;
        }else if(CellUtil.matchingQualifier(keyValue,
                FixedSpliceConstants.PACKED_COLUMN_BYTES)){
            return CellType.USER_DATA;
        }else if(CellUtil.matchingQualifier(keyValue,
                FixedSIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)){
            return CellType.FOREIGN_KEY_COUNTER;
        }else if(CellUtil.matchingQualifier(keyValue,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES)){
            return CellType.CHECKPOINT;
        }else{
            return CellType.OTHER;
        }
    }

    private static abstract class BaseTestTxnFilter implements TxnFilter<Cell>{
        private final TxnView readTxn;
        public BaseTestTxnFilter(TxnView readTxn){
            this.readTxn=readTxn;
        }



        @Override public void nextRow(){ }
        @Override public Cell produceAccumulatedKeyValue(){ return null; }
        @Override public DataStore getDataStore(){ return null; }
        @Override public boolean isPacked(){ return false; }
        @Override public TxnSupplier getTxnSupplier(){ return null; }

        @Override public TxnView unwrapReadingTxn(){
            return readTxn;
        }

        @Override public boolean getExcludeRow(){ return false; }
        @Override
        public CellType getType(Cell keyValue) throws IOException{
            return getKeyValueType(keyValue);
        }
    }
}
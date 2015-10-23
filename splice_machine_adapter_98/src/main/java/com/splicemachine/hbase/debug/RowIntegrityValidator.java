package com.splicemachine.hbase.debug;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.CellTypeParser;
import com.splicemachine.si.impl.CommittedTxn;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/7/15
 */
class RowIntegrityValidator{
    private final TxnSupplier txnSupplier;
    private final CellTypeParser ctParser;

    private transient LongOpenHashSet checkpointCells = new LongOpenHashSet();
    private transient LongOpenHashSet emptyCheckpointCells = new LongOpenHashSet();
    private transient LongOpenHashSet commitTimestampCells = new LongOpenHashSet();
    private transient LongOpenHashSet tombstoneCells = new LongOpenHashSet();
    private transient LongOpenHashSet antiTombstoneCells = new LongOpenHashSet();
    private transient LongOpenHashSet userDataCells = new LongOpenHashSet();

    private long checkpointCount = 0;
    private long committedCheckpointCount = 0;
    private long commitTimestampCount = 0;
    private long userDataCount = 0;
    private long tombstoneCount = 0;
    private long antiTombstoneCount =0;
    private long totalCellCount =0;

    private long rolledBackRowCount = 0; //the number of rows which have been completely rolled back
    private long tombstonedRowCount = 0; //the number of rows with a tombstone at the top
    private long rowsVisited = 0;

    private boolean inRow = false;

    private ByteSlice currentRowKey = new ByteSlice();
    private String rkString;

    public RowIntegrityValidator(TxnSupplier txnSupplier,CellTypeParser ctParser){
        this.txnSupplier=txnSupplier;
        this.ctParser=ctParser;
    }

    public void rowDone() throws IOException{
        postValidateCheckpointCells();
        postValidateUserCells();
        postValidateTombstoneCells();
        postValidateAntiTombstoneCells();

        checkpointCells.clear();
        emptyCheckpointCells.clear();
        commitTimestampCells.clear();
        tombstoneCells.clear();
        antiTombstoneCells.clear();
        userDataCells.clear();
        inRow=false;
        rkString =null;
    }

    public void validate(Result row){
        if(row==null) return;
        if(!inRow){
            rowsVisited++;
            setRowKey(row);
        }

        Cell[] cells = row.rawCells();
        for(Cell c : cells){
            long ts=c.getTimestamp();
            switch(ctParser.parseCellType(c)){
                case COMMIT_TIMESTAMP:
                    validateCommitTimestamp(c,ts);
                    break;
                case TOMBSTONE:
                    validateTombstone(c,ts);
                    break;
                case ANTI_TOMBSTONE:
                    validateAntiTombstone(c,ts);
                    break;
                case USER_DATA:
                    validateUserData(c,ts);
                    break;
                case FOREIGN_KEY_COUNTER:
                    break;
                case CHECKPOINT:
                    validateCheckpoint(c,ts);
                    break;
                case OTHER:
                    break;
            }
        }
        totalCellCount+=cells.length;
    }

    public void printSummaryInformation(){
        System.out.printf("Rows: %d%n",rowsVisited);
        System.out.printf("Cells: %d%n",totalCellCount);
        if(rowsVisited>0){
            System.out.printf("Avg User Cell Count: %d%n",(userDataCount)/rowsVisited);
            System.out.printf("Avg Checkpoint Count: %d%n",(checkpointCount)/rowsVisited);
            System.out.printf("Avg Committed Checkpoint Count: %d%n",committedCheckpointCount/rowsVisited);
            System.out.printf("Avg Commit Timestamp Count: %d%n",(commitTimestampCount+1)/rowsVisited);
            System.out.printf("Avg Tombstone Count: %d%n",tombstoneCount/rowsVisited);
            System.out.printf("Avg AntiTombstone Count: %d%n",antiTombstoneCount/rowsVisited); //should be 0

            System.out.printf("Committed Checkpoints/Total Checkpoints: %d%n",(committedCheckpointCount/checkpointCount));
            System.out.printf("Uncommitted user versions: %d%n",Math.max(0,userDataCount-checkpointCount-commitTimestampCount));
            System.out.printf("Committed versions/User versions: %f%n",Math.min(1d,((checkpointCount+commitTimestampCount)/(double)userDataCount)));
        }else{
            System.out.printf("Avg User Cell Count: %d%n",0);
            System.out.printf("Avg Checkpoint Count: %d%n",0);
            System.out.printf("Avg Committed Checkpoint Count: %d%n",0);
            System.out.printf("Avg Commit Timestamp Count: %d%n",0);
            System.out.printf("Avg Tombstone Count: %d%n",0);
            System.out.printf("Avg AntiTombstone Count: %d%n",0);

            System.out.printf("Committed Checkpoints/Total Checkpoints: %d%n",0);
            System.out.printf("Uncommitted user versions: %d%n",0);
            System.out.printf("Committed versions/User versions: %.3f%n",0d);
        }

    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void validateCheckpoint(Cell c,long ts){
        if(checkpointCells.contains(ts))
            System.out.printf("Row <%s>: contains checkpoint cell %d twice!%n",rowKeyString(),ts);
        checkpointCells.add(ts);
        checkpointCount++;

        if(c.getValueLength()==8){
            committedCheckpointCount++;
            cacheTxn(c,ts);
        }else{
            emptyCheckpointCells.add(ts);
        }
    }

    private void validateUserData(Cell c,long ts){
        if(userDataCells.contains(ts))
            System.out.printf("Row <%s>: contains user data cell %d twice!%n",rowKeyString(),ts);
        userDataCells.add(ts);
        userDataCount++;

    }

    private void validateAntiTombstone(Cell c,long ts){
        if(antiTombstoneCells.contains(ts))
            System.out.printf("Row <%s>: contains anti tombstone cell %d twice!%n",rowKeyString(),ts);
        antiTombstoneCount++;
    }

    private void validateTombstone(Cell c,long ts){
        if(tombstoneCells.contains(ts))
            System.out.printf("Row <%s>: contains tombstone cell %d twice!%n",rowKeyString(),ts);
        tombstoneCount++;
    }

    private void validateCommitTimestamp(Cell c,long ts){
        if(commitTimestampCells.contains(ts))
            System.out.printf("Row <%s>: contains commit timestamp cell %d twice!%n",rowKeyString(),ts);
        commitTimestampCells.add(ts);
        commitTimestampCount++;

        if(c.getValueLength()!=8)
            System.out.printf("Row <%s>: commit timestamp %d has improper contents!",rowKeyString(),ts);

        TxnView txnView=cacheTxn(c,ts);
        if(checkpointCells.contains(commitTimestampCount)){
            long cts = Bytes.toLong(c.getValueArray(),c.getValueOffset(),c.getValueLength());
            CommittedTxn committedTxn=new CommittedTxn(ts,cts);
            if(!txnView.equals(committedTxn)){
                System.out.printf("Row <%s>: Commit timestamp and checkpoint cells at timestamp <%d> do not agree%n",rowKeyString(),ts);
            }
        }
    }

    private TxnView cacheTxn(Cell c,long ts){
        long cts = Bytes.toLong(c.getValueArray(),c.getValueOffset(),c.getValueLength());
        CommittedTxn toCache=new CommittedTxn(ts,cts);
        txnSupplier.cache(toCache);
        return toCache;
    }

    private String rowKeyString(){
        if(rkString==null){
            rkString = Bytes.toStringBinary(currentRowKey.array(),currentRowKey.offset(),currentRowKey.length());
        }
        return rkString;
    }

    private void setRowKey(Result row){
        currentRowKey.set(row.getRow());
        rkString = null;
    }

    private void postValidateCheckpointCells() throws IOException{
        for(LongCursor lc:checkpointCells){
            long ts = lc.value;

            TxnView txn = txnSupplier.getTransaction(ts);
            if(txn.getEffectiveState()==Txn.State.ROLLEDBACK){
                if(!emptyCheckpointCells.contains(ts)){
                    System.out.printf("Row <%s>: Version <%d> has a non-empty checkpoint, but is rolled back%n",rowKeyString(),ts);
                }
            }
        }
    }

    private void postValidateAntiTombstoneCells(){
        for(LongCursor lc:antiTombstoneCells){
            long ts = lc.value;
            if(!checkpointCells.contains(ts)){
                System.out.printf("Row<%s>: Version <%d> has an anti tombstone, but not a checkpoint%n",rowKeyString(),ts);
            }
        }
    }

    private void postValidateTombstoneCells() throws IOException{
        for(LongCursor lc:tombstoneCells){
            long ts = lc.value;
            TxnView txnView = txnSupplier.getTransaction(ts);
            if(txnView.getEffectiveState()==Txn.State.ROLLEDBACK){
                if(commitTimestampCells.contains(ts)){
                    System.out.printf("Row<%s>: Version <%d> contains a rolled back tombstone and a commit timestamp%n",rowKeyString(),ts);
                }
            }
            commitTimestampCells.remove(ts);
            if(checkpointCells.contains(ts)){
                System.out.printf("Row<%s>: Version <%d> contains a tombstone and a checkpoint cell%n",rowKeyString(),ts);
            }
            if(antiTombstoneCells.contains(ts)){
                System.out.printf("Row<%s>: Version <%d> contains a tombstone and an antiTombstone cell%n",rowKeyString(),ts);
            }
        }
    }

    private void postValidateUserCells() throws IOException{
        for(LongCursor uc:userDataCells){
            long timestamp = uc.value;
            if(tombstoneCells.contains(timestamp)){
                System.out.printf("Row<%s>: Version <%d> contains a user cell and a tombstone%n",rowKeyString(),timestamp);
            }
            TxnView txnView = txnSupplier.getTransaction(timestamp);
            if(txnView.getEffectiveState()==Txn.State.ROLLEDBACK){
                if(commitTimestampCells.contains(timestamp)){
                    System.out.printf("Row<%s>: Version <%d> contains a rolled back user cell and a commit timestamp%n",rowKeyString(),timestamp);
                }
            }
            commitTimestampCells.remove(timestamp);
            checkpointCells.remove(timestamp);
        }
    }
}

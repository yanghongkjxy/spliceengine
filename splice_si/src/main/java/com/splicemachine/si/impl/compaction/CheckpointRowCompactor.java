package com.splicemachine.si.impl.compaction;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.CellType;
import com.splicemachine.si.impl.CellTypeParser;
import com.splicemachine.si.impl.CommittedTxn;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * A RowCompactor which adds in Commit timestamp, removes rolled back records, and so on.
 *
 * @author Scott Fines
 *         Date: 10/22/15
 */
public class CheckpointRowCompactor implements RowCompactor{
    private final long mat;
    private final CellTypeParser ctParser;
    private final TxnSupplier transactionStore;
    private final RollForward rollForward;
    /*
     * A Bitmap for all the versions which already have a commit timestamp field set.
     *
     * Note that we don't actually use a BitSet here. That's because we are likely to have entries like
     * 12345678 which are very large (in the millions), but we are unlikely to have any set bits below that value.
     * The traditional java.util.BitSet will use 1 bit for every 0-position below the lowest set location (and
     * for every non-set location). When the timestamp values are in the millions, this will means MILLIONS of
     * wasted bits(e.g. Megabytes of wasted data), which is obviously bad. To adjust for this, we use
     * a hashtable of longs instead.
     *
     */
    private BitSet setCommitTimestampVersions = new BitSet();
    private BitSet emptyCheckpoints = new BitSet();
    private BitSet visitedCheckpoints = new BitSet();
    /*
     * This is the timestamp to which we will write the accumulated data.
     */
    private long checkpointVersion;
    /*
     * This is the timestamp such that, if any cell comes in with a timestamp <= this point,
     * just throw it away directly without doing anything.
     */
    protected long discardPoint = -1l;

    protected CompactionRow compactionRow = new CompactionRow();

    private CellType lastCellType;
    private RowAccumulator<Cell> accumulator;
    private ByteSlice currentRowKey = new ByteSlice();


    public CheckpointRowCompactor(long mat,
                                  CellTypeParser ctParser,
                                  RowAccumulator<Cell> accumulator,
                                  TxnSupplier transactionStore){
        this(mat,ctParser,accumulator,transactionStore,NoopRollForward.INSTANCE);
    }

    public CheckpointRowCompactor(long mat,
                                  CellTypeParser ctParser,
                                  RowAccumulator<Cell> accumulator,
                                  TxnSupplier transactionStore,
                                  RollForward rollForward){
        this.mat=mat;
        this.ctParser=ctParser;
        this.transactionStore=transactionStore;
        this.accumulator = accumulator;
        this.rollForward = rollForward;

    }

    @Override
    public void reverse() throws IOException{
        setAccumulatedValue();
        compactionRow.clearDiscardedData(checkpointVersion);
        reset();
    }

    @Override
    public boolean placeCells(List<Cell> destination,int limit){
        if(limit>0){
            boolean drain=compactionRow.drain(destination,limit);
            if(!drain){
                currentRowKey.reset(); //reset the row key
            }
            return drain;
        }else{
            compactionRow.drain(destination);
            currentRowKey.reset(); //reset the row key
            return false;
        }
    }

    @Override
    public ByteSlice currentRowKey(){
        return currentRowKey;
    }

    @Override
    public boolean isEmpty(){
        return compactionRow.isEmpty() && !accumulator.hasAccumulated();
    }

    @Override
    public void addCell(Cell cell) throws IOException{
        if(currentRowKey.length()<=0) //set the current row key for future usage
            currentRowKey.set(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
        /*
         * We automatically discard any record which falls below our discard point.
         */
        if(cell.getTimestamp()<discardPoint) return;

        CellType cellType=ctParser.parseCellType(cell);
        if(lastCellType==null)
            lastCellType = cellType;
        switch(cellType){
            case CHECKPOINT:
                processCheckpoint(cell);
                break;
            case COMMIT_TIMESTAMP:
                processCommitTimestamp(cell);
                break;
            case TOMBSTONE:
                processTombstone(cell);
                break;
            case ANTI_TOMBSTONE:
                /*
                 * An Anti-tombstone really just indicates the presence of an insert after a delete; then we stop
                 * looking at cells which are blow the anti-tombstone. since inserts are all checkpointed, this is
                 * a bit redundant--we will stop processing records which are below the checkpoint, which is the same
                 * location as the anti-tombstone. Therefore, we can discard these entries in all cases. We obviously
                 * don't discard the actual data, just the anti-tombstone element.
                 *
                 * However, on the off chance that we don't have a checkpoint cell, we keep this around just to be
                 * careful (It costs very little to check, and it helps us to feel confident about the correctness
                 * of our algorithm)
                 */
                if(!visitedCheckpoints.contains(cell.getTimestamp()))
                    compactionRow.append(cell);
                break;
            case USER_DATA:
                processUserData(cell);
                break;
            case FOREIGN_KEY_COUNTER:
                /*
                 * FK counters occur after the user data, so if we see one, then we know that the user data
                 * is done after this. Time to add the accumulated record.
                 *
                 * DON'T BREAK HERE: Otherwise you'll throw away the FK counter, which would be bad news
                 */
                setAccumulatedValue();
            default:
                compactionRow.append(cell); //we don't know what to do with this, so just throw it in
        }
        lastCellType = cellType;
    }


    /**
     * Accumulate this user data cell.
     *
     * @param cell a user cell. Assumed to be a user-cell, and that it is not rolled back (there is no guarantee
     *             that it belongs to a globally committed txn, however).
     * @return {@code true} if the user cell was accumulated, {@code false} otherwise. If {@code true}
     * is returned, then the user cell will <em>not</em> be appended to the compacted row.
     * @throws IOException
     */
    protected boolean accumulateUserData(Cell cell) throws IOException{
        long ts = cell.getTimestamp();
        if(visitedCheckpoints.contains(ts)){
            /*
             * This is a checkpoint cell. One of two situations can occur:
             *
             * 1. We haven't accumulated anything (i.e. the checkpoint IS the highest version <MAT)
             * 2. We have accumulated.
             *
             * If in scenario 2, then we need to accumulate and discard this record. Otherwise, we move the discard
             * point to just below us, and append our value.
             */
            if(accumulator.hasAccumulated()){
                accumulate(cell);
                return true;
            }
        }else{
            accumulate(cell);
            return true;
        }
        return false;
    }

    /**
     * Adjust the discard point for this row based on whether or not it's a tombstone.
     *
     * @param ts the timestamp for the tombstone transaction
     * @return {@code true} if the discard point was adjusted, false otherwise. If the discard point was adjusted,
     * then this tombstone will <em>not</em> be appended to the list
     */
    protected boolean adjustTombstoneDiscardPoint(long ts){
        /*
         * We know that this element must be above the discard point
         * (otherwise, it would be discarded), and we know it can't be a checkpoint cell
         * (we don't checkpoint deletes), so it must be higher than the current discard point.
         *
         * Since a tombstone indicates that you should discard all records below it,
         * we can treat it like a checkpoint cell, and move the discard point up to (and including)
         * this location.
         *
         * Note: doing this introduces a possible race condition between Readers and writers:
         * The scenario is as follows:
         *
         * 1. T1 begins
         * 2. T2 begins
         * 3. T1 writes tombstone
         * 4. T1 commits (moving MAT to T2)
         * 5. T2 reads row. Because T1 commits after T2 starts, T2 does not see tombstone, but sees version
         * below instead.
         * 6. Compaction occurs, and the version below the tombstone is removed.
         * 7. T2 reads same row again. Because all versions at or below the tombstone are removed, the
         * tombstone "appears" even though it wasn't visible before.
         *
         * This is an example of the Phantom Reads ANSI SQL phenomenon(although in reverse of
         * the normally accepted example). As long as the reader is not in SERIALIZABLE mode,
         * then this artifact is allowed by the spec, so we can safely do this. However, if there are any
         * SERIALIZABLE reads, then we cannot remove the row until the SERIALIZABLE read finishes (which we would
         * indicate via the presence of some form of lock). As of Dec. 2015, SpliceMachine doesn't support
         * ANSI SERIALIZABLE mode, so we are safe to do this activity, but when we *do* support the mode we'll
         * need to adjust this approach.
         */
        discardPoint = ts;
        compactionRow.clearDiscardedData(discardPoint+1);
        return false;
    }
    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void setAccumulatedValue() throws IOException{
        if(!accumulator.hasAccumulated()) return; //nothing to do
        byte[] result = accumulator.result();

        TxnView checkpointTxn = transactionStore.getTransaction(checkpointVersion);
        Cell valueCell = new KeyValue(currentRowKey.array(),currentRowKey.offset(),currentRowKey.length(),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,0,FixedSpliceConstants.DEFAULT_FAMILY_BYTES.length,
                FixedSpliceConstants.PACKED_COLUMN_BYTES,0,FixedSpliceConstants.PACKED_COLUMN_BYTES.length,
                checkpointVersion,KeyValue.Type.Put,
                result,0,result.length);
        addCheckpoint(valueCell,checkpointTxn);
        addCommitTimestamp(valueCell,checkpointTxn);
        compactionRow.append(valueCell);
        accumulator.reset();
        currentRowKey.reset();
    }

    private void processTombstone(Cell cell) throws IOException{
        long ts = cell.getTimestamp();
        TxnView txn = transactionStore.getTransaction(ts);
        if(txn.getEffectiveState()==Txn.State.ROLLEDBACK) return; //discard rolled back data
        if(ts<mat){
            if(adjustTombstoneDiscardPoint(ts)) return;
        }
        /*
         * This data is required, since it could be read by another transaction. Hence, we want to append it.
         * However, if the commit timestamp cell isn't yet present, we need to add it in
         */
        if(txn.getEffectiveState()==Txn.State.COMMITTED && !setCommitTimestampVersions.contains(ts)){
            addCommitTimestamp(cell,txn);
        }
        compactionRow.append(cell);
    }


    private void addCommitTimestamp(Cell c,TxnView txn){
        if(txn.getEffectiveState()!=Txn.State.COMMITTED) return;
        byte[] qualifier=FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES;
        byte[] commitTs = Bytes.toBytes(txn.getGlobalCommitTimestamp());
        Cell ct = new KeyValue(c.getRowArray(),c.getRowOffset(),c.getRowLength(),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,0,FixedSpliceConstants.DEFAULT_FAMILY_BYTES.length,
                qualifier,0,qualifier.length,
                txn.getTxnId(),
                KeyValue.Type.Put,
                commitTs,0,commitTs.length);
        compactionRow.insertCommitTimestamp(ct);
        setCommitTimestampVersions.add(c.getTimestamp());
        rollForward.recordResolved(c.getRowArray(),c.getRowOffset(),c.getRowLength(),txn.getTxnId());
    }

    private void addCheckpoint(Cell c,TxnView txn){
        byte[] qualifier=FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES;
        long globalCommitTimestamp=txn.getGlobalCommitTimestamp();
        byte[] commitTs;
        if(globalCommitTimestamp>0l){
            commitTs=Bytes.toBytes(globalCommitTimestamp);
        }else{
            commitTs =FixedSIConstants.EMPTY_BYTE_ARRAY;
        }
        Cell ct = new KeyValue(c.getRowArray(),c.getRowOffset(),c.getRowLength(),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,0,FixedSpliceConstants.DEFAULT_FAMILY_BYTES.length,
                qualifier,0,qualifier.length,
                txn.getTxnId(),
                KeyValue.Type.Put,
                commitTs,0,commitTs.length);
        compactionRow.insert(ct);
    }

    private void processCommitTimestamp(Cell cell){

        long timestamp=cell.getTimestamp();
        if(!transactionStore.transactionCached(timestamp)){
            if(cell.getValueLength()!=8){
                /*
                 * Programmer protection here: Just in case someone doesn't properly construct the
                 * commit timestamp. In that case, we disregard this cell and rebuild it properly.
                 */
                return;
            }
            long globalCommitTs=Bytes.toLong(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
            TxnView txn  = new CommittedTxn(timestamp,globalCommitTs);
            transactionStore.cache(txn);
            setCommitTimestampVersions.set(timestamp);
        }
        if(timestamp>=checkpointVersion){
            //keep the CT around
            compactionRow.appendCommitTimestamp(cell);
        }
    }

    private void processUserData(Cell cell) throws IOException{
        if(lastCellType==CellType.TOMBSTONE){
            //we were looking at tombstone data before this, so we need to reset the commit timestamp cell
            compactionRow.resetCommitTimestampPointers();
        }
        long ts = cell.getTimestamp();
        TxnView txn = transactionStore.getTransaction(ts); //hopefully this is cached already
        if(txn.getEffectiveState()==Txn.State.ROLLEDBACK) return; //discard rolled back data
        if(ts<mat){
            /*
             * this element is below the MAT, but above the discard point (otherwise it would already be
             * discarded). Therefore, we know it's committed, so we just accumulate this into the checkpoint
             * element, and discard the precise cell
             */
            if(checkpointVersion<ts){
                checkpointVersion=ts;
            }
            if(accumulateUserData(cell)) return;
        }

        /*
         * The data is above the mat,so we have to keep it. Make sure it has a commit timestamp, though. And
         * if it is a checkpoint cell, make sure that the checkpoint cell is populated with the commit timestamp
         */
        if(txn.getEffectiveState()==Txn.State.COMMITTED){
            if(emptyCheckpoints.contains(ts)){
                addCheckpoint(cell,txn);
                emptyCheckpoints.remove(ts);
            } else if(!visitedCheckpoints.contains(ts)){ //only add a commit timestamp if we aren't a checkpoint
                if(!setCommitTimestampVersions.contains(ts)){
                    addCommitTimestamp(cell,txn);
                }
            }
        }else{
            //add back in the empty checkpoint
            if(emptyCheckpoints.contains(ts)){
                addCheckpoint(cell,txn);
                emptyCheckpoints.remove(ts);
            }
        }
        compactionRow.append(cell);
    }


    private void accumulate(Cell cell) throws IOException{
        if(!accumulator.isFinished() && accumulator.isOfInterest(cell)){
            accumulator.accumulate(cell);
        }
    }

    private void processCheckpoint(Cell cell) throws IOException{
        long ts = cell.getTimestamp();
        visitedCheckpoints.set(ts);
        if(ts<mat){
            /*
             * We don't normally need this entry, so we will discard it. It's always possible that this is
             * actually the highest checkpoint cell < mat, in which case we will be re-creating it. However,
             * when we do this we may ALSO be appending the commit timestamp to the cell (if it's empty), in which
             * case we would need to recreate anyway, so to simplify logic, we ignore this entry, and recreate it
             * later if we need it.
             */
            if(discardPoint<0){
                /*
                 * The discard point was not set yet, so this is the first checkpoint that we have seen which is
                 * less than the Mat. Therefore, anything which falls below this timestamp can be discarded (since
                 * the information is wholly contained in this checkpoint).
                 *
                 * Because we receive records in sorted order, and this is the first checkpoint cell which is
                 * found < mat, we can say (until we get better information anyway) that this is the checkpoint
                 *  version as well.
                 */
                TxnView checkpointTxn = getCheckpointTxn(cell);
                if(checkpointTxn.getEffectiveState()!=Txn.State.ROLLEDBACK){
                    discardPoint = ts;
                    checkpointVersion = ts;
                    if(cell.getValueLength()==0)
                        emptyCheckpoints.add(cell.getTimestamp());
                    else
                        compactionRow.append(cell);
                }
            }

            return;
        }
        /*
         * This checkpoint is above the MAT, so we need to keep it around. However, we want to make
         * sure that we deal with it transactionally.
         */
        if(processCheckpointTxn(cell)){
            compactionRow.append(cell);
        }
    }

    private boolean processCheckpointTxn(Cell cell) throws IOException{
        TxnView txn = getCheckpointTxn(cell);
        if(txn.getEffectiveState()==Txn.State.ROLLEDBACK) return false;

        if(cell.getValueLength()==0){
            txn = transactionStore.getTransaction(cell.getTimestamp());
            if(txn.getEffectiveState()!=Txn.State.ROLLEDBACK)
                emptyCheckpoints.set(cell.getTimestamp());
            return false;
        }

        return true;
    }

    private TxnView getCheckpointTxn(Cell cell) throws IOException{
        TxnView txn;
        if(cell.getValueLength()==0){
            txn=transactionStore.getTransaction(cell.getTimestamp());
        }else{
            txn=transactionStore.getTransactionFromCache(cell.getTimestamp());
            if(txn==null){
                //the transaction is not present in the cache yet, so populate the cache with our contents
                long globalCommitTs=Bytes.toLong(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                txn=new CommittedTxn(cell.getTimestamp(),globalCommitTs);
                transactionStore.cache(txn);
            }
        }
        return txn;
    }

    private void reset(){
        this.setCommitTimestampVersions.clear();
        this.emptyCheckpoints.clear();
        this.visitedCheckpoints.clear();

        this.checkpointVersion=-1;
        this.discardPoint=-1;

        this.currentRowKey.reset();
        this.accumulator.reset();
    }
}

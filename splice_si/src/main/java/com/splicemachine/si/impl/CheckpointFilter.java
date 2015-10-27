package com.splicemachine.si.impl;

import com.splicemachine.si.impl.checkpoint.CheckpointResolver;
import com.splicemachine.si.impl.checkpoint.NoOpCheckpointResolver;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 10/8/15
 */
public class CheckpointFilter extends FilterBase{
    private TxnFilter<Cell> txnFilter;
    private long seekThreshold;
    private long resolveThreshold;
    private CheckpointResolver resolver;

    private long checkpointVersion = -1l;
    private boolean seekColumns = false;
    private int checkpointCount; //the number of checkpoints that we've seen
    private CellType lastkvT;
    private long numUserCells;
    private boolean resolved;

    CheckpointFilter(TxnFilter<Cell> txnFilter,long seekThreshold){
        this(txnFilter, seekThreshold,NoOpCheckpointResolver.INSTANCE,Long.MAX_VALUE);
    }

    public CheckpointFilter(TxnFilter<Cell> txnFilter,long seekThreshold,CheckpointResolver resolver,long resolveThreshold){
        this.txnFilter=txnFilter;
        this.seekThreshold=seekThreshold;
        if(seekThreshold<0)
            seekColumns = true;
        this.resolveThreshold = resolveThreshold;
        this.resolver = resolver;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException{
        CellType type=txnFilter.getType(v);
        boolean sameKvType = lastkvT!=null && type==lastkvT;
        lastkvT = type;
        return applyFilter(v,type,sameKvType);
    }

    private ReturnCode applyFilter(Cell v,CellType type,boolean sameKvType) throws IOException{
        switch(type){
            case CHECKPOINT:
                return filterCheckpointCell(v);
            case USER_DATA:
                /*
                 * Count the number of visible USER_DATA records. If the number
                 * of records between the first entry and the checkpointVersion exceeds
                 * the threshold, then submit this row for checkpoint resolution
                 */
                return filterUserDataCell(v,sameKvType);
            default:
                return filterNonCheckpointCell(v,sameKvType);
        }
    }

    @Override
    public void reset() throws IOException{
        checkpointVersion = -1l;
        checkpointCount =0;
        if(seekThreshold>0)
            seekColumns = false;
        lastkvT = null;
        txnFilter.nextRow();
        numUserCells=0;
        resolved=false;
    }

    @Override
    public void filterRowCells(List<Cell> keyValues) throws IOException{
        if(txnFilter.isPacked()){
            if(!filterRow()){
               keyValues.remove(0);
            }
            Cell accumulatedValue = txnFilter.produceAccumulatedKeyValue();
            if(accumulatedValue!=null)
                keyValues.add(accumulatedValue);
        }
    }

    @Override public boolean hasFilterRow(){ return txnFilter.isPacked(); }
    @Override public boolean filterRow() throws IOException{ return txnFilter.getExcludeRow(); }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private ReturnCode filterCheckpointCell(Cell v) throws IOException{
        long cVersion=v.getTimestamp();
        if(cVersion<checkpointVersion){
            /*
             * We keep track of the number of Checkpoint cells we see. If we see a bunch of checkpoint cells,
             * then we will ALSO see a bunch of data cells which are less than our checkpoint timestamp; therefore,
             * seeking will be more efficient, so we go ahead and trigger that.
             *
             * This isn't the only way of enabling seeking, because it's possible that we have never checkpointed
             * a given row, and just have millions of versions. Therefore, we will also do detection when
             * filtering non-checkpoint cells
             */
            checkpointCount++;
            if(checkpointCount>seekThreshold){
                seekColumns = true;
                return ReturnCode.NEXT_COL;
            }
        }else{
            ReturnCode txnCode=txnFilter.filterKeyValue(v);
            if(txnCode==ReturnCode.INCLUDE){
                checkpointVersion = cVersion;
            }
        }
        return ReturnCode.SKIP;
    }

    private ReturnCode filterUserDataCell(Cell v,boolean sameCellType) throws IOException{
        long timestamp=v.getTimestamp();
        if(timestamp<checkpointVersion){
            return skipBelowCheckpoint(sameCellType);
        }else {
            ReturnCode returnCode=txnFilter.filterKeyValue(v);
            if(!resolved && returnCode==ReturnCode.INCLUDE){
                numUserCells++;
                if(numUserCells>resolveThreshold){
                    long beginTimestamp=txnFilter.unwrapReadingTxn().getBeginTimestamp();
                    resolver.resolveCheckpoint(v.getRowArray(),v.getRowOffset(),v.getRowLength(), beginTimestamp);
                    numUserCells=0;
                    resolved=true;
                }
            }
            return returnCode;
        }
    }

    private ReturnCode filterNonCheckpointCell(Cell v,boolean sameCellType) throws IOException{
        long timestamp=v.getTimestamp();
        if(timestamp<checkpointVersion){
            return skipBelowCheckpoint(sameCellType);
        }else {
            return txnFilter.filterKeyValue(v);
        }
    }

    private ReturnCode skipBelowCheckpoint(boolean sameCellType){
        if(seekColumns) return ReturnCode.NEXT_COL;

            /*
             * We need to determine if a seek is appropriate on this column. To do that, we keep track
             * of the number of cells that we've visited which were between this checkpoint and the last. If the
             * number exceeds the seek threshold, then we switch to seek mode. However, if we have moved
             * to another column type (i.e. going from Commit timestamp to Tombstone), then we actually know that
             * seeking is not necessary, so we just return skip for it.
             */
        if(!sameCellType){
            checkpointCount=0;
            return ReturnCode.SKIP;
        }else{
            /*
             * We have the same cell type, so keep track of how many we see. If we see enough, switch
             * to seek mode
             */
            checkpointCount++;
            if(checkpointCount>seekThreshold){
                seekColumns=true;
            }
        }
        return ReturnCode.SKIP;
    }
}

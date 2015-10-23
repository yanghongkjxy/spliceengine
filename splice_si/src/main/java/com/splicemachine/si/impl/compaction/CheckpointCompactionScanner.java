package com.splicemachine.si.impl.compaction;

import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.RollForward;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.CellTypeParser;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 10/21/15
 */
public class CheckpointCompactionScanner implements InternalScanner{
    private static final Logger LOG=Logger.getLogger(CheckpointCompactionScanner.class);
    private final InternalScanner delegate;

    private RowCompactor rowCompactor;
    private List<Cell> priorKeyValues;


    /*Monitoring information*/
    private Counter compactedRowInputCounter;
    private Counter compactedRowOutputCounter;
    private Counter cellsInput;
    private Counter cellsOutput;

    private long compactionStartTimestamp=-1l;
    private long compactionStopTimestamp;

    private CheckpointCompactionScanner(InternalScanner delegate,
                                        RowCompactor rowCompactor,
                                        MetricFactory metricFactory){
        this.delegate=delegate;
        this.rowCompactor=rowCompactor;
        this.compactedRowInputCounter=metricFactory.newCounter();
        this.compactedRowOutputCounter=metricFactory.newCounter();
        this.cellsInput=metricFactory.newCounter();
        this.cellsOutput=metricFactory.newCounter();
    }

    public static CheckpointCompactionScanner minorScanner(TxnSupplier txnStore,
                                                           CellTypeParser ctParser,
                                                           InternalScanner delegate,
                                                           RollForward rollForward,
                                                           long mat){
        if(LOG.isInfoEnabled()){
            //record metrics if we want to log them
            return minorScanner(txnStore,ctParser,delegate,rollForward,mat,Metrics.basicMetricFactory());
        }else
            return minorScanner(txnStore,ctParser,delegate,rollForward,mat,Metrics.noOpMetricFactory());
    }

    public static CheckpointCompactionScanner minorScanner(TxnSupplier txnStore,
                                                           CellTypeParser ctParser,
                                                           InternalScanner delegate,
                                                           RollForward rollForward,
                                                           long mat,
                                                           MetricFactory metricFactory){
        RowCompactor rc=new MinorCheckpointRowCompactor(mat,ctParser,txnStore,rollForward);

        return new CheckpointCompactionScanner(delegate,rc,metricFactory);
    }

    public static CheckpointCompactionScanner majorScanner(TxnSupplier txnStore,
                                                           SDataLib dataLib,
                                                           CellTypeParser ctParser,
                                                           InternalScanner delegate,
                                                           RollForward rollForward,
                                                           long mat){
        if(LOG.isInfoEnabled()){
//            record metrics if we want to log them
            return majorScanner(txnStore,dataLib,ctParser,delegate,rollForward,mat,Metrics.basicMetricFactory());
        }else
            return majorScanner(txnStore,dataLib,ctParser,delegate,rollForward,mat,Metrics.noOpMetricFactory());

    }

    public static CheckpointCompactionScanner majorScanner(TxnSupplier txnStore,
                                                           SDataLib dataLib,
                                                           CellTypeParser ctParser,
                                                           InternalScanner delegate,
                                                           RollForward rollForward,
                                                           long mat,
                                                           MetricFactory metricFactory){
        RowAccumulator<Cell> ra=new HRowAccumulator<>(dataLib,
                EntryPredicateFilter.EMPTY_PREDICATE,new EntryDecoder(),false);
        RowCompactor rc=new CheckpointRowCompactor(mat,ctParser,ra,txnStore,rollForward);

        return new CheckpointCompactionScanner(delegate,rc,metricFactory);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException{
        return next(results,-1);
    }

    @Override
    public boolean next(List<Cell> result,int limit) throws IOException{
        if(compactionStartTimestamp<0) compactionStartTimestamp=System.currentTimeMillis();
        /*
         * Data is returned in the order of
         * <checkpoint>....<checkpoint>
         * <commitTimestamp>...<commitTimestamp>
         * <tombstone>...<tombstone>
         * <value>...<value>
         * <fk_counter>
         *
         * So we will always see all the checkpoints before we see any commit timestamps, and so forth.
         *
         * This is problematic, because we will need to actually insert a commit timestamp
         * into specific locations in the stream, but we don't want to necessarily use the memory
         * that is present on this. The only way to insert a commit timestamp into the stream is to
         * keep the entire row (up to that version of the values data, anyway).
         *
         * To do that, we use a "RowCompactor" abstraction, which hides a lot of our memory manipulations
         * behind the interface. The basic idea is to do the following:
         *
         * 1. Read a batch of records.
         * 2. add those rows to the rowCompactor.
         *
         * And when no more cells are present for that row, then we switch into a push mode, where
         * we push a batch of Cells into the result until we run out of cells to add to that result for
         * that particular row.
         */
        if(!fillCompactor(limit)){
            //we could not read another row of data, so we are done
            //when we are more confident, we can wrap this in a debug statement
            validateTermination(limit);
            compactionStopTimestamp=System.currentTimeMillis();
            return false;
        }

        assert !rowCompactor.isEmpty() : "Row compactor should not be empty!";

        rowCompactor.placeCells(result,limit);
        cellsOutput.add(result.size());
        return true;
    }


    @Override
    public void close() throws IOException{
        LOG.trace("Closing compaction scanner");
        delegate.close();

        if(LOG.isInfoEnabled()){
            String message=String.format(
                    "Compaction stats(type=%s): TotalTime(ms)=%d,cellsInput=%d,cellsOutput=%d,rowsInput=%d,rowsOutput=%d (rowsRemoved=%d)",
                    rowCompactor instanceof MinorCheckpointRowCompactor?"Minor":"Major",
                    getCompactionTimeMillis(),
                    getInputCellCount(),
                    getOutputCellCount(),
                    getInputRowCount(),
                    getOutputRowCount(),
                    getInputRowCount()-getOutputRowCount());
            LOG.info(message);
        }
    }

    /**
     * This method is <em>not</em> thread-safe: calling it from multiple threads <em>will</em> result in
     * errors.
     *
     * @return the amount of time taken to perform the compaction scan. If the scan is not yet complete,
     * then this will return the amount of time that has been taken so far.
     */
    public long getCompactionTimeMillis(){
        if(compactionStartTimestamp<0)
            return 0;
        else if(compactionStopTimestamp<0)
            return System.currentTimeMillis()-compactionStartTimestamp;
        else
            return compactionStopTimestamp-compactionStartTimestamp;
    }

    /**
     * @return the number of cells that were read from the underlying files. This is <em>not</em> the same
     * as the number of cells output (since we can remove cells, and also add in other cells).
     */
    public long getInputCellCount(){
        return cellsInput.getTotal();
    }

    /**
     * @return the number of cells that were written by this compaction. This is <em>not</em> the same
     * as the number of cells read (since we can add or remove cells during compaction).
     */
    public long getOutputCellCount(){
        return cellsOutput.getTotal();
    }

    /**
     * @return the number of rows read by this compaction. This will differ from the number of rows written only by
     * the number of rows which were deleted.
     */
    public long getInputRowCount(){
        return compactedRowInputCounter.getTotal();
    }

    /**
     * @return the number of rows written by this compaction. This will differ from the number of rows read only
     * by the number of rows which were deleted.
     */
    public long getOutputRowCount(){
        return compactedRowOutputCounter.getTotal();
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/

    private boolean fillCompactor(int limit) throws IOException{
        /*
         * Fill the row compactor with another row's worth of data, if the compactor doesn't already
         * have data in it. This mainly manages the calls to readNextRow() to ensure that metadata and stuff
         * is properly handled
         */
        if(!rowCompactor.isEmpty()) return true; //There is still data for this row to read

        readNextRow(limit); //read in another row of data
        boolean dataToReturn=!rowCompactor.isEmpty(); //if there's data to return, then the row compactor won't be empty
        if(dataToReturn){
            rowCompactor.reverse();
            compactedRowOutputCounter.increment();
        }
        return dataToReturn;
    }

    private boolean readNextRow(int limit) throws IOException{
        /*
         * Read and process the next rows' worth of data. This will continue reading until 1 of 2 conditions
         * is met:
         *
         * 1. We finish read an entire row and there is data
         * 2. We are out of records to read
         *
         * A row is "removed" during this step if we read it in but the compactor discards all the entries (either
         * due to being rolled back, or because there was a removable tombstone or whatever). When this happens,
         * the rowCompactor will be empty, so the next row's worth of data will be read in this loop.
         *
         */
        //initialization: make sure that priorKeyValues is non-null and is occupied
        if(priorKeyValues==null)
            priorKeyValues=new LinkedList<>();
        if(priorKeyValues.size()<=0) //try and read another row so that we are occupied
            delegate.next(priorKeyValues,limit);

        //read loop
        while(priorKeyValues.size()>0){
            Cell c=priorKeyValues.get(0);
            if(rowCompactor.isEmpty()){
                /*
                 * If the rowCompactor is empty, then one of two things have happened:
                 *
                 * 1. We are the first time through the loop => we can process this record
                 * 2. The compactor has discarded all cells for the last row seen (so far).
                 *
                 * In case #1, we can process the record as normal.
                 *
                 * In case #2, we have discarded all the entries that we've seen so far. Those old entries
                 * can be from the same row (if an entire batch was discarded) or from the row prior to the
                 * currently visited one, but it doesn't matter because we have discarded them. Therefore,
                 * we can treat case #2 as a new row each time we see it.
                 */
                cellsInput.add(priorKeyValues.size());
                LOG.trace("new row found");
                compactedRowInputCounter.increment();
                rowCompactor.reverse();
                processBatch();
            }else if(rowCompactor.currentRowKey().equals(c.getRowArray(),c.getRowOffset(),c.getRowLength())){
                /*
                 * The row compactor is not empty, so we need to make sure that the batch of data we are
                 * processing comes from the same row. If it does, then we can process it; if it does not, then we
                 * need to terminate the loop because we've populated the next row in the compactor
                 */
                cellsInput.add(priorKeyValues.size());
                LOG.trace("Another batch on existing row found");
                processBatch();
            }else{
                /*
                 * We read a new row of data in, but the prior row has cells to return in the compactor still,
                 * so we break out of the loop so that we can return those rows first.
                 */
                LOG.trace("New row found, but existing row was processed");
                return true;
            }
            delegate.next(priorKeyValues,limit);
        }
        return false;
    }

    private void processBatch() throws IOException{
        Iterator<Cell> kvIter=priorKeyValues.iterator();
        while(kvIter.hasNext()){
            Cell n=kvIter.next();
            rowCompactor.addCell(n);
            kvIter.remove();
        }
    }

    private void validateTermination(int limit) throws IOException{
        /*
         * Debug method: If the compaction logic is broken for whatever reason, we might terminate early.
         * Since terminating early might result in corrupting data (since whatever isn't read will be discarded
         * by HBase), we want to avoid that. Thus, this method verifies that the invariants for actually
         * being done are met, and if not, it blows up. If it blows up, the Compaction will fail, which will
         * undo everything we just did, but will ensure that our data integrity remains safe.
         *
         * Once we have a high degree of confidence in the quality of our compaction logic, we can wrap calls
         * to this method in some form of a debug check to make sure we don't do this in the normal course of events,
         * but for now it's best to leave it in as extra protection.
         */
        if(!rowCompactor.isEmpty()){
            throw new IOException("Row Compactor still has data!");
        }else if(priorKeyValues.size()>0){
            throw new IOException("Still some rows in priorKeyValues!");
        }else{
            boolean next=delegate.next(priorKeyValues,limit);
            if(priorKeyValues.size()>0){
                throw new IOException("Delegate still returning rows!");
            }else if(next){
                throw new IOException("Delegate believes it still has data, but none is being returned!");
            }
        }
    }
}

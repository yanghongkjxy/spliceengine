package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 9/16/15
 */
public class MergingReader<Data>{
    public interface RowKeyFilter{
        RowKeyFilter NOOPFilter = new RowKeyFilter(){
            @Override public boolean filter(ByteSlice rowKey) throws IOException{ return true; }
            @Override public boolean applied(){ return false; }
            @Override public void reset(){ }
        };
        boolean filter(ByteSlice rowKey) throws IOException;
        boolean applied();
        void reset();
    }
    private final RowAccumulator<Data> accumulator;
    private final SDataLib<Data,Put,Delete,Get,Scan> dataLib;
    private final RowKeyFilter rowKeyFilter;

    //metrics
    private final Counter outputBytesCounter;
    private final Counter filterCounter;
    private final Counter returnedRows;

    private MeasuredRegionScanner<Data> regionScanner;
    private Supplier<SIFilter<Data>> filterSupplier;

    //transient here indicates that it is lazily constructed on an as-needed basis
    private transient SIFilter<Data> siFilter;
    private transient ByteSlice rowKeySlice;
    private transient ByteSlice skipRowKeySlice;
    private transient List<Data> keyValues;
    private transient List<Data> priorKeyValues;
    private transient boolean exhausted = false;
    private transient boolean filteredKey = false;
    private transient boolean visited;

    public MergingReader(RowAccumulator<Data> accumulator,
                         MeasuredRegionScanner<Data> regionScanner,
                         SDataLib<Data, Put, Delete, Get, Scan> dataLib,
                         RowKeyFilter rowKeyFilter,
                         Supplier<SIFilter<Data>> filterSupplier,
                         MetricFactory metricFactory){
        this.accumulator=accumulator;
        this.regionScanner=regionScanner;
        this.dataLib=dataLib;
        this.rowKeyFilter=rowKeyFilter;
        this.filterSupplier=filterSupplier;
        this.outputBytesCounter = metricFactory.newCounter();
        this.filterCounter = metricFactory.newCounter();
        this.returnedRows = metricFactory.newCounter();
    }

    /**
     * Reads the next row from the underlying scanner, skipping rows and batches as appropriate.
     *
     * @return true if the accumulator has a complete row ready, false if no more rows are
     * available.
     * @throws IOException if something goes wrong.
     */
    public boolean readNext() throws IOException{
        /*
         * The intent here is to process the next row, reading data in batches until
         *
         * 1. The SIFilter tells us to discard the row.
         * 2. The Accumulator is finished
         * 3. A new row key is seen
         * 4. There is no more data to visit
         *
         * When 1 is found, then we want to skip any remaining batches with the same row key that we see, then
         * continue reading until we've found a row that DOES match our filter requirements. I.e. we reset the
         * row data, and move on in the batch
         *
         * When 2 is found, we return early. However, this means that we may see cells with the same row key
         * the next time around. To avoid that, we will mark that row key as a row key to skip, then
         * skip over that key until a new key is found
         *
         * When 3 is found, we need to return the *previous* row (the one we just stopped processing), but we can't
         * lose the information found in the new row (the one we just saw), so we stash those away into a holder
         * list, then return early. Since we already know we are on a different row key, we don't need to skip a
         * batch.
         *
         * When 4 is found, we need to return what we've finished processing. This is very simple
         */
        SIFilter<Data> filter=getSIFilter();
        resetStateForNextRow(filter);
        if(exhausted) return false;

        /*
         * Most of the time, we'll only return 2 columns or so, but since we re-use the list, it will expand
         * up to the batch size of the return, which will be enough for our purposes. Setting to 2 here instead
         *  of to the batch size right away avoids wasting memory when the batch size is much larger than
         *  the average row size (a very common occurrence).
         */
        if(keyValues==null)
            keyValues=new ArrayList<>(2);

        boolean hasNextRow;
        int batchSize;
        boolean rowProcessed=false;
        do{
            hasNextRow=fetchNextBatch(keyValues);
            batchSize=keyValues.size();
            if(batchSize<=0){
                break;
            }
            ProcessCode result=processBatch(filter,keyValues,rowProcessed);
            switch(result){
                case RETURN: //we found a row, hooray!
                    measureOutputSize();
                    skipBatch();
                    return true;
                case SKIP: //the filter told us to skip this, so mark it skipped and move on
                    skipBatch();
                    resetStateForNextRow(filter);
                    rowProcessed=false;
                    break;
                case CONTINUE: //we aren't quite done yet, keep going
                    rowProcessed=true;
                case PRIOR_SKIPPED: //a previous batch said skip this batch, so move on
                case CONTINUE_IGNORE:
                    break;
                default:
                    assert false:"Programmer error: Unexpected process code!";
            }
        }while(hasNextRow);
        /*
         * We hit scenario 4--we are out of data. There are three possible circumstances:
         * 1. There are no rows in the table at all
         * 2. All the rows are filtered out
         * 3. We just ran out of batches and need to return what we have
         */
        exhausted=true; //mark as exhausted so we don't keep trying
        if(rowProcessed && notFiltered()){
            measureOutputSize();
            return true;
        }else return false;
    }

    public TimeView getTime(){ return regionScanner.getReadTime(); }
    public long getRowsFiltered(){ return filterCounter.getTotal(); }
    public long getRowsVisited() { return filterCounter.getTotal()+returnedRows.getTotal(); }
    public long getBytesVisited() { return regionScanner.getBytesVisited(); }
    public long getBytesOutput() { return outputBytesCounter.getTotal(); }

    public void setRegionScanner(MeasuredRegionScanner<Data> scanner){ this.regionScanner = scanner; }
    public MeasuredRegionScanner<Data> getRegionScanner() { return regionScanner; }

    public ByteSlice currentRowKey(){
       return rowKeySlice;
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private ProcessCode processBatch(SIFilter<Data> filter,List<Data> kvs,boolean priorProcessed) throws IOException{
        assert kvs.size()>0: "Programmer error: processBatch called with an empty list!";
        /*
         * We know we have data in this batch, so we just need to determine what to do with it.
         * There are several scenarios:
         *
         * 1. The batch is from a new row => push into priorKeyValues, then RETURN
         * 2. The batch is from the same row as previous:
         *    A. the previous batch was filtered out => PRIOR_SKIP
         *    B. The previous batch was already returned => PRIOR_SKIP
         *    C. The previous batch is not finished => process
         *
         * So first we determine whether this batch is in the same row as the previous batch.
         */
        Data firstKv =kvs.get(0); //safe by assumption of the method
        if(!matchesRowKey(firstKv)){
            if(rowChanged(filter,kvs,priorProcessed)) return ProcessCode.RETURN;
        }
        setRowKey(firstKv); //first step in processing: set the row key

        if(shouldSkipBatch()){
            /*
             * We were told to skip this batch (because the prior batch with this row key was filtered out or
             * because it was already returned. Therefore, skip it)
             */
            return ProcessCode.PRIOR_SKIPPED;
        }

        visited=true;
        if(!filterRowKey()){
            filterCounter.increment();
            return ProcessCode.SKIP;
        }
        ProcessCode pCode = filterRow(kvs,filter);
        if(pCode==ProcessCode.SKIP){
            filterCounter.increment();
            return ProcessCode.SKIP;
        }else if(pCode==ProcessCode.CONTINUE && accumulator.isFinished()){
            if(accumulator.getEntryAccumulator().checkFilterAfter()) return ProcessCode.SKIP;
            else
                return ProcessCode.RETURN;
        }else
            return pCode;
    }

    protected boolean rowChanged(SIFilter<Data> filter,List<Data> kvs,boolean priorProcessed){
        /*
         * We have read a new row. We have a few scenarios:
         *
         * 1. The previous row was filtered out => just process this
         * 2. The previous row was returned already => process this
         * 3. The previous row was not yet done => push into priorKeyValues, then RETURN
         */
        if(priorProcessed && !shouldSkipBatch() &&!accumulator.getEntryAccumulator().checkFilterAfter()){
            /*
             * shouldSkip is indicated when we have either filtered out the row, or the row
             * was returned due to the accumulator being finished. Therefore, we can treat this
             * as understanding cases 1 and 2. If shouldSkipBatch()==true, then we should process,
             * otherwise, return the row that we already have
             */
            pushIntoPriorKeyValues(kvs);
            return true;
        }else resetStateForNextRow(filter);
        return false;
    }

    protected void resetStateForNextRow(SIFilter<Data> filter){
        /*
         * This is the start of the next read. By definition, this means that
         * we are at the start of a new row (i.e. we don't have any data to parse. Thus,
         * do all of our resets here, then process the next row.
         *
         * Classes which extend this should be sure to call the super method as well
         */
        accumulator.reset(); //reset the accumulator
        rowKeyFilter.reset();
        filteredKey = false;
        filter.nextRow();
        visited = false;
    }

    private boolean notFiltered(){
        return visited && !shouldSkipBatch() && !accumulator.getEntryAccumulator().checkFilterAfter();
    }


    protected boolean fetchNextBatch(List<Data> data) throws IOException{
        /*
         * We want to get the next batch of records. Sometimes, we saw the next batch while
         * still processing the last batch. When that happens, we will have stashed the results away
         * in priorKeyValues, so we want to read from that instead of from the region scanner.
         */
        data.clear();
        if(priorKeyValues!=null && priorKeyValues.size()>0){
            //fetch the prior batch out of cold storage
            data.addAll(priorKeyValues);
            setRowKey(data.get(0));
            priorKeyValues.clear();
            return true;
        }else{
            return dataLib.regionScannerNext(regionScanner,data);
        }
    }

    private void skipBatch(){
        if(skipRowKeySlice==null)
            skipRowKeySlice = new ByteSlice(rowKeySlice);
        else{
           skipRowKeySlice.set(rowKeySlice);
        }
    }

    private boolean shouldSkipBatch(){
        if(skipRowKeySlice==null || skipRowKeySlice.length()<=0) return false;
        else if(skipRowKeySlice.equals(rowKeySlice)) return true;

        //reset the skip key, because we've moved to a new row
        skipRowKeySlice.reset();
        return false;
    }

    private void measureOutputSize(){
        returnedRows.increment();
        if(outputBytesCounter.isActive()){
            for(Data cell:keyValues){
                long len = 0;
                Cell c = (Cell)cell;
                len+=c.getRowLength();
                len+=c.getFamilyLength();
                len+=c.getQualifierLength();
                len+=c.getValueLength();
                outputBytesCounter.add(len);
            }
        }
    }

    private boolean matchesRowKey(Data rowKey){
        Cell c = (Cell)rowKey;
        byte[] newRowArray=c.getRowArray();
        int newRowOffset=c.getRowOffset();
        short newRowKeyLength=c.getRowLength();

        //noinspection SimplifiableIfStatement
        if(rowKeySlice==null||(skipRowKeySlice!=null && skipRowKeySlice.length()>0)){
            return true;
        }else
            return rowKeySlice.equals(newRowArray,newRowOffset,newRowKeyLength);
    }


    private boolean filterRowKey() throws IOException{
        if(filteredKey) return true;
        boolean filter=rowKeyFilter.filter(rowKeySlice);

        filteredKey = true;
        return filter;
    }

    private ProcessCode filterRow(List<Data> kvs,SIFilter<Data> filter) throws IOException {
        Iterator<Data> kvIter = kvs.iterator();
        while(kvIter.hasNext()){
            Data kv = kvIter.next();
            Filter.ReturnCode returnCode = filter.filterKeyValue(kv);
            switch(returnCode){
                case NEXT_COL:
                case NEXT_ROW:
                case SEEK_NEXT_USING_HINT:
                    return ProcessCode.SKIP;
                case SKIP:
                    kvIter.remove();
                default:
                    //these are okay--they mean the encoding is good
            }
        }
        if(kvs.size()>0)
            return ProcessCode.CONTINUE;
        else return ProcessCode.CONTINUE_IGNORE;
    }

    @SuppressWarnings("unchecked")
    private SIFilter<Data> getSIFilter() throws IOException {
        if(siFilter==null) {
            try{
                siFilter=filterSupplier.get();
            }catch(RuntimeException re){
                Throwable t =Throwables.getRootCause(re);
                throw Exceptions.getIOException(t);
            }
            /*
             * There is no longer any reason to hold on to the filter supplier, so let the garbage
             * collector clean it up
             */
            filterSupplier = null;
        }
        return siFilter;
    }


    private void pushIntoPriorKeyValues(List<Data> kvs){
    /*
     * We read a batch which starts the next row, so set this into
     * the prior row keys and return true, so that we tell people
     * that the row changed
     */
        if(priorKeyValues==null)
            priorKeyValues = new ArrayList<>(kvs.size());
        priorKeyValues.addAll(kvs);
    }

    private void setRowKey(Data currentKV){
        Cell c = (Cell)currentKV;
        if(rowKeySlice==null)
            rowKeySlice = new ByteSlice();

        rowKeySlice.set(c.getRowArray(),c.getRowOffset(),c.getRowLength());
    }

    private enum ProcessCode{
        PRIOR_SKIPPED, //indicate that we should skip this batch because a prior batch told us to
        SKIP, //indicate that this row should be skipped. Do not return anything
        RETURN, //indicate that we are ready to return the row
        CONTINUE, //continue processing. If there are no more cells with this keyvalue, treat this as RETURN
        CONTINUE_IGNORE, //contiue processing, but we didn't actually do anything, so if we run out of rows, return false not true
    }
}

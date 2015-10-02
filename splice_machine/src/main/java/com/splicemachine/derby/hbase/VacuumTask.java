package com.splicemachine.derby.hbase;

import com.google.common.base.Suppliers;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.MergingReader;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.*;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 10/2/15
 */
public class VacuumTask extends ZkTask{
    private long minimumActiveTimestamp;

    private Scan scan;

    public VacuumTask(){ }

    public VacuumTask(String jobId,long minimumActiveTimestamp){
        super(jobId,0);
        this.minimumActiveTimestamp=minimumActiveTimestamp;
    }

    @Override
    public void prepareTask(byte[] start,byte[] stop,RegionCoprocessorEnvironment rce,SpliceZooKeeperManager zooKeeper) throws ExecutionException{
        super.prepareTask(start,stop,rce,zooKeeper);
        this.scan =new Scan();
        this.scan.setStartRow(start);
        this.scan.setStopRow(stop);
        this.scan.setBatch(SpliceConstants.scannerBatchSize);
        this.scan.setMaxVersions();
        try{
            this.scan.setTimeRange(0l,minimumActiveTimestamp+1);
        }catch(IOException wontHappen){
            throw new RuntimeException(wontHappen);
        }
    }

    @Override
    public boolean invalidateOnClose(){
        return true;
    }

    @Override
    public RegionTask getClone(){
        return new VacuumTask(getJobId(),minimumActiveTimestamp);
    }

    @Override
    public int getPriority(){
        return SchedulerPriorities.INSTANCE.getBasePriority(VacuumTask.class);
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException{
        TxnView matTxn = new ActiveReadTxn(minimumActiveTimestamp);
        HDataLib dataLib=new HDataLib();
        RowAccumulator<Cell> accumulator =new HRowAccumulator<>(dataLib,
                EntryPredicateFilter.EMPTY_PREDICATE,new EntryDecoder(),false);
        SIFilter<Cell> siFilter = newSiFilter(matTxn,accumulator);
        MetricFactory mf = Metrics.basicMetricFactory();
        try(MeasuredRegionScanner<Cell> mrs = getRegionScanner(dataLib,mf)){

            final long[] highestTimestamp = new long[]{0l};
            MergingReader<Cell> mr = new MergingReader<Cell>(accumulator,
                    mrs,
                    dataLib,
                    MergingReader.RowKeyFilter.NOOPFilter,
                    Suppliers.ofInstance(siFilter),
                    Metrics.basicMetricFactory()){
                @Override
                protected void resetStateForNextRow(SIFilter<Cell> filter){
                    highestTimestamp[0] = 0l;
                    super.resetStateForNextRow(filter);
                }

                @Override
                protected boolean fetchNextBatch(List<Cell> data) throws IOException{
                    boolean b= super.fetchNextBatch(data);
                    if(data.size()<=0) return b;
                    long currMaxTs = highestTimestamp[0];
                    for(Cell c:data){
                        long timestamp=c.getTimestamp();
                        if(timestamp<=minimumActiveTimestamp){
                            if(currMaxTs<timestamp){
                                highestTimestamp[0] = timestamp;
                            }
                            break;
                        }
                    }

                    return b;
                }

                @Override
                protected boolean rowChanged(SIFilter<Cell> filter,List<Cell> kvs,boolean priorProcessed){
                    if(!priorProcessed){
                        //TODO -sf- issue a delete
                        //there was nothing to process, so just delete all the old stuff
                    }
                    return super.rowChanged(filter,kvs,priorProcessed);
                }
            };

            //TODO -sf- move this to its own class

            //make a power of 2
            int s = 1;
            int writeBufferSize=SpliceConstants.DEFAULT_CACHE_SIZE;
            while(s <writeBufferSize){
                s<<=1;
            }
            Mutation[] writeBuffer = new Mutation[s];
            int writeBufferPos = 0;
            int shift = s-1;
            while(mr.readNext()){
                byte[] value = accumulator.result();
                ByteSlice key = mr.currentRowKey();
                int add = bufferWrite(writeBuffer,writeBufferPos,highestTimestamp[0],key,value);
                writeBufferPos = (writeBufferPos+add) & shift;
                if(writeBufferPos==0){
                    flushBuffer(writeBuffer);
                }
            }
            if(writeBufferPos>0){
                writeBuffer = Arrays.copyOf(writeBuffer,writeBufferPos);
                flushBuffer(writeBuffer);
            }

            TaskStats ts = new TaskStats(mrs.getReadTime().getWallClockTime(),mrs.getRowsVisited(),mrs.getRowsOutput());
            status.setStats(ts);
        }catch(IOException e){
            throw new ExecutionException(e);
        }
    }



    @Override
    protected String getTaskType(){
        return "Vacuum";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeLong(minimumActiveTimestamp);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        minimumActiveTimestamp = in.readLong();
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private MeasuredRegionScanner<Cell> getRegionScanner(SDataLib dataLib,
                                                         MetricFactory metricFactory) throws IOException{

        RegionScanner scanner=region.getScanner(scan);
        return new BufferedRegionScanner(region,scanner,scan,SpliceConstants.scannerBatchSize,metricFactory,dataLib);
    }

    private SIFilter<Cell> newSiFilter(TxnView matTxn,RowAccumulator<Cell> accumulator){
        TxnSupplier txnSupplier = TransactionStorage.getTxnSupplier();
        IgnoreTxnCacheSupplier ignoreTxnSupplier = TransactionStorage.getIgnoreTxnSupplier();
        //want to make sure that we bail the task on error
        ReadResolver resolver = SynchronousReadResolver.getResolver(region,txnSupplier,TransactionalRegions.getRollForwardStatus(),SpliceBaseIndexEndpoint.independentTrafficControl,true);
        DataStore dataStore = TxnDataStore.getDataStore();
        TxnFilter tf = new SimpleTxnFilter(region.getTableDesc().getNameAsString(),txnSupplier,ignoreTxnSupplier,matTxn,resolver,dataStore);

        return new PackedTxnFilter<>(tf,accumulator);
    }

    private int bufferWrite(Mutation[] mutations,int pos,long timestamp,ByteSlice key,byte[] value){
        Put p = getCheckpointPut(timestamp,key,value);
        mutations[pos] = p;
        //delete everything < timestamp, since we are writing an entry at timestamp
        Delete d = getCheckpointDelete(timestamp-1,key);
        mutations[pos+1] = d;
        return 2;
    }

    private Delete getCheckpointDelete(long timestamp,ByteSlice key){
        Delete d = new Delete(key.array(),key.offset(),key.length());
        d=d.deleteColumns(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,timestamp)
                .deleteColumns(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,timestamp)
                .deleteColumns(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSpliceConstants.PACKED_COLUMN_BYTES,timestamp);
        d.setAttribute(FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        d.setDurability(Durability.SKIP_WAL);
        return d;
    }

    private Put getCheckpointPut(long ts,ByteSlice key,byte[] value){
        Put p = new Put(key.array(),key.offset(),key.length());
        p.setAttribute(FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        p.add(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,ts,value);
        p.setDurability(Durability.SKIP_WAL);
        return p;
    }

    private void flushBuffer(Mutation[] writeBuffer) throws IOException{
        OperationStatus[] operationStatuses=region.batchMutate(writeBuffer);
        int successCount = 0;
        int notRunCount = 0;
        int failedCount = 0;
        if(LOG.isTraceEnabled()){
            //noinspection ForLoopReplaceableByForEach
            for(int i=0;i<operationStatuses.length;i++){
                OperationStatus operationStatuse=operationStatuses[i];
                switch(operationStatuse.getOperationStatusCode()){
                    case NOT_RUN:
                        notRunCount++;
                        break;
                    case SUCCESS:
                        successCount++;
                        break;
                    case BAD_FAMILY:
                    case SANITY_CHECK_FAILURE:
                    case FAILURE:
                        failedCount++;
                        break;
                }
            }

            LOG.trace("Successfully checkpointed "+successCount+" rows, with "+failedCount+" failures and "+notRunCount+" not run");
        }
    }
}

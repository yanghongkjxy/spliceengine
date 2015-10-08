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
import org.apache.hadoop.hbase.util.Bytes;

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
        MetricFactory mf = Metrics.basicMetricFactory();
        HDataLib dataLib=new HDataLib();
        RowAccumulator<Cell> accumulator =new HRowAccumulator<>(dataLib, EntryPredicateFilter.EMPTY_PREDICATE,new EntryDecoder(),false);
        SIFilter<Cell> siFilter = newSiFilter(matTxn,accumulator);
        try(MeasuredRegionScanner<Cell> mrs = getRegionScanner(dataLib,mf)){
            CheckpointReader cr = new CheckpointReader(minimumActiveTimestamp,new CheckpointReader.ReadHandler(){
                @Override
                public void onDelete(ByteSlice rowKey){
                    //TODO -sf- do stuff here
                }
            },accumulator,siFilter,dataLib,mrs,Metrics.basicMetricFactory());

            Checkpointer checkpointer = new BufferedCheckpointer(new Region(region),SpliceConstants.DEFAULT_CACHE_SIZE);

            long writtenRows =0;
            while(cr.readNext()){
                byte[] value = cr.currentCheckpointValue();
                ByteSlice key = cr.currentRowKey();
                TxnView highTxn = cr.currentMatTxn();
                checkpointer.checkpoint(key,value,highTxn.getBeginTimestamp(),highTxn.getGlobalCommitTimestamp());
                writtenRows++;
            }

            TaskStats ts = new TaskStats(mrs.getReadTime().getWallClockTime(),cr.getRowsVisited(),writtenRows);
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

}

package com.splicemachine.derby.impl.job.index;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.MergingReader;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.ReadAheadRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.si.api.*;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.*;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class PopulateIndexTask extends ZkTask{
    private static final long serialVersionUID=5l;
    private long operationId;
    private long statementId;
    private long indexConglomId;
    private long baseConglomId;
    private int[] mainColToIndexPosMap;
    private boolean isUnique;
    private boolean isUniqueWithDuplicateNulls;
    private BitSet indexedColumns;
    private BitSet descColumns;
    private int[] columnOrdering;
    private int[] format_ids;
    private long demarcationPoint;
    private HRegion region;

    //performance improvement
    private KVPair mainPair;

    private byte[] scanStart;
    private byte[] scanStop;
    boolean isTraced; //could be null, if no stats are to be collected

    @SuppressWarnings("UnusedDeclaration")
    public PopulateIndexTask(){
    }

    public PopulateIndexTask(
            long indexConglomId,
            long baseConglomId,
            int[] mainColToIndexPosMap,
            BitSet indexedColumns,
            boolean unique,
            boolean uniqueWithDuplicateNulls,
            String jobId,
            BitSet descColumns,
            boolean isTraced,
            long statementId,
            long operationId,
            int[] columnOrdering,
            int[] format_ids,
            long demarcationPoint){
        super(jobId,OperationJob.operationTaskPriority,null);
        this.indexConglomId=indexConglomId;
        this.baseConglomId=baseConglomId;
        this.mainColToIndexPosMap=mainColToIndexPosMap;
        this.indexedColumns=indexedColumns;
        this.descColumns=descColumns;
        this.isUnique=unique;
        this.isUniqueWithDuplicateNulls=uniqueWithDuplicateNulls;
        this.isTraced=isTraced;
        this.statementId=statementId;
        this.operationId=operationId;
        this.columnOrdering=columnOrdering;
        this.format_ids=format_ids;
        this.demarcationPoint=demarcationPoint;
    }

    @Override
    public RegionTask getClone(){
        return new PopulateIndexTask(indexConglomId,baseConglomId,mainColToIndexPosMap,indexedColumns,isUnique,
                isUniqueWithDuplicateNulls,jobId,descColumns,isTraced,statementId,operationId,columnOrdering,format_ids,demarcationPoint);
    }

    @Override
    public boolean isSplittable(){
        return true;
    }

    @Override
    public void prepareTask(byte[] start,byte[] end,RegionCoprocessorEnvironment rce,SpliceZooKeeperManager zooKeeper) throws ExecutionException{
        this.region=rce.getRegion();
        super.prepareTask(start,end,rce,zooKeeper);
        this.scanStart=start;
        this.scanStop=end;
    }

    @Override
    protected String getTaskType(){
        return "populateIndexTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeLong(indexConglomId);
        out.writeLong(baseConglomId);
        out.writeInt(indexedColumns.wlen);
        ArrayUtil.writeLongArray(out,indexedColumns.bits);
        ArrayUtil.writeIntArray(out,mainColToIndexPosMap);
        out.writeBoolean(isUnique);
        out.writeBoolean(isUniqueWithDuplicateNulls);
        out.writeInt(descColumns.wlen);
        ArrayUtil.writeLongArray(out,descColumns.bits);
        out.writeBoolean(isTraced);
        if(isTraced){
            out.writeLong(statementId);
            out.writeLong(operationId);
        }
        ArrayUtil.writeIntArray(out,columnOrdering);
        ArrayUtil.writeIntArray(out,format_ids);
        out.writeLong(demarcationPoint);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        indexConglomId=in.readLong();
        baseConglomId=in.readLong();
        int numWords=in.readInt();
        indexedColumns=new BitSet(ArrayUtil.readLongArray(in),numWords);
        mainColToIndexPosMap=ArrayUtil.readIntArray(in);
        isUnique=in.readBoolean();
        isUniqueWithDuplicateNulls=in.readBoolean();
        numWords=in.readInt();
        descColumns=new BitSet(ArrayUtil.readLongArray(in),numWords);
        if(isTraced=in.readBoolean()){
            statementId=in.readLong();
            operationId=in.readLong();
        }
        columnOrdering=ArrayUtil.readIntArray(in);
        format_ids=ArrayUtil.readIntArray(in);
        demarcationPoint=in.readLong();
    }

    @Override
    public boolean invalidateOnClose(){
        return true;
    }


    @Override
    public void doExecute() throws ExecutionException, InterruptedException{
        Scan regionScan=SpliceUtils.createScan(getTxn());
        regionScan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        regionScan.setStartRow(scanStart);
        regionScan.setStopRow(scanStop);
        regionScan.setCacheBlocks(false);

        //TODO -sf- disable when stats tracking is disabled
        MetricFactory metricFactory=isTraced?Metrics.samplingMetricFactory(SpliceConstants.sampleTimingSize):Metrics.noOpMetricFactory();
        long numRecordsRead=0l;
        long startTime=System.currentTimeMillis();

        Timer transformationTimer=metricFactory.newTimer();
        try{
            //backfill the index with previously committed data

            EntryPredicateFilter predicateFilter=new EntryPredicateFilter(indexedColumns,ObjectArrayList.<Predicate>newInstance(),true);
            HRowAccumulator<Cell> accumulator=new HRowAccumulator<>(dataLib,predicateFilter,new EntryDecoder(),false);
            SIFilter<Cell> siFilter = buildSIFilter(accumulator);
            try(MeasuredRegionScanner<Cell> brs=getRegionScanner(dataLib,siFilter,regionScan,metricFactory)){
                IndexTransformer transformer=new IndexTransformer(isUnique,
                        isUniqueWithDuplicateNulls,
                        null,
                        columnOrdering,
                        format_ids,
                        null,
                        mainColToIndexPosMap,
                        descColumns,
                        indexedColumns);

                byte[] indexTableLocation=Bytes.toBytes(Long.toString(indexConglomId));


                SDataLib dataLib = new HDataLib();
                MergingReader<Cell> mr = new MergingReader<>(accumulator,
                        brs,
                        dataLib,
                        MergingReader.RowKeyFilter.NOOPFilter,
                        Suppliers.ofInstance(siFilter),
                        metricFactory);
                try(RecordingCallBuffer<KVPair> writeBuffer =
                            SpliceDriver.driver().getTableWriter().writeBuffer(indexTableLocation,getTxn(),metricFactory)){
                    while(mr.readNext()){
                        numRecordsRead++;
                        SpliceBaseOperation.checkInterrupt(numRecordsRead,SpliceConstants.interruptLoopCheck);
                        translateResult(mr.currentRowKey(),accumulator.result(),transformer,writeBuffer,transformationTimer);
                    }
                    writeBuffer.flushBufferAndWait();

                    reportStats(startTime,mr,writeBuffer.getWriteStats(),transformationTimer.getTime());
                }
            }

        }catch(IOException e){
            SpliceLogUtils.error(LOG,e);
            throw new ExecutionException(e);
        }catch(Exception e){
            SpliceLogUtils.error(LOG,e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    private SIFilter<Cell> buildSIFilter(RowAccumulator<Cell> accumulator) throws IOException{
        //manually create the SIFilter
        DDLTxnView demarcationPoint=new DDLTxnView(getTxn(),this.demarcationPoint);
        TransactionalRegion transactionalRegion=TransactionalRegions.get(region);
        TxnFilter unpacked = transactionalRegion.unpackedFilter(demarcationPoint);
        transactionalRegion.close();
        //noinspection unchecked
        return new PackedTxnFilter<Cell>(unpacked, accumulator){
            @Override
            public Filter.ReturnCode doAccumulate(Cell dataKeyValue) throws IOException {
                if (!accumulator.isFinished() && accumulator.isOfInterest(dataKeyValue)) {
                    if (!accumulator.accumulate(dataKeyValue)) {
                        return Filter.ReturnCode.NEXT_ROW;
                    }
                    return Filter.ReturnCode.INCLUDE;
                }else return Filter.ReturnCode.INCLUDE;
            }
        };
    }

    protected MeasuredRegionScanner<Cell> getRegionScanner(SDataLib dataLib,SIFilter<Cell> siFilter,Scan regionScan,MetricFactory metricFactory) throws IOException{
        TxnFilter<Cell> unpacked = siFilter.unwrapFilter();
        regionScan.setFilter(new CheckpointFilter(unpacked,SIConstants.checkpointSeekThreshold));
        RegionScanner sourceScanner=region.getScanner(regionScan);
        return SpliceConstants.useReadAheadScanner?new ReadAheadRegionScanner(region,SpliceConstants.DEFAULT_CACHE_SIZE,sourceScanner,metricFactory,dataLib)
                :new BufferedRegionScanner(region,sourceScanner,regionScan,SpliceConstants.DEFAULT_CACHE_SIZE,SpliceConstants.DEFAULT_CACHE_SIZE,metricFactory,dataLib);
    }

    protected void reportStats(long startTime,MergingReader brs,WriteStats writeStats,TimeView manipulationTime) throws IOException{
        if(isTraced){
            //record some stats
            OperationRuntimeStats stats=new OperationRuntimeStats(statementId,operationId,Bytes.toLong(taskId),region.getRegionNameAsString(),12);
            stats.addMetric(OperationMetric.STOP_TIMESTAMP,System.currentTimeMillis());

            TimeView readTime=brs.getTime();
            stats.addMetric(OperationMetric.START_TIMESTAMP,startTime);
            stats.addMetric(OperationMetric.TASK_QUEUE_WAIT_WALL_TIME,waitTimeNs);
            stats.addMetric(OperationMetric.OUTPUT_ROWS,writeStats.getRowsWritten());
            stats.addMetric(OperationMetric.TOTAL_WALL_TIME,manipulationTime.getWallClockTime()+readTime.getWallClockTime());
            stats.addMetric(OperationMetric.TOTAL_CPU_TIME,manipulationTime.getCpuTime()+readTime.getCpuTime());
            stats.addMetric(OperationMetric.TOTAL_USER_TIME,manipulationTime.getUserTime()+readTime.getUserTime());

            stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,brs.getBytesVisited());
            stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,brs.getRowsVisited());
            stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,readTime.getWallClockTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,readTime.getCpuTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,readTime.getUserTime());

            stats.addMetric(OperationMetric.PROCESSING_WALL_TIME,manipulationTime.getWallClockTime());
            stats.addMetric(OperationMetric.PROCESSING_CPU_TIME,manipulationTime.getCpuTime());
            stats.addMetric(OperationMetric.PROCESSING_USER_TIME,manipulationTime.getUserTime());

            OperationRuntimeStats.addWriteStats(writeStats,stats);

            SpliceDriver.driver().getTaskReporter().report(stats,getTxn());
        }
    }

    private void translateResult(ByteSlice rowKey,
                                 byte[] rowValue,
                                 IndexTransformer transformer,
                                 CallBuffer<KVPair> writeBuffer,
                                 Timer manipulationTimer) throws Exception{
        //we know that there is only one KeyValue for each row
        manipulationTimer.startTiming();
        if(mainPair==null)
            mainPair=new KVPair(rowKey.array(),rowKey.offset(),rowKey.length(),
                    rowValue,0,rowValue.length);
        else{
            mainPair.setKey(rowKey.array(),rowKey.offset(),rowKey.length());
            mainPair.setValue(rowValue);
        }
        KVPair pair=transformer.translate(mainPair);

        writeBuffer.add(pair);
        manipulationTimer.tick(1);
    }

    @Override
    public int getPriority(){
        return SchedulerPriorities.INSTANCE.getBasePriority(PopulateIndexTask.class);
    }

    @Override
    protected Txn beginChildTransaction(TxnView parentTxn,TxnLifecycleManager tc) throws IOException{
        return tc.beginChildTransaction(parentTxn,Long.toString(this.indexConglomId).getBytes());
    }
}

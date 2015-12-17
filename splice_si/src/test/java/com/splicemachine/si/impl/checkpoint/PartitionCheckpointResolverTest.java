package com.splicemachine.si.impl.checkpoint;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.compaction.CompactionTestData;
import com.splicemachine.si.impl.compaction.CompactionTestUtils;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 11/16/15
 */
@SuppressWarnings("deprecation")
public class PartitionCheckpointResolverTest{
    private static final SDataLib dataLib = new TestDataLib();

    @Test
    public void doesNothingWhenLessThanVersionLimitCheckpointObjects() throws Exception{
        final byte[] rk = Encoding.encode("Hello");
        final NoOpCheckLock checkLock = new NoOpCheckLock();
        Partition partition = new TestPartition("tt"){
            @Override
            public Lock lock(byte[] rowKey) throws IOException{
                Assert.assertArrayEquals("Incorrect row key!",rk,rowKey);
                return checkLock;
            }
        };

        TxnSupplier supplier = new TestTxnSupplier();
        List<Cell> cells=addRows(rk,3,supplier);
        Put p = new Put(rk);
        for(Cell cell:cells){
            p.add(cell);
        }

        partition.mutate(p);

        SimpleCheckpointer checkpointer=new SimpleCheckpointer(partition,false);
        DataStore ds=newDataStore();
        TxnFilterFactory tff= new TestFilterFactory(partition,supplier,new IgnoreTxnCacheSupplier(dataLib),ds);
        PartitionCheckpointResolver pcr = new PartitionCheckpointResolver(partition,10,10,checkpointer,dataLib,supplier,tff);

        Checkpoint cp= new Checkpoint();
        cp.set(rk,0,rk.length,10);
        pcr.resolveCheckpoint(cp);

        //check concurrency primitives
        Assert.assertTrue("Never acquired the lock!",checkLock.acquired);
        Assert.assertTrue("Never released the lock!",checkLock.released);

        validateCorrectOutput(rk,partition,cells);
    }


    @Test
    public void doesNothingWhenLessThanVersionLimit() throws Exception{
        final byte[] rk = Encoding.encode("Hello");
        final NoOpCheckLock checkLock = new NoOpCheckLock();
        Partition partition = new TestPartition("tt"){
            @Override
            public Lock lock(byte[] rowKey) throws IOException{
                Assert.assertArrayEquals("Incorrect row key!",rk,rowKey);
                return checkLock;
            }
        };

        TxnSupplier supplier = new TestTxnSupplier();
        List<Cell> cells=addRows(rk,3,supplier);
        Put p = new Put(rk);
        for(Cell cell:cells){
            p.add(cell);
        }

        partition.mutate(p);

        SimpleCheckpointer checkpointer=new SimpleCheckpointer(partition,false);
        DataStore ds=newDataStore();
        TxnFilterFactory tff= new TestFilterFactory(partition,supplier,new IgnoreTxnCacheSupplier(dataLib),ds);
        PartitionCheckpointResolver pcr = new PartitionCheckpointResolver(partition,10,10,checkpointer,dataLib,supplier,tff);

        pcr.resolveCheckpoint(rk,0,rk.length,10);

        //check concurrency primitives
        Assert.assertTrue("Never acquired the lock!",checkLock.acquired);
        Assert.assertTrue("Never released the lock!",checkLock.released);

        validateCorrectOutput(rk,partition,cells);
    }

    @Test
    public void mergesTogetherWhenAboveCheckpointThresholdCheckpointObject() throws Exception{
        final byte[] rk = Encoding.encode("Hello");
        final NoOpCheckLock checkLock = new NoOpCheckLock();
        Partition partition = new TestPartition("tt"){
            @Override
            public Lock lock(byte[] rowKey) throws IOException{
                Assert.assertArrayEquals("Incorrect row key!",rk,rowKey);
                return checkLock;
            }
        };

        TxnSupplier supplier = new TestTxnSupplier();
        List<Cell> cells=new ArrayList<>();
        CommittedTxn txn=new CommittedTxn(1l,2l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.fullUserCell(txn,rk,4));
        BitSet cols = new BitSet();
        cols.set(0);
        txn=new CommittedTxn(3l,4l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.partialUserCell(txn,rk,4,cols,"v1"));
        cols.clear();
        cols.set(1);
        txn=new CommittedTxn(5l,6l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.partialUserCell(txn,rk,4,cols,"v2"));
        cols.clear();
        cols.set(2);
        txn=new CommittedTxn(7l,8l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.partialUserCell(txn,rk,4,cols,"v3"));
        Collections.reverse(cells);

        RowAccumulator<Cell> accumulator =new HRowAccumulator<>(dataLib,EntryPredicateFilter.EMPTY_PREDICATE,new EntryDecoder(),false);
        for(Cell c:cells){
            Assert.assertFalse("Still want data!",accumulator.isFinished());
            Assert.assertTrue("Not interested!",accumulator.isOfInterest(c));
            accumulator.accumulate(c);
        }
        Put p = new Put(rk);
        for(Cell cell:cells){
            p.add(cell);
        }

        partition.mutate(p);

        SimpleCheckpointer checkpointer=new SimpleCheckpointer(partition,false);
        DataStore ds=newDataStore();
        TxnFilterFactory tff= new TestFilterFactory(partition,supplier,new IgnoreTxnCacheSupplier(dataLib),ds);
        PartitionCheckpointResolver pcr = new PartitionCheckpointResolver(partition,2,10,checkpointer,dataLib,supplier,tff);

        Checkpoint cp = new Checkpoint();
        cp.set(rk,0,rk.length,10);
        pcr.resolveCheckpoint(cp);

        //check concurrency primitives
        Assert.assertTrue("Never acquired the lock!",checkLock.acquired);
        Assert.assertTrue("Never released the lock!",checkLock.released);

        Get g = new Get(rk);
        Collection<Cell> retCells=partition.get(g);
        //there should be 5 cells returned: a checkpoint, then 4 version cells
        Assert.assertEquals("Incorrect return size!",5,retCells.size());

        byte[] expected = accumulator.result();
        Iterator<Cell> retCellList = retCells.iterator();
        Cell f = retCellList.next();
        Assert.assertEquals("First cell isn't a checkpoint!",CellType.CHECKPOINT,DefaultCellTypeParser.INSTANCE.parseCellType(f));
        Assert.assertEquals("Did not include a commit timestamp!",8,f.getValueLength());

        //next Cell should be a full user cell
        Assert.assertTrue("Iterator failing!",retCellList.hasNext());
        Cell n = retCellList.next();
        Assert.assertArrayEquals("Incorrect accumulated value!",expected,n.getValue());

        //now the remaining cells should match that of the original cell list
        Iterator<Cell> cIter = cells.iterator();
        cIter.next(); //skip the first, since it should be replaced
        while(cIter.hasNext()){
            Assert.assertTrue("Did not return the back cells!",retCellList.hasNext());
            Cell c = cIter.next();
            Cell a = retCellList.next();

            Assert.assertEquals("Did not preserve old data!",c,a);
        }
    }

    @Test
    public void mergesTogetherWhenAboveCheckpointThreshold() throws Exception{
        final byte[] rk = Encoding.encode("Hello");
        final NoOpCheckLock checkLock = new NoOpCheckLock();
        Partition partition = new TestPartition("tt"){
            @Override
            public Lock lock(byte[] rowKey) throws IOException{
                Assert.assertArrayEquals("Incorrect row key!",rk,rowKey);
                return checkLock;
            }
        };

        TxnSupplier supplier = new TestTxnSupplier();
        List<Cell> cells=new ArrayList<>();
        CommittedTxn txn=new CommittedTxn(1l,2l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.fullUserCell(txn,rk,4));
        BitSet cols = new BitSet();
        cols.set(0);
        txn=new CommittedTxn(3l,4l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.partialUserCell(txn,rk,4,cols,"v1"));
        cols.clear();
        cols.set(1);
        txn=new CommittedTxn(5l,6l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.partialUserCell(txn,rk,4,cols,"v2"));
        cols.clear();
        cols.set(2);
        txn=new CommittedTxn(7l,8l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.partialUserCell(txn,rk,4,cols,"v3"));
        Collections.reverse(cells);

        RowAccumulator<Cell> accumulator =new HRowAccumulator<>(dataLib,EntryPredicateFilter.EMPTY_PREDICATE,new EntryDecoder(),false);
        for(Cell c:cells){
            Assert.assertFalse("Still want data!",accumulator.isFinished());
            Assert.assertTrue("Not interested!",accumulator.isOfInterest(c));
            accumulator.accumulate(c);
        }
        Put p = new Put(rk);
        for(Cell cell:cells){
            p.add(cell);
        }

        partition.mutate(p);

        SimpleCheckpointer checkpointer=new SimpleCheckpointer(partition,false);
        DataStore ds=newDataStore();
        TxnFilterFactory tff= new TestFilterFactory(partition,supplier,new IgnoreTxnCacheSupplier(dataLib),ds);
        PartitionCheckpointResolver pcr = new PartitionCheckpointResolver(partition,2,10,checkpointer,dataLib,supplier,tff);

        pcr.resolveCheckpoint(rk,0,rk.length,10);

        //check concurrency primitives
        Assert.assertTrue("Never acquired the lock!",checkLock.acquired);
        Assert.assertTrue("Never released the lock!",checkLock.released);

        Get g = new Get(rk);
        Collection<Cell> retCells=partition.get(g);
        //there should be 5 cells returned: a checkpoint, then 4 version cells
        Assert.assertEquals("Incorrect return size!",5,retCells.size());

        byte[] expected = accumulator.result();
        Iterator<Cell> retCellList = retCells.iterator();
        Cell f = retCellList.next();
        Assert.assertEquals("First cell isn't a checkpoint!",CellType.CHECKPOINT,DefaultCellTypeParser.INSTANCE.parseCellType(f));
        Assert.assertEquals("Did not include a commit timestamp!",8,f.getValueLength());

        //next Cell should be a full user cell
        Assert.assertTrue("Iterator failing!",retCellList.hasNext());
        Cell n = retCellList.next();
        Assert.assertArrayEquals("Incorrect accumulated value!",expected,n.getValue());

        //now the remaining cells should match that of the original cell list
        Iterator<Cell> cIter = cells.iterator();
        cIter.next(); //skip the first, since it should be replaced
        while(cIter.hasNext()){
            Assert.assertTrue("Did not return the back cells!",retCellList.hasNext());
            Cell c = cIter.next();
            Cell a = retCellList.next();

            Assert.assertEquals("Did not preserve old data!",c,a);
        }
    }

    @Test
    public void skipsRolledBackDataDuringMerge() throws Exception{
        final byte[] rk = Encoding.encode("Hello");
        final NoOpCheckLock checkLock = new NoOpCheckLock();
        Partition partition = new TestPartition("tt"){
            @Override
            public Lock lock(byte[] rowKey) throws IOException{
                Assert.assertArrayEquals("Incorrect row key!",rk,rowKey);
                return checkLock;
            }
        };

        TxnSupplier supplier = new TestTxnSupplier();
        List<Cell> cells=new ArrayList<>();
        TxnView txn=new CommittedTxn(1l,2l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.fullUserCell(txn,rk,4));
        BitSet cols = new BitSet();
        cols.set(0);
        txn=new CommittedTxn(3l,4l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.partialUserCell(txn,rk,4,cols,"v1"));
        cols.clear();
        cols.set(1);
        txn=new RolledBackTxn(5l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.partialUserCell(txn,rk,4,cols,"v2"));
        cols.clear();
        cols.set(2);
        txn=new CommittedTxn(7l,8l);
        supplier.cache(txn);
        cells.add(CompactionTestUtils.partialUserCell(txn,rk,4,cols,"v3"));
        Collections.reverse(cells);

        RowAccumulator<Cell> accumulator =new HRowAccumulator<>(dataLib,EntryPredicateFilter.EMPTY_PREDICATE,new EntryDecoder(),false);
        for(Cell c:cells){
            if(supplier.getTransaction(c.getTimestamp()).getEffectiveState()==Txn.State.ROLLEDBACK) continue; //skip rolled back entries
            Assert.assertFalse("Still want data!",accumulator.isFinished());
            Assert.assertTrue("Not interested!",accumulator.isOfInterest(c));
            accumulator.accumulate(c);
        }
        Put p = new Put(rk);
        for(Cell cell:cells){
            p.add(cell);
        }

        partition.mutate(p);

        SimpleCheckpointer checkpointer=new SimpleCheckpointer(partition,false);
        DataStore ds=newDataStore();
        TxnFilterFactory tff= new TestFilterFactory(partition,supplier,new IgnoreTxnCacheSupplier(dataLib),ds);
        PartitionCheckpointResolver pcr = new PartitionCheckpointResolver(partition,2,10,checkpointer,dataLib,supplier,tff);

        pcr.resolveCheckpoint(rk,0,rk.length,10);

        //check concurrency primitives
        Assert.assertTrue("Never acquired the lock!",checkLock.acquired);
        Assert.assertTrue("Never released the lock!",checkLock.released);

        Get g = new Get(rk);
        Collection<Cell> retCells=partition.get(g);
        //there should be 5 cells returned: a checkpoint, then 4 version cells
        Assert.assertEquals("Incorrect return size!",5,retCells.size());

        byte[] expected = accumulator.result();
        Iterator<Cell> retCellList = retCells.iterator();
        Cell f = retCellList.next();
        Assert.assertEquals("First cell isn't a checkpoint!",CellType.CHECKPOINT,DefaultCellTypeParser.INSTANCE.parseCellType(f));
        Assert.assertEquals("Did not include a commit timestamp!",8,f.getValueLength());

        //next Cell should be a full user cell
        Assert.assertTrue("Iterator failing!",retCellList.hasNext());
        Cell n = retCellList.next();
        Assert.assertArrayEquals("Incorrect accumulated value!",expected,n.getValue());

        //now the remaining cells should match that of the original cell list
        Iterator<Cell> cIter = cells.iterator();
        cIter.next(); //skip the first, since it should be replaced
        while(cIter.hasNext()){
            Cell c = cIter.next();
            Cell a = retCellList.next();

            Assert.assertEquals("Did not preserve old data!",c,a);
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void validateCorrectOutput(byte[] rk,Partition partition,List<Cell> expectedOutput) throws IOException{
        Get g = new Get(rk);
        Collection<Cell> retCells=partition.get(g);
        Assert.assertEquals("Returned Cell list is not the same!",expectedOutput.size(),retCells.size());

        Iterator<Cell> cIter = expectedOutput.iterator();
        Iterator<Cell> aIter = retCells.iterator();
        while(cIter.hasNext()){
            Cell c = cIter.next();
            Cell a = aIter.next();

            Assert.assertEquals("Incorrect cell!",c,a);
        }
    }

    @SuppressWarnings("unchecked")
    private DataStore newDataStore(){
        return new DataStore(dataLib,new PartitionReader(),new PartitionWriter(),
                    FixedSIConstants.SI_NEEDED,FixedSIConstants.SI_DELETE_PUT,
                    FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                    FixedSIConstants.EMPTY_BYTE_ARRAY,
                    FixedSIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP,
                    FixedSIConstants.SI_PACKED,
                    DefaultCellTypeParser.INSTANCE);
    }

    private List<Cell> addRows(byte[] rk, int numVersions,TxnSupplier txnSupplier) throws IOException{
        TxnView txn = new CommittedTxn(1l,2l);
        txnSupplier.cache(txn);
        CompactionTestData ctd = new CompactionTestData();
        ctd.rowKey = rk;
        ctd = ctd.insert(txn);
        ctd.numCols = 4;
        for(int i=0;i<2*numVersions;i+=2){
            if(i%3==0){
                txn = new ActiveWriteTxn(i+3,i+3);
            }else if(i%4==0){
                txn= new RolledBackTxn(i+3);
            }else
                txn = new CommittedTxn(i+3,i+4);
            txnSupplier.cache(txn);
            ctd.update(txn);
        }

        return ctd.build();
    }

    private static class TestFilterFactory implements  TxnFilterFactory{
        private final Partition p;
        private final TxnSupplier txnSupplier;
        private final IgnoreTxnCacheSupplier ignoreTxnSupplier;
        private final DataStore dataStore;

        public TestFilterFactory(Partition p,TxnSupplier txnSupplier,IgnoreTxnCacheSupplier ignoreTxnSupplier,DataStore dataStore){
            this.p=p;
            this.txnSupplier=txnSupplier;
            this.ignoreTxnSupplier=ignoreTxnSupplier;
            this.dataStore=dataStore;
        }

        @Override
        public TxnFilter<Cell> unpackedFilter(TxnView txn) throws IOException{
            return new SimpleTxnFilter<>(p.getTableName(),txnSupplier,ignoreTxnSupplier,txn,NoOpReadResolver.INSTANCE,dataStore,128);
        }

        @Override
        public TxnFilter<Cell> noOpFilter(TxnView txn) throws IOException{
            return new NoopTxnFilter(dataStore,txnSupplier);
        }

        @Override
        public TxnFilter<Cell> packedFilter(TxnView txn,EntryPredicateFilter predicateFilter,boolean countStar) throws IOException{
            HRowAccumulator accumulator=new HRowAccumulator(dataStore.getDataLib(),predicateFilter,new EntryDecoder(),countStar);
            return new PackedTxnFilter<>(unpackedFilter(txn),accumulator);
        }

        @Override
        public DDLFilter ddlFilter(Txn ddlTxn) throws IOException{
            throw new UnsupportedOperationException("Should not use a DDLFilter in Checkpoint resolving!");
        }
    }

    private static class PartitionReader implements STableReader{
        @Override public Object open(String tableName) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public void close(Object o) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public String getTableName(Object o){ throw new UnsupportedOperationException(); }
        @Override public Result get(Object o,Object o2) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public CloseableIterator<Result> scan(Object o,Object o2) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public void openOperation(Object o) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public void closeOperation(Object o) throws IOException{ throw new UnsupportedOperationException(); }
    }

    private static class PartitionWriter implements STableWriter{
        @Override public void write(Object Table,Object o) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public void write(Object Table,Object o,SRowLock rowLock) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public void write(Object Table,Object o,boolean durable) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public void write(Object Table,List list) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public OperationStatus[] writeBatch(Object o,Pair[] puts) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public void delete(Object Table,Object o,SRowLock lock) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public SRowLock tryLock(Object o,byte[] rowKey) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public SRowLock tryLock(Object o,ByteSlice rowKey) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public SRowLock lockRow(Object Table,byte[] rowKey) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public void unLockRow(Object Table,SRowLock lock) throws IOException{ throw new UnsupportedOperationException(); }
        @Override public boolean checkAndPut(Object Table,byte[] family,byte[] qualifier,byte[] expectedValue,Object o) throws IOException{ throw new UnsupportedOperationException(); }
    }

    private static class NoOpCheckLock implements Lock{
        private boolean acquired = false;
        private boolean released = false;

        public NoOpCheckLock(){
        }

        @Override
        public void lock(){
            acquired = true;
        }

        @Override
        public void lockInterruptibly() throws InterruptedException{
            lock();
        }

        @Override
        public boolean tryLock(){
            lock();
            return true;
        }

        @Override
        public boolean tryLock(long time,TimeUnit unit) throws InterruptedException{
            lock();
            return true;
        }

        @Override public void unlock(){ released = true; }
        @Override public Condition newCondition(){ throw new UnsupportedOperationException(); }
    }
    private static abstract class TestPartition implements Partition{
        private Map<byte[],Set<Cell>> dataMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        private final String tableName;

        protected TestPartition(String tableName){
            this.tableName=tableName;
        }

        @Override
        public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException{
            OperationStatus[] status = new OperationStatus[mutations.length];
            for(int i=0;i<mutations.length;i++){
                Mutation mutation = mutations[i];
                if(mutation instanceof Put){
                    doPut((Put)mutation);
                }else if(mutation instanceof Delete){
                    doDelete((Delete)mutation);
                }
                status[i] = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
            }
            return status;
        }



        @Override
        public void mutate(Mutation mutation) throws IOException{
            batchMutate(new Mutation[]{mutation});
        }

        @Override
        public Collection<Cell> get(Get get) throws IOException{
            Set<Cell> cells=dataMap.get(get.getRow());
            if(cells==null) cells =Collections.emptySet();
            final Filter filter=get.getFilter();
            if(filter!=null){
                return Collections2.filter(cells,new Predicate<Cell>(){
                    @Override
                    public boolean apply(Cell cell){
                        try{
                            return filter.filterKeyValue(cell)==Filter.ReturnCode.INCLUDE;
                        }catch(IOException e){
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            return cells;
        }

        @Override
        public boolean rowInRange(byte[] row,int offset,int length){
            return true;
        }

        @Override
        public boolean rowInRange(ByteSlice slice){
            return true;
        }

        @Override
        public boolean isClosed(){
            return false;
        }

        @Override
        public boolean containsRange(byte[] start,byte[] stop){
            return true;
        }

        @Override
        public String getTableName(){
            return tableName;
        }

        @Override public void markWrites(long numWrites){ }
        @Override public void markReads(long numReads){ }

        private void doPut(Put mutation){
            byte[] row=mutation.getRow();
            Set<Cell> cells = dataMap.get(row);
            if(cells==null){
                cells = new TreeSet<>(new KeyValue.KVComparator());
                dataMap.put(row,cells);
            }

            List<Cell> familyCellMap=mutation.getFamilyCellMap().get(FixedSpliceConstants.DEFAULT_FAMILY_BYTES);
            for(Cell c:familyCellMap){
                if(cells.contains(c)){
                    cells.remove(c);
                }
                cells.add(c);
            }
        }

        private void doDelete(Delete mutation){
            Set<Cell> cells=dataMap.get(mutation.getRow());
            if(cells==null) return; //nothing to do
            List<Cell> cellsToDelete=mutation.getFamilyCellMap().get(FixedSpliceConstants.DEFAULT_FAMILY_BYTES);
            cells.removeAll(cellsToDelete);

            //now also remove all cells in between the time range of interest as well
            long timeStamp=mutation.getTimeStamp();
            Iterator<Cell> cellIterator = cells.iterator();
            while(cellIterator.hasNext()){
                if(cellIterator.next().getTimestamp()<timeStamp) cellIterator.remove();
            }
        }
    }
}
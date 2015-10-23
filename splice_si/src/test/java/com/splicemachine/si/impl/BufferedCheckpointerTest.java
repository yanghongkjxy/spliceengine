package com.splicemachine.si.impl;

import com.splicemachine.concurrent.ConcurrentTicker;
import com.splicemachine.concurrent.TickingClock;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.Checkpointer;
import com.splicemachine.storage.InMemoryPartition;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 10/2/15
 */
@SuppressWarnings("deprecation")
public class BufferedCheckpointerTest{

    @Test
    public void testCheckpointsASingleRow() throws Exception{

        TickingClock clock = new ConcurrentTicker(0l);
        InMemoryPartition partition = new InMemoryPartition(clock);


        clock.tickMillis(1l);
        byte[] originalValue = Encoding.encode("Hello");
        byte[] rowKey = Encoding.encode("rowKey");
        Put p = new Put(rowKey);
        p.add(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSpliceConstants.PACKED_COLUMN_BYTES,clock.currentTimeMillis(),originalValue);
        partition.mutate(p);

        Checkpointer checkpointer = new BufferedCheckpointer(partition,16);
        byte[] newValue = Encoding.encode("Goodbye");
        checkpointer.checkpoint(ByteSlice.wrap(rowKey),newValue,clock.currentTimeMillis(),-1l);

        //since the buffer has 16 elements, we should not have flushed yet.
        Assert.assertFalse("Already flushed!",mutationExecuted[0]);

        //force the flush
        checkpointer.flush();

        //NOW we should have flushed
        Assert.assertTrue("Already flushed!",mutationExecuted[0]);
    }

    @Test
    public void testCheckpointsAutomaticallyWhenBatchIsFull() throws Exception{
        int size = 10;
        final Map<byte[],byte[]> rowMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        final long mat = 2;

        final Set<byte[]> visitedPuts = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        final Set<byte[]> visitedDeletes = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        final int[] callCount = new int[]{0};
        Partition partition = new TestPartition(){
            @Override
            public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException{
                callCount[0]++;
                OperationStatus [] op = new OperationStatus[mutations.length];
                for(int i=0;i<mutations.length;i++){
                    byte[] row = mutations[i].getRow();
                    Assert.assertTrue("Missing row!",rowMap.containsKey(row));
                    byte[] value = rowMap.get(row);
                    if(mutations[i] instanceof Put){
                        Put p = (Put)mutations[i];
                        assertCorrectCheckpointPut(p,row,value,mat);
                        Assert.assertFalse("Already seen this row for puts!",visitedPuts.contains(row));
                        visitedPuts.add(row);
                    }else{
                        Assert.assertTrue("Does not include a delete!",mutations[i] instanceof Delete);
                        Delete d = (Delete)mutations[i];

                        assertCorrectCheckpointDelete(d,row,mat);
                        Assert.assertFalse("Already seen this row for deletes!",visitedDeletes.contains(row));
                        visitedDeletes.add(row);
                    }
                    op[i] = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
                }

                return op;
            }

        };

        int bufferSize=8;
        Checkpointer cp = new BufferedCheckpointer(partition,bufferSize,2,true); //sized for 1 automatic flush, and 1 manual
        for(int i=0;i<size;i++){
            byte[] rowKey = Encoding.encode("rowKey"+i);
            byte[] checkpointValue = Encoding.encode("checkpoint"+i);
            rowMap.put(rowKey,checkpointValue);
            cp.checkpoint(ByteSlice.wrap(rowKey),checkpointValue,mat,-1l);
        }

        Assert.assertEquals("Incorrect visited puts size!",bufferSize,visitedPuts.size());
        Assert.assertEquals("Incorrect visited puts size!",bufferSize,visitedDeletes.size());

        //now flush the rest
        cp.flush();
        Assert.assertEquals("Incorrect call count!",2,callCount[0]);
        Assert.assertEquals("Incorrect visited puts size!",size,visitedPuts.size());
        Assert.assertEquals("Incorrect visited puts size!",size,visitedDeletes.size());

        for(byte[] visitedRow:visitedPuts){
            Assert.assertTrue("Put did not come with a delete!",visitedDeletes.contains(visitedRow));
            Assert.assertTrue("Put row was not contained in the row map!",rowMap.containsKey(visitedRow));
        }

        for(byte[] visitedRow:visitedDeletes){
            Assert.assertTrue("Delete did not come with a put!",visitedPuts.contains(visitedRow));
            Assert.assertTrue("Delete row was not contained in the row map!",rowMap.containsKey(visitedRow));
        }
    }

    @Test
    public void testCheckpointsASingleRowNoDelete() throws Exception{

        final byte[] rowKey = Encoding.encode("rowKey");
        final byte[] newValue = Encoding.encode("Goodbye");
        final boolean[] mutationExecuted = new boolean[]{false};
        final long mat = 2;
        Partition partition = new TestPartition(){
            @Override
            public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException{
                Assert.assertFalse("Mutation was executed twice!",mutationExecuted[0]);
                mutationExecuted[0] = true;
                Assert.assertEquals("incorrect mutation length!",1,mutations.length);

                Assert.assertTrue("Was not an instanceof Put!",mutations[0] instanceof Put);
                Put checkpointPut = (Put)mutations[0];

                assertCorrectCheckpointPut(checkpointPut,rowKey,newValue,mat);

                OperationStatus [] op = new OperationStatus[1];
                op[0] = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
                return op;
            }

            @Override public void mutate(Mutation mutation) throws IOException{ }
            @Override public Lock lock(byte[] rowKey) throws IOException{ return null; }
        };
        Checkpointer checkpointer = new BufferedCheckpointer(partition,16,false);
        checkpointer.checkpoint(ByteSlice.wrap(rowKey),newValue,mat,-1l);

        //since the buffer has 16 elements, we should not have flushed yet.
        Assert.assertFalse("Already flushed!",mutationExecuted[0]);

        //force the flush
        checkpointer.flush();

        //NOW we should have flushed
        Assert.assertTrue("Already flushed!",mutationExecuted[0]);
    }

    @Test
    public void testCheckpointsAutomaticallyWhenBatchIsFullNoDelete() throws Exception{
        int size = 10;
        final Map<byte[],byte[]> rowMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        final long mat = 2;

        final Set<byte[]> visitedPuts = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        final int[] callCount = new int[]{0};
        Partition partition = new TestPartition(){
            @Override
            public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException{
                callCount[0]++;
                OperationStatus [] op = new OperationStatus[mutations.length];
                for(int i=0;i<mutations.length;i++){
                    byte[] row = mutations[i].getRow();
                    Assert.assertTrue("Missing row!",rowMap.containsKey(row));
                    byte[] value = rowMap.get(row);
                    Assert.assertTrue("Was not an instanceof Put!",mutations[i] instanceof Put);
                    Put p = (Put)mutations[i];
                    assertCorrectCheckpointPut(p,row,value,mat);
                    Assert.assertFalse("Already seen this row for puts!",visitedPuts.contains(row));
                    visitedPuts.add(row);
                    op[i] = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
                }

                return op;
            }

        };

        int bufferSize=8;
        Checkpointer cp = new BufferedCheckpointer(partition,bufferSize,2,false); //sized for 1 automatic flush, and 1 manual
        for(int i=0;i<size;i++){
            byte[] rowKey = Encoding.encode("rowKey"+i);
            byte[] checkpointValue = Encoding.encode("checkpoint"+i);
            rowMap.put(rowKey,checkpointValue);
            cp.checkpoint(ByteSlice.wrap(rowKey),checkpointValue,mat,-1l);
        }
>>>>>>> cfee1ef... DB-3854: Created and integrated a CheckpointResolver.


    }

    private abstract class TestPartition implements Partition{
        @Override
        public Lock lock(byte[] rowKey) throws IOException{
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Cell> get(Get get) throws IOException{
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rowInRange(byte[] row,int offset,int length){
            throw new UnsupportedOperationException();
        }

        @Override public boolean rowInRange(ByteSlice slice){
            throw new UnsupportedOperationException();
        }

        @Override public boolean isClosed(){ return false; }

        @Override
        public boolean containsRange(byte[] start,byte[] stop){
            throw new UnsupportedOperationException();
        }

        @Override public void mutate(Mutation mutation) throws IOException{
            throw new UnsupportedOperationException();
        }
>>>>>>> 03ec992... Fixing early-termination error when compacting rolled back records



    }
}

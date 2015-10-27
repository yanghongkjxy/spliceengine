package com.splicemachine.storage;

import com.splicemachine.si.api.Partition;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 10/6/15
 */
public class Region implements Partition{
    private final HRegion region;
    private final boolean useCoprocessorWithGet;

    public Region(HRegion region){
       this(region,true);
    }

    public Region(HRegion region,boolean useCoprocessorWithGet){
        this.region=region;
        this.useCoprocessorWithGet = useCoprocessorWithGet;
    }

    @Override
    public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException{
        return region.batchMutate(mutations);
    }

    @Override
    public void mutate(Mutation mutation) throws IOException{
        if(mutation instanceof Put)
            region.put((Put)mutation);
        else if(mutation instanceof Delete)
            region.delete((Delete)mutation);
    }

    @Override
    public Lock lock(byte[] rowKey) throws IOException{
        return new LazyLock(rowKey);
    }

    @Override
    public List<Cell> get(Get get) throws IOException{
        return region.get(get,useCoprocessorWithGet);
    }

    @Override
    public boolean rowInRange(byte[] row,int offset,int length){
        return HRegionUtil.containsRow(region.getRegionInfo(),row,offset,length);
    }

    @Override
    public boolean rowInRange(ByteSlice slice){
        return rowInRange(slice.array(),slice.offset(),slice.length());
    }

    @Override
    public boolean isClosed(){
        return region.isClosed()||region.isClosing();
    }

    @Override
    public boolean containsRange(byte[] start,byte[] stop){
        return HRegionUtil.containsRange(region,start,stop);
    }

    @Override
    public String getTableName(){
        return region.getTableDesc().getNameAsString();
    }

    @Override
    public void markWrites(long numWrites){
        HRegionUtil.updateWriteRequests(region,numWrites);
    }

    @Override
    public void markReads(long numReads){
        HRegionUtil.updateReadRequests(region,numReads);
    }


    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof Region)) return false;

        Region region1=(Region)o;

        return region.equals(region1.region);
    }

    @Override
    public int hashCode(){
        return region.hashCode();
    }

    /* ***********************************************************************************************************/
    /*private helper classes and methods*/
    private class LazyLock implements Lock{
        private final byte[] rowKey;

        private volatile HRegion.RowLock rowLock;

        public LazyLock(byte[] rowKey){
            this.rowKey = rowKey;
        }

        @Override
        public void lock(){
            try{
                tryLock(Long.MAX_VALUE,TimeUnit.NANOSECONDS);
            }catch(InterruptedException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException{
            tryLock(Long.MAX_VALUE,TimeUnit.NANOSECONDS);
        }

        @Override
        public boolean tryLock(){
            try{
                this.rowLock = region.getRowLock(rowKey,false);
                return rowLock!=null;
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean tryLock(long time,TimeUnit unit) throws InterruptedException{
            try{
                this.rowLock=region.getRowLock(rowKey,true);
                return rowLock!=null;
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public void unlock(){
            if(rowLock!=null) rowLock.release();
        }

        @Override
        public Condition newCondition(){
            throw new UnsupportedOperationException("Cannot support conditions on region locks");
        }
    }
}

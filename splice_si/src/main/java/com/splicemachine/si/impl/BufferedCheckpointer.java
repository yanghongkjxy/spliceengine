package com.splicemachine.si.impl;

import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.si.api.Checkpointer;
import com.splicemachine.storage.api.Partition;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 10/5/15
 */
public class BufferedCheckpointer implements Checkpointer{
    private final int maxBufferSize;
    private final Partition region;
    private final boolean issueDeletes;

    private Checkpoint[] buffer;
    private int bufferPos;
    private transient int shift;
    private transient Mutation[] cachedMutationArray;


    public BufferedCheckpointer(Partition region,int maxBufferSize){
        this(region,maxBufferSize,true);
    }

    public BufferedCheckpointer(Partition region,int maxBufferSize,boolean issueDeletes){
       this(region,maxBufferSize,16,issueDeletes);
    }

    public BufferedCheckpointer(Partition region,int maxBufferSize,int initialBufferSize,boolean issueDeletes){
        this.region=region;
        this.issueDeletes = issueDeletes;

        int s = 1;
        while(s<maxBufferSize){
            s<<=1;
        }

        this.maxBufferSize = s;
        if(initialBufferSize>s)
            initialBufferSize = s;
        else{
            s = 1;
            while(s<initialBufferSize)
                s<<=1;
            initialBufferSize = s;
        }

        this.buffer = new Checkpoint[initialBufferSize];
        this.shift = initialBufferSize-1;
        this.bufferPos = 0;
    }

    @Override
    public void checkpoint(ByteSlice rowKey,byte[] checkpointValue,long timestamp,long commitTimestamp) throws IOException{
        Checkpoint cp = buffer[bufferPos];
        if(cp==null){
            cp = new Checkpoint();
            buffer[bufferPos] = cp;
        }
        cp.set(rowKey, checkpointValue, timestamp, commitTimestamp);
        advanceBuffer();
    }


    @Override
    public void flush() throws IOException{
        writeBuffer(bufferPos);
        bufferPos=0;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void advanceBuffer() throws IOException{
        int s = (bufferPos+1) & shift;
        if(s==0){
            if(buffer.length<maxBufferSize){
                int newSize = Math.min(maxBufferSize,2*buffer.length);
                buffer =Arrays.copyOf(buffer,newSize);
                shift = newSize-1;
            }else{
               writeBuffer(buffer.length);
            }
        }
        bufferPos = s;
    }

    private void writeBuffer(int size) throws IOException{
        Mutation[] mutations = getMutationArray(size);
        if(issueDeletes)
            fillMutationsWithDelete(mutations,size);
        else
            fillMutationsNoDelete(mutations,size);

        region.batchMutate(mutations);
    }

    private void fillMutationsNoDelete(Mutation[] mutations,int size){
        /*
         * Fill the mutations, but do not include a delete with each put
         */
        for(int bPos=0,dPos=0;bPos<size;bPos++,dPos++){
            Checkpoint checkpoint=buffer[bPos];
            mutations[dPos] = getCheckpointPut(checkpoint);
        }
    }

    private void fillMutationsWithDelete(Mutation[] mutations,int size){
        /*
         * Fill the mutations, including a delete with each record
         */
        for(int bPos=0,dPos=0;bPos<size;bPos++,dPos+=2){
            Checkpoint checkpoint=buffer[bPos];
            mutations[dPos] = getCheckpointPut(checkpoint);
            mutations[dPos+1] = getCheckpointDelete(checkpoint);
        }
    }

    @SuppressWarnings("deprecation")
    private Mutation getCheckpointDelete(Checkpoint checkpoint){
        ByteSlice key = checkpoint.rowKey;
        long timestamp = checkpoint.timestamp-1;
        Delete d = new Delete(key.array(),key.offset(),key.length());
        d=d.deleteColumns(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,timestamp)
                .deleteColumns(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,timestamp)
                .deleteColumns(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSpliceConstants.PACKED_COLUMN_BYTES,timestamp);
        d.setAttribute(FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        d.setDurability(Durability.SKIP_WAL);
        return d;
    }

    @SuppressWarnings("deprecation")
    private Mutation getCheckpointPut(Checkpoint checkpoint){
        ByteSlice key = checkpoint.rowKey;
        long ts = checkpoint.timestamp;

        Put p = new Put(key.array(),key.offset(),key.length());
        p.setAttribute(FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        p.setDurability(Durability.SKIP_WAL);

        //add the data field
        p.add(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSpliceConstants.PACKED_COLUMN_BYTES,ts,checkpoint.checkpointValue);

        long cTs = checkpoint.commitTimestamp;
        byte[] checkpointCellValue;
        if(cTs>0){
            checkpointCellValue = Bytes.toBytes(cTs);
        }else{
            checkpointCellValue = FixedSpliceConstants.EMPTY_BYTE_ARRAY;
        }

        //add the checkpoint field
        p.add(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,ts,checkpointCellValue);
        return p;
    }

    private Mutation[] getMutationArray(int size){
        int arraySize = issueDeletes? 2*size: size;
        Mutation[] mutations;
        if(cachedMutationArray!=null && cachedMutationArray.length==arraySize)
            mutations = cachedMutationArray;
        else
            mutations = cachedMutationArray = new Mutation[arraySize];

        return mutations;
    }

    private class Checkpoint{
        ByteSlice rowKey;
        byte[] checkpointValue;
        long timestamp;
        long commitTimestamp;

        void set(ByteSlice rowKey, byte[] checkpointValue,long timestamp, long commitTimestamp){
            if(this.rowKey==null)
                this.rowKey = new ByteSlice(rowKey);
            else this.rowKey.set(rowKey);
            this.checkpointValue = checkpointValue;
            this.timestamp = timestamp;
            this.commitTimestamp = commitTimestamp;
        }
    }
}

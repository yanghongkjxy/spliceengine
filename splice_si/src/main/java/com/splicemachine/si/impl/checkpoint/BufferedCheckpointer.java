package com.splicemachine.si.impl.checkpoint;

import com.splicemachine.si.api.Checkpointer;
import com.splicemachine.si.api.Partition;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 10/5/15
 */
@NotThreadSafe
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
    public void checkpoint(byte[] rowKey,int keyOff,int keyLen,byte[] checkpointValue,long timestamp,long commitTimestamp) throws IOException{
        Checkpoint cp = buffer[bufferPos];
        if(cp==null){
            cp = new Checkpoint();
            buffer[bufferPos] = cp;
        }
        cp.set(rowKey,keyOff,keyLen,checkpointValue,timestamp,commitTimestamp);
        advanceBuffer();
    }

    @Override
    public void flush() throws IOException{
        writeBuffer(bufferPos);
        bufferPos=0;
    }

    @Override public boolean issuesDeletes(){ return issueDeletes; }

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
        return CheckpointerUtils.checkpointDelete(key,timestamp);
    }

    @SuppressWarnings("deprecation")
    private Mutation getCheckpointPut(Checkpoint checkpoint){
        ByteSlice key = checkpoint.rowKey;
        long ts = checkpoint.timestamp;

        return CheckpointerUtils.checkpointPut(key,ts,checkpoint.checkpointValue,checkpoint.commitTimestamp);
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

    private static class Checkpoint{
        ByteSlice rowKey;
        byte[] checkpointValue;
        long timestamp;
        long commitTimestamp;

        public void set(byte[] rowKey,int keyOff,int keyLen,byte[] checkpointValue,long timestamp,long commitTimestamp){
            if(this.rowKey==null)
                this.rowKey = ByteSlice.wrap(rowKey,keyOff,keyLen);
            else this.rowKey.set(rowKey,keyOff,keyLen);
            this.checkpointValue = checkpointValue;
            this.timestamp = timestamp;
            this.commitTimestamp = commitTimestamp;

        }

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

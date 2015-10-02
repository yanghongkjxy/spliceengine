package com.splicemachine.si.api;

import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * @author Scott Fines
 *         Date: 10/5/15
 */
public class BufferedCheckpointer implements Checkpointer{
    private final int maxBufferSize;
    private final HRegion region;
    private final boolean issueDeletes;

    private Checkpoint[] buffer;
    private int bufferPos;
    private transient int shift;


    public BufferedCheckpointer(HRegion region,int maxBufferSize,int initialBufferSize,boolean issueDeletes){
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
    public void checkpoint(ByteSlice rowKey,byte[] checkpointValue,long timestamp,long commitTimestamp){
        Checkpoint cp = buffer[bufferPos];
        if(cp==null){
            cp = new Checkpoint();
        }
        cp.set(rowKey, checkpointValue, timestamp, commitTimestamp);
        bufferPos = (bufferPos+1) & shift;
        if(bufferPos==0) flush();
    }

    @Override
    public void flush(){

    }

    /*private helper methods*/
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

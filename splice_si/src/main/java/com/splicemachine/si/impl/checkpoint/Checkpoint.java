package com.splicemachine.si.impl.checkpoint;

/**
 * @author Scott Fines
 *         Date: 11/3/15
 */
class Checkpoint{
    byte[] rowKey;
    int offset;
    int length;
    long checkpointTimestamp;

    public void set(byte[] rowKey, int offset,int length, long checkpointTimestamp){
        this.rowKey = rowKey;
        this.offset = offset;
        this.length = length;
        this.checkpointTimestamp = checkpointTimestamp;
    }

    public void set(Checkpoint checkpoint){
        this.rowKey = checkpoint.rowKey;
        this.offset = checkpoint.offset;
        this.length = checkpoint.length;
        this.checkpointTimestamp = checkpoint.checkpointTimestamp;
    }
}

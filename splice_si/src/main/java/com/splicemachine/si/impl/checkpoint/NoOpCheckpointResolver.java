package com.splicemachine.si.impl.checkpoint;

import com.splicemachine.utils.ByteSlice;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 10/30/15
 */
public class NoOpCheckpointResolver implements CheckpointResolver{
    public static final NoOpCheckpointResolver INSTANCE = new NoOpCheckpointResolver();

    private NoOpCheckpointResolver(){}

    @Override public void resolveCheckpoint(ByteSlice rowKey,long checkpointTimestamp) throws IOException{ }

    @Override public void resolveCheckpoint(byte[] rowKeyArray,int offset,int length,long checkpointTimestamp) throws IOException{ }

    @Override public void resolveCheckpoint(Checkpoint... checkpoint) throws IOException{ }
}

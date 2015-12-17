package com.splicemachine.si.impl.checkpoint;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 10/30/15
 */
public class NoOpCheckpointResolver implements CheckpointResolver{
    public static final NoOpCheckpointResolver INSTANCE = new NoOpCheckpointResolver();

    private NoOpCheckpointResolver(){}

    @Override public void resolveCheckpoint(byte[] rowKeyArray,int offset,int length,long checkpointTimestamp) throws IOException{ }

    @Override public void pauseCheckpointing(){ }
    @Override public void resumeCheckpointing(){ }

    @Override public void resolveCheckpoint(Checkpoint... checkpoint) throws IOException{ }
}
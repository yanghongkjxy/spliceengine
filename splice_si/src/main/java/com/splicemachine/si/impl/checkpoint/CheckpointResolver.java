package com.splicemachine.si.impl.checkpoint;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
public interface CheckpointResolver{

    void resolveCheckpoint(Checkpoint... checkpoint) throws IOException;

    void resolveCheckpoint(byte[] rowKeyArray,int offset,int length,long checkpointTimestamp) throws IOException;

    /**
     * Indicate that this resolver should stop (temporarily) the resolution of records, because
     * it may cause problems with some other concurrently running operation (such as Compaction) which
     * would also affect the structure of the row.
     *
     * Multiple calls to this without calling {@link #resumeCheckpointing()} ()} will do nothing
     */
    void pauseCheckpointing();

    /**
     * Indicate that this resolver should resume resolution of records. Calling this without first calling
     * {@link #pauseCheckpointing()} ()}} will do nothing.
     */
    void resumeCheckpointing();
}


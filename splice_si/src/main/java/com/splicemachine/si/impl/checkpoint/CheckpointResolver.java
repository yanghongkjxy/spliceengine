package com.splicemachine.si.impl.checkpoint;

import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
public interface CheckpointResolver{

    void resolveCheckpoint(Checkpoint... checkpoint) throws IOException;

    void resolveCheckpoint(ByteSlice rowKey,long checkpointTimestamp) throws IOException;

    void resolveCheckpoint(byte[] rowKeyArray,int offset,int length,long checkpointTimestamp) throws IOException;
}

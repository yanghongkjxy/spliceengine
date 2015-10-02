package com.splicemachine.si.api;

import com.splicemachine.utils.ByteSlice;

/**
 * Responsible for "checkpointing" a row. Specificially, this takes the result of a merged process (the checkpoint value),
 * and writes it back as a checkpoint version to the specified row.
 *
 * @author Scott Fines
 *         Date: 10/2/15
 */
public interface Checkpointer{

    void checkpoint(ByteSlice rowKey, byte[] checkpointValue,long timestamp,long commitTimestamp);

    void flush();
}

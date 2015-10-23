package com.splicemachine.si.api;

import com.splicemachine.utils.ByteSlice;

/**
 * Represents a Roll Forward structure.
 *
 * @author Scott Fines
 * Date: 6/26/14
 */
public interface RollForward {

    void submitForResolution(ByteSlice rowKey, long txnId);

	void recordResolved(ByteSlice rowKey,long txnId);

	void recordResolved(byte[] array, int offset, int length,long txnId);

	void pauseRollForward();

	void resumeRollForward();
}

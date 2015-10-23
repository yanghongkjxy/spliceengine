package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.api.RollForward;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class NoopRollForward implements RollForward{
		public static final RollForward INSTANCE = new NoopRollForward();

		private NoopRollForward(){}

	@Override public void submitForResolution(ByteSlice rowKey, long txnId) {  }
		@Override public void recordResolved(ByteSlice rowKey, long txnId) {  }
	@Override public void recordResolved(byte[] array,int offset,int length,long txnId){ }

	@Override public void pauseRollForward(){ }

	@Override public void resumeRollForward(){ }
}

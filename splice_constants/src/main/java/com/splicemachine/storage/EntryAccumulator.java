package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.index.BitIndex;

/**
 * @author Scott Fines
 *         Created on: 7/9/13
 */
public interface EntryAccumulator<T extends EntryAccumulator<T>>{

    void add(int position,byte[] data,int offset,int length);

    void addScalar(int position,byte[] data,int offset,int length);

    void addFloat(int position,byte[] data,int offset,int length);

    void addDouble(int position,byte[] data,int offset,int length);

    BitSet getRemainingFields();

    boolean isFinished();

    byte[] finish();

    boolean checkFilterAfter();

    void reset();

    boolean isInteresting(BitIndex potentialIndex);

    void complete();

    /**
     * @return true if the accumulator has added any fields
     */
    boolean hasAccumulated();
}

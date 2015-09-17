package com.splicemachine.si.api;

import com.splicemachine.storage.EntryAccumulator;

import java.io.Closeable;
import java.io.IOException;

public interface RowAccumulator<Data> extends Closeable {
    boolean isOfInterest(Data value);
    boolean accumulate(Data value) throws IOException;
    boolean isFinished();

    /**
     *
     * @return true if the accumulator has accumulated any data
     */
    boolean hasAccumulated();
    byte[] result();
    long getBytesVisited();
    boolean isCountStar();
    void reset();

    EntryAccumulator getEntryAccumulator();
}

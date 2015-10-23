package com.splicemachine.si.data.hbase;

import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.storage.EntryAccumulator;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/23/15
 */
public class NoOpAccumulator<T> implements RowAccumulator<T>{
    public static final RowAccumulator INSTANCE = new NoOpAccumulator();

    private NoOpAccumulator(){}

    @SuppressWarnings("unchecked")
    public static <T> NoOpAccumulator<T> instance(){
        return (NoOpAccumulator<T>)INSTANCE;
    }

    @Override public boolean isOfInterest(T value){ return false; }
    @Override public boolean accumulate(T value) throws IOException{ return false; }
    @Override public boolean isFinished(){ return true; }
    @Override public boolean hasAccumulated(){ return false; }
    @Override public byte[] result(){ return FixedSIConstants.EMPTY_BYTE_ARRAY; }
    @Override public long getBytesVisited(){ return 0; }
    @Override public boolean isCountStar(){ return false; }
    @Override public void reset(){ }
    @Override public EntryAccumulator getEntryAccumulator(){ return null; }
    @Override public void close() throws IOException{ }
}

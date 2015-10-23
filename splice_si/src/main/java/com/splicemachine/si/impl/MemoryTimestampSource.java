package com.splicemachine.si.impl;

import com.splicemachine.si.api.TimestampSource;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 12/7/15
 */
public class MemoryTimestampSource implements TimestampSource{
    private AtomicLong counter;

    private volatile long rememberedTimestamp;
    public MemoryTimestampSource(long startPoint){
        this.counter = new AtomicLong(startPoint);
    }

    @Override
    public long nextTimestamp(){
        return counter.incrementAndGet();
    }

    @Override
    public void rememberTimestamp(long timestamp){
        rememberedTimestamp = timestamp;
    }

    @Override
    public long retrieveTimestamp(){
        return rememberedTimestamp;
    }

    @Override public void shutdown(){ }
}

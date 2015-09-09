package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;

/**
 * @author Scott Fines
 *         Date: 9/9/15
 */
public class NoopKeepAliveScheduler implements KeepAliveScheduler{
    public static final KeepAliveScheduler INSTANCE = new NoopKeepAliveScheduler();

    private NoopKeepAliveScheduler(){}

    @Override public void scheduleKeepAlive(Txn txn){ }

    @Override public void start(){ }

    @Override public void stop(){ }
}

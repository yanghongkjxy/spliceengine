package com.splicemachine.si.impl;

import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnStore;

/**
 * @author Scott Fines
 *         Date: 7/3/14
 */
public class TransactionLifecycle{
    private static final Object lock=new Integer("2");

    private static volatile @ThreadSafe TxnLifecycleManager lifecycleManager;
    private static volatile TxnLifecycleObserver tcObserver;

    public static TxnLifecycleManager getLifecycleManager(){
        TxnLifecycleManager tc=lifecycleManager;
        if(tc==null)
            tc=initialize();
        return tc;
    }

    public static TxnLifecycleObserver getLifecycleObserver(){
        TxnLifecycleObserver tco = tcObserver;
        if(tco==null){
            initialize();
            tco = tcObserver;
        }
        return tco;
    }

    private static TxnLifecycleManager initialize(){
        synchronized(lock){
            TxnLifecycleManager tc=lifecycleManager;
            TxnLifecycleObserver ka=tcObserver;
            if(tc!=null && ka!=null) return tc;

            TxnStore txnStore=SIFactoryDriver.siFactory.getTxnStore();
            ClientTxnLifecycleManager lfManager=new ClientTxnLifecycleManager(TransactionTimestamps.getTimestampSource());
            lfManager.setStore(txnStore);

            if(ka==null){
                KeepAliveScheduler ks=new QueuedKeepAliveScheduler(SIConstants.transactionKeepAliveInterval/2,
                        SIConstants.transactionKeepAliveInterval,
                        SIConstants.transactionKeepAliveThreads,txnStore);
                ka = tcObserver = new TxnLifecycleObserver(ks);
            }

            lfManager.setLifecycleObserver(ka);
            lifecycleManager=lfManager;
            return lfManager;
        }
    }

}

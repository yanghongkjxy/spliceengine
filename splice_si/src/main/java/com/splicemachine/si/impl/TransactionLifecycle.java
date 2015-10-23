package com.splicemachine.si.impl;

import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnStore;

/**
 * @author Scott Fines
 *         Date: 7/3/14q
 */
public class TransactionLifecycle{
    private static final Object lock=new Object();

    private static volatile @ThreadSafe TxnLifecycleManager lifecycleManager;
    private static volatile TxnLifecycleObserver tcObserver;
    private static volatile MinimumTransactionWatcher matWatcher;

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

    public static void setTransactionWatcher(MinimumTransactionWatcher watcher){
        matWatcher = watcher;
    }

    public static MinimumTransactionWatcher getMatWatcher(){
        return matWatcher;
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

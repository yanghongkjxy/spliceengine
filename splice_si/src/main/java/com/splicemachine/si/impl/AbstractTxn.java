package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public abstract class AbstractTxn extends AbstractTxnView implements Txn{
    protected TxnLifecycleObserver tcObserver;

    protected AbstractTxn(){ }

    protected AbstractTxn(long txnId,
                          long beginTimestamp,
                          IsolationLevel isolationLevel,TxnLifecycleObserver tcObserver){
        super(txnId,beginTimestamp,isolationLevel);
        this.tcObserver = tcObserver;
    }

    @Override
    public TxnView view(){
        //since we are a TxnView, we can just return ourself
        return this;
    }
}

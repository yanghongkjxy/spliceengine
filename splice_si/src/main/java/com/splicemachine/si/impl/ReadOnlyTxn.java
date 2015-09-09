package com.splicemachine.si.impl;

import com.splicemachine.si.api.CannotCommitException;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public class ReadOnlyTxn extends AbstractTxn{

    private static final Logger LOG=Logger.getLogger(ReadOnlyTxn.class);
    private volatile TxnView parentTxn;
    private AtomicReference<State> state=new AtomicReference<>(State.ACTIVE);
    private final TxnLifecycleManager tc;
    private final boolean additive;

    public static Txn create(long txnId,IsolationLevel isolationLevel,TxnLifecycleManager tc,TxnLifecycleObserver tcObserver){
        return new ReadOnlyTxn(txnId,txnId,isolationLevel,Txn.ROOT_TRANSACTION,tc,tcObserver,false);
    }

    public static Txn wrapReadOnlyInformation(TxnView myInformation,
                                              TxnLifecycleManager control,
                                              TxnLifecycleObserver tcObserver){
        return new ReadOnlyTxn(myInformation.getTxnId(),
                myInformation.getBeginTimestamp(),
                myInformation.getIsolationLevel(),
                myInformation.getParentTxnView(),
                control,
                tcObserver,
                myInformation.isAdditive());
    }

    public static Txn createReadOnlyTransaction(long txnId,
                                                TxnView parentTxn,
                                                long beginTs,
                                                IsolationLevel level,
                                                boolean additive,
                                                TxnLifecycleManager control,
                                                TxnLifecycleObserver tcObserver ){
        return new ReadOnlyTxn(txnId,beginTs,level,parentTxn,control,tcObserver,additive);
    }

    public static ReadOnlyTxn createReadOnlyChildTransaction(
            TxnView parentTxn,
            TxnLifecycleManager tc,
            TxnLifecycleObserver tcObserver,
            boolean additive){
        //make yourself a copy of the parent transaction, for the purposes of reading
        return new ReadOnlyTxn(parentTxn.getTxnId(),
                parentTxn.getBeginTimestamp(),
                parentTxn.getIsolationLevel(),parentTxn,tc,tcObserver,additive);
    }

    public static ReadOnlyTxn createReadOnlyParentTransaction(long txnId,long beginTimestamp,
                                                              IsolationLevel isolationLevel,
                                                              TxnLifecycleManager tc,
                                                              TxnLifecycleObserver tcObserver,
                                                              boolean additive){
        return new ReadOnlyTxn(txnId,beginTimestamp,isolationLevel,ROOT_TRANSACTION,tc,tcObserver,additive);
    }

    protected ReadOnlyTxn(long txnId,
                          long beginTimestamp,
                          IsolationLevel isolationLevel,
                          TxnView parentTxn,
                          TxnLifecycleManager tc,
                          TxnLifecycleObserver tcObserver,
                          boolean additive){
        super(txnId,beginTimestamp,isolationLevel,tcObserver);
        this.parentTxn=parentTxn;
        this.tc=tc;
        this.additive=additive;

    }

    @Override
    public boolean isAdditive(){
        return additive;
    }

    @Override
    public long getCommitTimestamp(){
        return -1l; //read-only transactions do not need to commit
    }

    @Override
    public long getGlobalCommitTimestamp(){
        return -1l; //read-only transactions do not need a global commit timestamp
    }

    @Override
    public long getEffectiveCommitTimestamp(){
        if(state.get()==State.ROLLEDBACK) return -1l;
        if(parentTxn!=null)
            return parentTxn.getEffectiveCommitTimestamp();
        return -1l; //read-only transactions do not need to commit, so they don't need a TxnId
    }

    @Override
    public TxnView getParentTxnView(){
        return parentTxn;
    }

    @Override
    public State getState(){
        return state.get();
    }

    @Override
    public void commit() throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Before commit: txn=%s",this);
        boolean shouldContinue;
        do{
            State currState=state.get();
            switch(currState){
                case COMMITTED:
                    return;
                case ROLLEDBACK:
                    throw new CannotCommitException(txnId,currState);
                default:
                    shouldContinue=!state.compareAndSet(currState,State.COMMITTED);
            }
        }while(shouldContinue);
        observeFinished();
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"After commit: txn=%s",this);
    }


    @Override
    public void rollback() throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Before rollback: txn=%s",this);
        boolean shouldContinue;
        do{
            State currState=state.get();
            switch(currState){
                case COMMITTED:
                case ROLLEDBACK:
                    return;
                default:
                    shouldContinue=!state.compareAndSet(currState,State.ROLLEDBACK);
            }
        }while(shouldContinue);
        observeFinished();
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"After rollback: txn=%s",this);
    }

    @Override
    public boolean allowsWrites(){
        return false;
    }

    @Override
    public Txn elevateToWritable(byte[] writeTable) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Before elevateToWritable: txn=%s,writeTable=%s",this,writeTable);
        assert state.get()==State.ACTIVE:"Cannot elevate an inactive transaction!";
        Txn newTxn;
        if((parentTxn!=null && !ROOT_TRANSACTION.equals(parentTxn))){
            /*
			 * We are a read-only child transaction of a parent. This means that we didn't actually
			 * create a child transaction id or a begin timestamp of our own. Instead of elevating,
			 * we actually create a writable child transaction.
			 */
            newTxn=tc.beginChildTransaction(parentTxn,isolationLevel,additive,writeTable);
        }else{
            newTxn=tc.elevateTransaction(this,writeTable); //requires at least one network call
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"After elevateToWritable: newTxn=%s",newTxn);
        return newTxn;
    }

    public void parentWritable(TxnView newParentTxn){
        if(newParentTxn==parentTxn) return;
        this.parentTxn=newParentTxn;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void observeFinished(){
        /*
         * When we create a ReadOnly child of a ReadOnly parent, then we inherit the transaction id. In that
         * case we could spuriously affect the observer (since the two transactions are effectively the same). Therefore,
         * we only mark ourselves finished if our parent transaction id is not the same as ours
         */
//        if(getParentTxnId()!=getParentTxnId())
            tcObserver.txnFinished(this);
    }
}

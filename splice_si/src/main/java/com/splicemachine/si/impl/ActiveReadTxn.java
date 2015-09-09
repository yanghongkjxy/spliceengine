package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 9/9/15
 */
public class ActiveReadTxn extends AbstractTxnView{
    private TxnView parentTxn;

    public ActiveReadTxn(long beginTimestamp){
       this(beginTimestamp,Txn.IsolationLevel.SNAPSHOT_ISOLATION);
    }

    public ActiveReadTxn(long beginTimestamp,Txn.IsolationLevel isolationLevel){
        this(beginTimestamp, beginTimestamp,isolationLevel);
    }

    public ActiveReadTxn(long txnId,long beginTimestamp,Txn.IsolationLevel isolationLevel){
        this(txnId, beginTimestamp, isolationLevel,Txn.ROOT_TRANSACTION);
    }

    public ActiveReadTxn(long txnId,long beginTimestamp,Txn.IsolationLevel isolationLevel,TxnView parentTxn){
        super(txnId,beginTimestamp,isolationLevel);
        this.parentTxn  = parentTxn;
    }

    @Override public long getCommitTimestamp(){ return -1l; }
    @Override public boolean isAdditive(){ return false; }
    @Override public long getGlobalCommitTimestamp(){ return -1l; }

    @Override
    public TxnView getParentTxnView(){
        return parentTxn;
    }

    @Override
    public Txn.State getState(){
        return Txn.State.ACTIVE;
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException{
        super.readExternal(input);
        parentTxn = (TxnView)input.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException{
        super.writeExternal(output);
        output.writeObject(parentTxn);
    }
}

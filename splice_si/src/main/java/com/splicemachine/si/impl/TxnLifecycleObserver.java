package com.splicemachine.si.impl;

import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Date: 9/9/15
 */
@ThreadSafe
public class TxnLifecycleObserver{
    private static final Comparator<Txn> comparator = new Comparator<Txn>(){
        @Override
        public int compare(Txn o1,Txn o2){
            //short circuit--if you are pointing to the same object, we are clearly equivalent
            if(o1==o2) return 0;
            int compare = Long.compare(o1.getBeginTimestamp(),o2.getBeginTimestamp());
            if(compare!=0) return compare;

            /*
             * The two begin timestamps are the same. This means that we are dealing with two separate
             * representations of the same logical transaction. By definition, we know that
             * t1.equals(t2) in this case, so we have the following combinations:
             *
             * 1. t1 ReadOnly, t2 ReadOnly => Either t1 is a child of t2, t2 is a child of t1, or they are equivalent
             * 2. t1 ReadOnly, t2 Writable => sort t1 before t2
             * 3. t1 Writable, t2 ReadOnly => sort t1 after t2
             * 4. t1 Writable, t2 Writable => logically equivalent
             *
             */
            if(o1.allowsWrites()){
                if(o2.allowsWrites()) return 0;
                else return 1; //sort t1 after
            }else if(o2.allowsWrites()){
                return -1; //sort t1 before
            }else{
               /*
                * we have two read only transactions with the same id. This may be caused by a ReadOnly child
                 * of a ReadOnly transaction, so we check for descendency to determine proper behavior, sorting
                 * children before parents
                */
                if(o1.descendsFrom(o2.getParentTxnView())) return -1;
                else return 1;
            }
        }
    };
    private final ConcurrentSkipListSet<Txn> txns;
    private final KeepAliveScheduler keepAliveScheduler;
    private final AtomicReference<Txn> lastActiveTxn= new AtomicReference<>(Txn.ROOT_TRANSACTION);

    public TxnLifecycleObserver(KeepAliveScheduler keepAliveScheduler){
        this.keepAliveScheduler=keepAliveScheduler;
        this.txns=new ConcurrentSkipListSet<>(comparator);
    }

    /**
     * Indicates that a transaction has begun.
     * @param txn the transaction
     */
    public void txnBegun(Txn txn){
        /*
         * In the task framework, we create a child transaction on one server, but commit or roll it back
         * on another. This means that we would have a memory leak here if we allow child transactions
         * to be present (because we would not register txnComplete on the server that registered txnBegun).
         *
         * Thankfully, for the purposes of the observer (that is, finding the minimum active transaction), we don't
         * need to keep track of child transactions, because child transactions will always have a begin timestamp
         * that is the same or higher than that of the parent, so children will not contribute anyway. Therefore,
         * we just don't register child transactions.
         */
        if(txn.getParentTxnId()!=Txn.ROOT_TRANSACTION.getTxnId()) return; //nothing to do
        txns.add(txn);
        if(txn.allowsWrites())
            keepAliveScheduler.scheduleKeepAlive(txn);
    }

    /**
     * Indicates that a transaction has been elevated.
     *
     * @param oldTxn the previous transaction
     * @param txn the new transaction
     */
    public void txnElevated(Txn oldTxn,Txn txn){
        if(txn.getParentTxnId()!=Txn.ROOT_TRANSACTION.getTxnId()) return; //nothing to do
        assert oldTxn!=txn: "Cannot elevate a transaction which has not changed!";
        keepAliveScheduler.scheduleKeepAlive(txn);

        /*
         * Concurrency trickery here:
         *
         * By requirement, we must keep the minimum transaction held in memory.
         * However, the minimum transaction may be what we are elevating. It is
         * therefore possible that, were we to remove then add, that a reader
         * could come in, and accidentally see a state where this transaction
         * was not present (which is clearly bad). In order to avoid this,
         * we need an atomic "replace" style operation. Of course, that's not available
         * in the SkipListSet api, so we fudge it using this "add then remove" behavior,
         * which relies on the comparator distinguishing between the old (read-only) transaction,
         * and the new (writable) transaction as distinct elements which do not compare to 0.
         */
        txns.add(txn);
        txns.remove(oldTxn);
    }

    /**
     * Indicates that a transaction has terminated, either committed
     * or rolled back ( we don't make a distinction).
     *
     * @param txn the transaction
     */
    public void txnFinished(Txn txn){
        if(txns.first()==txn){
            lastActiveTxn.set(txn);
        }
        txns.remove(txn);
    }

    /**
     * @return the minimum active transaction which has been registered with this observer.
     */
    public TxnView minimumActiveTransaction(){
        try{
            Txn n = txns.first();
            assert n.getEffectiveState()==Txn.State.ACTIVE: "Programmer error: an inactive transaction is " +
                    "still being observed. This probably means that you haven't committed all your child transactions yet";
            return n;
        }catch(NoSuchElementException ignored){
            return Txn.ROOT_TRANSACTION;
        }
    }

    /**
     * @return either the minimum active transaction, or the last known transaction if no transactions
     * are currently active on this JVM
     */
    public TxnView lastMinimumActiveTransaction(){
        TxnView txn = minimumActiveTransaction();
        if(txn==Txn.ROOT_TRANSACTION) txn= lastActiveTxn.get();
        return txn;
    }
}

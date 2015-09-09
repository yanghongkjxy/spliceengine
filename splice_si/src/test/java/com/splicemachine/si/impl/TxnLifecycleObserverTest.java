package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 9/10/15
 */
public class TxnLifecycleObserverTest{

    @Test
    public void testRegistersReadOnlyTxn() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        ReadOnlyTxn roTxn = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                Txn.ROOT_TRANSACTION,
                UnsupportedLifecycleManager.INSTANCE,
                tco,false);
        tco.txnBegun(roTxn);
        Assert.assertEquals("Does not register read only transactions!",roTxn,tco.minimumActiveTransaction());
        Assert.assertEquals("Does not register read only transactions!",roTxn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testElevateThenCommit() throws Exception{
        final TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        final TxnLifecycleManager tlm = mock(TxnLifecycleManager.class);
        when(tlm.commit(anyLong())).thenReturn(1l);
        final boolean[] called = new boolean[]{false};
        when(tlm.elevateTransaction(any(Txn.class),any(byte[].class)))
                .thenAnswer(new Answer<Txn>(){
                    @Override
                    public Txn answer(InvocationOnMock invocation) throws Throwable{
                        Assert.assertFalse("Already called!",called[0]);
                        called[0] = true;
                        Txn oldTxn = (Txn)invocation.getArguments()[0];
                        Txn txn=new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tlm,tco,false);
                        tco.txnElevated(oldTxn,txn);
                        return txn;
                    }
                });
        ReadOnlyTxn roTxn = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                Txn.ROOT_TRANSACTION,
                tlm,
                tco,false);
        tco.txnBegun(roTxn);
        Assert.assertEquals("Does not register read only transactions!",roTxn,tco.minimumActiveTransaction());

        Txn elevated=roTxn.elevateToWritable("test".getBytes());
        Assert.assertEquals("Does not register read only transactions!",elevated,tco.minimumActiveTransaction());

        elevated.commit();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",elevated,tco.lastMinimumActiveTransaction());
    }

    /* *************************************************************
     * Determinism tests
     *
     * In the course of initial development and testing, we encountered
     * some non-deterministic behavior of the underlying observer instance.
     * Since we are operating on a single thread, that determinism is
     * a pretty good sign of an underlying bug, so we add these tests
     * here to make sure that we retain that determinism
     * *************************************************************/
    @Test
    public void testRepeatedElevateThenCommit() throws Exception{
        for(int i=0;i<100;i++){
            testElevateThenCommit();
        }
    }

    @Test
    public void testRepeatedElevateThenRollback() throws Exception{
        for(int i=0;i<100;i++){
            testElevateThenRollback();
        }
    }

    @Test
    public void testElevateThenRollback() throws Exception{
        final TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        final TxnLifecycleManager tlm = mock(TxnLifecycleManager.class);
        when(tlm.commit(anyLong())).thenReturn(1l);
        final boolean[] called = new boolean[]{false};
        when(tlm.elevateTransaction(any(Txn.class),any(byte[].class)))
                .thenAnswer(new Answer<Txn>(){
                    @Override
                    public Txn answer(InvocationOnMock invocation) throws Throwable{
                        Assert.assertFalse("Already called!",called[0]);
                        called[0] = true;
                        Txn oldTxn = (Txn)invocation.getArguments()[0];
                        Txn txn=new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tlm,tco,false);
                        tco.txnElevated(oldTxn,txn);
                        return txn;
                    }
                });
        ReadOnlyTxn roTxn = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                Txn.ROOT_TRANSACTION,
                tlm,
                tco,false);
        tco.txnBegun(roTxn);
        Assert.assertEquals("Does not register read only transactions!",roTxn,tco.minimumActiveTransaction());

        Txn elevated=roTxn.elevateToWritable("test".getBytes());
        Assert.assertEquals("Does not register read only transactions!",elevated,tco.minimumActiveTransaction());

        elevated.rollback();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",elevated,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testCommittingReadOnlyTxnsDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        ReadOnlyTxn roTxn = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                Txn.ROOT_TRANSACTION,
                UnsupportedLifecycleManager.INSTANCE,
                tco,false);
        tco.txnBegun(roTxn);
        Assert.assertEquals("Does not register read only transactions!",roTxn,tco.minimumActiveTransaction());
        roTxn.commit();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",roTxn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testRollingBackReadOnlyTxnsDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        ReadOnlyTxn roTxn = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                Txn.ROOT_TRANSACTION,
                UnsupportedLifecycleManager.INSTANCE,
                tco,false);
        tco.txnBegun(roTxn);
        Assert.assertEquals("Does not register read only transactions!",roTxn,tco.minimumActiveTransaction());
        roTxn.rollback();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",roTxn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testCommittingReadOnlyChildOfReadOnlyTxnDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        ReadOnlyTxn roTxn = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                Txn.ROOT_TRANSACTION,
                UnsupportedLifecycleManager.INSTANCE,
                tco,false);
        tco.txnBegun(roTxn);

        ReadOnlyTxn child = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                roTxn,
                UnsupportedLifecycleManager.INSTANCE,
                tco,false);
        tco.txnBegun(child);

        Assert.assertEquals("Does not register read only transactions!",roTxn,tco.minimumActiveTransaction());
        child.commit();
        Assert.assertEquals("Committing child breaks registry!",roTxn,tco.minimumActiveTransaction());
        roTxn.commit();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",roTxn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testRollingBackReadOnlyChildOfReadOnlyTxnDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        ReadOnlyTxn roTxn = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                Txn.ROOT_TRANSACTION,
                UnsupportedLifecycleManager.INSTANCE,
                tco,false);
        tco.txnBegun(roTxn);

        ReadOnlyTxn child = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                roTxn,
                UnsupportedLifecycleManager.INSTANCE,
                tco,false);
        tco.txnBegun(child);

        Assert.assertEquals("Does not register read only transactions!",roTxn,tco.minimumActiveTransaction());
        child.rollback();
        Assert.assertEquals("Committing child breaks registry!",roTxn,tco.minimumActiveTransaction());
        roTxn.rollback();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",roTxn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testRegistersWritableTxn() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        Txn txn = new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,UnsupportedLifecycleManager.INSTANCE,tco,false);
        tco.txnBegun(txn);
        Assert.assertEquals("Does not register read only transactions!",txn,tco.minimumActiveTransaction());
        Assert.assertEquals("Does not register read only transactions!",txn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testCommittingWritableTxnDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        TxnLifecycleManager tlm = mock(TxnLifecycleManager.class);
        when(tlm.commit(anyLong())).thenReturn(1l);
        Txn txn = new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tlm,tco,false);
        tco.txnBegun(txn);
        Assert.assertEquals("Does not register read only transactions!",txn,tco.minimumActiveTransaction());
        txn.commit();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",txn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testRollingBackWritableTxnDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        TxnLifecycleManager tlm = mock(TxnLifecycleManager.class);
        doNothing().when(tlm).rollback(anyLong());
        Txn txn = new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tlm,tco,false);
        tco.txnBegun(txn);
        Assert.assertEquals("Does not register read only transactions!",txn,tco.minimumActiveTransaction());
        txn.rollback();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",txn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testCommittingReadOnlyChildOfWritableTxnDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        TxnLifecycleManager tlm = mock(TxnLifecycleManager.class);
        doNothing().when(tlm).rollback(anyLong());
        Txn txn = new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tlm,tco,false);
        tco.txnBegun(txn);

        ReadOnlyTxn child = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                txn,
                UnsupportedLifecycleManager.INSTANCE,
                tco,false);
        tco.txnBegun(child);

        Assert.assertEquals("Does not register read only transactions!",txn,tco.minimumActiveTransaction());
        child.commit();
        Assert.assertEquals("Committing child breaks registry!",txn,tco.minimumActiveTransaction());
        txn.commit();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",txn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testRollingBackReadOnlyChildOfWritableTxnDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        TxnLifecycleManager tlm = mock(TxnLifecycleManager.class);
        doNothing().when(tlm).rollback(anyLong());
        Txn txn = new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tlm,tco,false);
        tco.txnBegun(txn);

        ReadOnlyTxn child = new ReadOnlyTxn(
                1l,1l,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                txn,
                UnsupportedLifecycleManager.INSTANCE,
                tco,false);
        tco.txnBegun(child);

        Assert.assertEquals("Does not register read only transactions!",txn,tco.minimumActiveTransaction());
        child.rollback();
        Assert.assertEquals("Committing child breaks registry!",txn,tco.minimumActiveTransaction());
        txn.rollback();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",txn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testCommittingWritableChildOfWritableTxnDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        TxnLifecycleManager tlm = mock(TxnLifecycleManager.class);
        doNothing().when(tlm).rollback(anyLong());
        Txn txn = new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tlm,tco,false);
        tco.txnBegun(txn);

        Txn child = new WritableTxn(2l,2l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,txn,tlm,tco,false);
        tco.txnBegun(child);

        Assert.assertEquals("Does not register read only transactions!",txn,tco.minimumActiveTransaction());
        child.commit();
        Assert.assertEquals("Committing child breaks registry!",txn,tco.minimumActiveTransaction());
        txn.commit();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",txn,tco.lastMinimumActiveTransaction());
    }

    @Test
    public void testRollingBackWritableOfWritableTxnDeregisters() throws Exception{
        TxnLifecycleObserver tco = new TxnLifecycleObserver(NoopKeepAliveScheduler.INSTANCE);
        TxnLifecycleManager tlm = mock(TxnLifecycleManager.class);
        doNothing().when(tlm).rollback(anyLong());
        Txn txn = new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tlm,tco,false);
        tco.txnBegun(txn);

        Txn child = new WritableTxn(2l,2l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,txn,tlm,tco,false);
        tco.txnBegun(child);

        Assert.assertEquals("Does not register read only transactions!",txn,tco.minimumActiveTransaction());
        child.rollback();
        Assert.assertEquals("Committing child breaks registry!",txn,tco.minimumActiveTransaction());
        txn.rollback();
        Assert.assertEquals("Committing does not clear the transaction!",Txn.ROOT_TRANSACTION,tco.minimumActiveTransaction());
        Assert.assertEquals("Committing clears the last transaction!",txn,tco.lastMinimumActiveTransaction());
    }
}

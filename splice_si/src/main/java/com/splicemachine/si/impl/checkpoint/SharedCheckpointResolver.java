package com.splicemachine.si.impl.checkpoint;

import com.splicemachine.concurrent.Striped;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 11/3/15
 */
@ThreadSafe
public class SharedCheckpointResolver{
    //must be thread-safe Partitions and Checkpointers
    private final ConcurrentMap<Partition,CheckpointResolver> checkpointerMap;
    private final Striped<Lock> lockStriper;
    private final DataStore dataStore;
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier ignoreTxnSupplier;

    public SharedCheckpointResolver(DataStore dataStore,
                                    TxnSupplier txnSupplier,
                                    IgnoreTxnCacheSupplier ignoreTxnSupplier,
                                    int concurrencyLevel){
        this.dataStore=dataStore;
        this.txnSupplier=txnSupplier;
        this.lockStriper = Striped.lock(concurrencyLevel);
        this.checkpointerMap = new ConcurrentHashMap<>(concurrencyLevel);
        this.ignoreTxnSupplier = ignoreTxnSupplier;
    }

    public void addResolver(Partition p){
        if(checkpointerMap.get(p)!=null) return;

        CheckpointResolver resolver = newResolver(p);
        this.checkpointerMap.putIfAbsent(p,resolver);
    }

    public void checkpoint(Partition partition,byte[] rowKeyArray,int offset,int length,long checkpointTimestamp) throws IOException{
        if(partition.isClosed()){
            checkpointerMap.remove(partition);
            return;
        }

        CheckpointResolver resolver=getResolver(partition);
        resolver.resolveCheckpoint(rowKeyArray,offset,length,checkpointTimestamp);
    }

    public void checkpoint(Partition partition,Checkpoint checkpoint) throws IOException{
        if(partition.isClosed()){
            checkpointerMap.remove(partition);
            return;
        }

        CheckpointResolver resolver=getResolver(partition);
        resolver.resolveCheckpoint(checkpoint);
    }


    public void batchCheckpoint(Partition partition, Checkpoint...checkpoints) throws IOException{
        if(partition.isClosed()){
            checkpointerMap.remove(partition);
            return;
        }

        CheckpointResolver resolver=getResolver(partition);
        resolver.resolveCheckpoint(checkpoints);
    }

    public void removeResolver(Partition partition){
        checkpointerMap.remove(partition);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private CheckpointResolver getResolver(Partition partition){
        CheckpointResolver checkpointResolver=checkpointerMap.get(partition);
        if(checkpointResolver==null){
            checkpointResolver = newResolver(partition);
            CheckpointResolver oldResolver=checkpointerMap.putIfAbsent(partition,checkpointResolver);
            if(oldResolver!=checkpointResolver)
                checkpointResolver = oldResolver;
        }
        return checkpointResolver;
    }

    private CheckpointResolver newResolver(Partition p){
        Checkpointer cp = new SimpleCheckpointer(p,false);
        Partition lockedP = new StripeLockedPartition(p);
        return new PartitionCheckpointResolver(lockedP,
                3*SIConstants.scannerBatchSize/4,
                cp,
                dataStore,
                txnSupplier,
                newFilterFactory(lockedP));
    }

    private TxnFilterFactory newFilterFactory(final Partition lockedP){
        return new TxnFilterFactory(){
            @Override
            public TxnFilter<Cell> unpackedFilter(TxnView txn) throws IOException{
                return new SimpleTxnFilter<>(lockedP.getTableName(),txnSupplier,ignoreTxnSupplier,txn,NoOpReadResolver.INSTANCE,dataStore);
            }

            @Override
            public TxnFilter<Cell> noOpFilter(TxnView txn) throws IOException{
                return new NoopTxnFilter(dataStore,txnSupplier);
            }

            @Override
            public TxnFilter<Cell> packedFilter(TxnView txn,EntryPredicateFilter predicateFilter,boolean countStar) throws IOException{
                HRowAccumulator accumulator=new HRowAccumulator(dataStore.getDataLib(),predicateFilter,new EntryDecoder(),countStar);
                return new PackedTxnFilter<>(unpackedFilter(txn),accumulator);
            }

            @Override
            public DDLFilter ddlFilter(Txn ddlTxn) throws IOException{
                throw new UnsupportedOperationException("Should not use a DDLFilter in Checkpoint resolving!");
            }
        };
    }

    private class StripeLockedPartition implements Partition{
        private Partition delegate;

        public StripeLockedPartition(Partition delegate){
            this.delegate=delegate;
        }

        @Override
        public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException{
            return delegate.batchMutate(mutations);
        }

        @Override
        public void mutate(Mutation mutation) throws IOException{
            delegate.mutate(mutation);
        }

        @Override
        public Lock lock(byte[] rowKey) throws IOException{
            return lockStriper.get(ByteBuffer.wrap(rowKey));
        }

        @Override
        public List<Cell> get(Get get) throws IOException{
            return delegate.get(get);
        }

        @Override
        public boolean rowInRange(byte[] row,int offset,int length){
            return delegate.rowInRange(row,offset,length);
        }

        @Override
        public boolean rowInRange(ByteSlice slice){
            return delegate.rowInRange(slice);
        }

        @Override
        public boolean isClosed(){
            return delegate.isClosed();
        }

        @Override
        public boolean containsRange(byte[] start,byte[] stop){
            return delegate.containsRange(start,stop);
        }

        @Override
        public String getTableName(){
            return delegate.getTableName();
        }

        @Override
        public void markWrites(long numWrites){
            delegate.markWrites(numWrites);
        }

        @Override
        public void markReads(long numReads){
            delegate.markReads(numReads);
        }
    }
}

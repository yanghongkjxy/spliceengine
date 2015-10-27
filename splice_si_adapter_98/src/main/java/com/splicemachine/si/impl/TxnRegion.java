package com.splicemachine.si.impl;

import com.google.common.annotations.VisibleForTesting;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.data.api.IHTable;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.impl.checkpoint.CheckpointResolver;
import com.splicemachine.si.impl.checkpoint.NoOpCheckpointResolver;
import com.splicemachine.si.impl.compaction.CheckpointCompactionScanner;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Region;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.io.IOException;
import java.util.Collection;

/**
 * Base implementation of a TransactionalRegion
 *
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class TxnRegion implements TransactionalRegion{
    private final HRegion region;
    private final Partition partition;
    private final RollForward rollForward;
    private final ReadResolver readResolver;
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier ignoreTxnCacheSupplier;
    private final DataStore dataStore;
    private final Transactor transactor;
    private final IHTable hbRegion;
    private final String tableName;
    private final CheckpointResolver checkpointResolver;

    private final boolean transactionalWrites; //if false, then will use straightforward writes

    @VisibleForTesting
    public TxnRegion(HRegion region,
                     RollForward rollForward,
                     ReadResolver readResolver,
                     TxnSupplier txnSupplier,
                     IgnoreTxnCacheSupplier ignoreTxnCacheSupplier,
                     DataStore dataStore,
                     Transactor transactor){
        this(region, rollForward, readResolver, txnSupplier, ignoreTxnCacheSupplier, dataStore, transactor,NoOpCheckpointResolver.INSTANCE);

    }
    public TxnRegion(HRegion region,
                     RollForward rollForward,
                     ReadResolver readResolver,
                     TxnSupplier txnSupplier,
                     IgnoreTxnCacheSupplier ignoreTxnCacheSupplier,
                     DataStore dataStore,
                     Transactor transactor,
                     CheckpointResolver checkpointResolver){
        this.region=region;
        this.rollForward=rollForward;
        this.readResolver=readResolver;
        this.txnSupplier=txnSupplier;
        this.ignoreTxnCacheSupplier=ignoreTxnCacheSupplier;
        this.dataStore=dataStore;
        this.transactor=transactor;
        this.hbRegion=new HbRegion(region);
        this.tableName=region.getTableDesc().getNameAsString();
        this.transactionalWrites=SIObserver.doesTableNeedSI(region.getTableDesc().getNameAsString());
        this.checkpointResolver = checkpointResolver;
        this.partition = new Region(region);
    }

    @Override
    public TxnFilter unpackedFilter(TxnView txn) throws IOException{
        return new SimpleTxnFilter(tableName,txnSupplier,ignoreTxnCacheSupplier,txn,readResolver,dataStore);
    }

    @Override
    public TxnFilter<Cell> noOpFilter(TxnView txn) throws IOException{
        return new NoopTxnFilter(dataStore,txnSupplier);
    }

    @Override
    public TxnFilter packedFilter(TxnView txn,EntryPredicateFilter predicateFilter,boolean countStar) throws IOException{
        return new PackedTxnFilter(unpackedFilter(txn),new HRowAccumulator(dataStore.getDataLib(),predicateFilter,new EntryDecoder(),countStar));
    }

    @Override
    public DDLFilter ddlFilter(Txn ddlTxn) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean rowInRange(byte[] row){
        return partition.rowInRange(row,0,row.length);
    }

    @Override
    public boolean rowInRange(ByteSlice slice){
        return partition.rowInRange(slice);
    }

    @Override
    public boolean isClosed(){
        return partition.isClosed();
    }

    @Override
    public boolean containsRange(byte[] start,byte[] stop){
        return partition.containsRange(start,stop);
    }

    @Override
    public String getTableName(){
        return partition.getTableName();
    }

    @Override
    public void updateWriteRequests(long writeRequests){
        partition.markWrites(writeRequests);
    }

    @Override
    public void updateReadRequests(long readRequests){
        partition.markReads(readRequests);
    }

    @Override
    @SuppressWarnings("unchecked")
    public OperationStatus[] bulkWrite(TxnView txn,
                                       byte[] family,byte[] qualifier,
                                       ConstraintChecker constraintChecker, //TODO -sf- can we encapsulate this as well?
                                       Collection<KVPair> data) throws IOException{
        if(transactionalWrites)
            return transactor.processKvBatch(hbRegion,rollForward,txn,family,qualifier,data,constraintChecker);
        else
            return hbRegion.batchMutate(data,txn);
    }

    @Override
    public boolean verifyForeignKeyReferenceExists(TxnView txnView,byte[] rowKey) throws IOException{
        SRowLock rowLock=null;
        try{
            rowLock=hbRegion.getLock(rowKey,true);
            Get get=TransactionOperations.getOperationFactory().newGet(txnView,rowKey);
            get.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
            if(!hbRegion.get(get).isEmpty()){
                // Referenced row DOES exist, update counter.
                transactor.updateCounterColumn(hbRegion,txnView,rowLock,rowKey);
                return true;
            }
        }finally{
            hbRegion.unLockRow(rowLock);
        }
        return false;
    }

    @Override
    public String getRegionName(){
        return region.getRegionNameAsString();
    }

    @Override
    public TxnSupplier getTxnSupplier(){
        return txnSupplier;
    }

    @Override
    public ReadResolver getReadResolver(){
        return readResolver;
    }

    @Override
    public DataStore getDataStore(){
        return dataStore;
    }

    @Override
    public void close(){
    } //no-op

    @Override
    public InternalScanner compactionScanner(InternalScanner scanner, CompactionRequest compactionRequest){
        long mat;
        MinimumTransactionWatcher matWatcher=TransactionLifecycle.getMatWatcher();
        if(matWatcher==null) mat = 0; //don't remove anything
        else
            mat=matWatcher.minimumActiveTimestamp(false);

        ActiveTxnCacheSupplier store=new ActiveTxnCacheSupplier(txnSupplier,SIConstants.activeTransactionCacheSize);

        CheckpointCompactionScanner ccs;
        CellTypeParser ctParser=dataStore.cellTypeParser();
        if(compactionRequest.isMajor()){
            /*
             * We can perform accumulations and remove tombstoned records, because we know that there are no
             * missing intermediate versions in a Major compaction.
             */
            ccs = CheckpointCompactionScanner.majorScanner(store,dataStore.getDataLib(),ctParser,scanner,rollForward,mat);
        }else{
            mat=0;
            ccs=CheckpointCompactionScanner.minorScanner(store,ctParser,scanner,rollForward,mat);
        }
        return ccs;
    }

    @Override
    public CheckpointResolver getCheckpointResolver(){
        return checkpointResolver;
    }

    @Override
    public Partition unwrapPartition(){
        return partition;
    }

    @Override
    public RollForward getRollForward(){
        return rollForward;
    }

    @Override
    public void pauseMaintenance(){
        readResolver.pauseResolution();
        checkpointResolver.pauseCheckpointing();
        rollForward.pauseRollForward();
    }

    @Override
    public void resumeMaintenance(){
        readResolver.resumeResolution();
        checkpointResolver.resumeCheckpointing();
        rollForward.resumeRollForward();
    }
}

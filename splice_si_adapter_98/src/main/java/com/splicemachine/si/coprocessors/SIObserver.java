package com.splicemachine.si.coprocessors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.ConstraintChecker;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.BaseSIFilterPacked;
import com.splicemachine.si.impl.CheckpointFilter;
import com.splicemachine.si.impl.SIFilterPacked;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.si.impl.compaction.CheckpointCompactionScanner;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public class SIObserver extends SIBaseObserver{


    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,Put put,WALEdit edit,Durability writeToWAL) throws IOException{
            /*
				 * This is relatively expensive--it's better to use the write pipeline when you need to load a lot of rows.
				 */
        if(!tableEnvMatch || put.getAttribute(SIConstants.SI_NEEDED)==null){
            super.prePut(e,put,edit,writeToWAL);
            return;
        }
        TxnView txn=txnOperationFactory.fromWrites(put);
        boolean isDelete=put.getAttribute(SIConstants.SI_DELETE_PUT)!=null;
        byte[] row=put.getRow();
        boolean isSIDataOnly=true;
        //convert the put into a collection of KVPairs
        Map<byte[], Map<byte[], KVPair>> familyMap=Maps.newHashMap();
        Iterable<KeyValue> keyValues=Iterables.concat(put.getFamilyMap().values());
        for(KeyValue kv : keyValues){
            byte[] family=kv.getFamily();
            byte[] column=kv.getQualifier();
            if(!Bytes.equals(column,SIConstants.PACKED_COLUMN_BYTES)) continue; //skip SI columns

            isSIDataOnly=false;
            byte[] value=kv.getValue();
            Map<byte[], KVPair> columnMap=familyMap.get(family);
            if(columnMap==null){
                columnMap=Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                familyMap.put(family,columnMap);
            }
            columnMap.put(column,new KVPair(row,value,isDelete?KVPair.Type.DELETE:KVPair.Type.UPSERT));
        }
        if(isSIDataOnly){
            byte[] family=SpliceConstants.DEFAULT_FAMILY_BYTES;
            byte[] column=SpliceConstants.PACKED_COLUMN_BYTES;
            byte[] value=HConstants.EMPTY_BYTE_ARRAY;
            Map<byte[], KVPair> columnMap=familyMap.get(family);
            if(columnMap==null){
                columnMap=Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                familyMap.put(family,columnMap);
            }
            columnMap.put(column,new KVPair(row,value,isDelete?KVPair.Type.DELETE:KVPair.Type.EMPTY_COLUMN));
        }
        boolean processed=false;
        for(Map.Entry<byte[], Map<byte[], KVPair>> family : familyMap.entrySet()){
            byte[] fam=family.getKey();
            Map<byte[], KVPair> cols=family.getValue();
            for(Map.Entry<byte[], KVPair> column : cols.entrySet()){
                OperationStatus[] status=region.bulkWrite(txn,fam,column.getKey(),ConstraintChecker.NO_CONSTRAINT,Collections.singleton(column.getValue()));
                switch(status[0].getOperationStatusCode()){
                    case NOT_RUN:
                        break;
                    case BAD_FAMILY:
                        throw new NoSuchColumnFamilyException(status[0].getExceptionMsg());
                    case SANITY_CHECK_FAILURE:
                        throw new IOException("Sanity Check failure:"+status[0].getExceptionMsg());
                    case FAILURE:
                        throw new IOException(status[0].getExceptionMsg());
                    default:
                        processed=true;
                }
            }
        }

//						final boolean processed = transactor.processPut(new HbRegion(e.getEnvironment().getRegion()), rollForward, put);
        if(processed){
            e.bypass();
            e.complete();
        }
    }


    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,Delete delete,WALEdit edit,
                          Durability writeToWAL) throws IOException{
        if(tableEnvMatch){
            if(delete.getAttribute(SUPPRESS_INDEXING_ATTRIBUTE_NAME)==null){
                TableName tableName=e.getEnvironment().getRegion().getTableDesc().getTableName();
                String message="Direct deletes are not supported under snapshot isolation. "+
                        "Instead a Put is expected that will set a record level tombstone. tableName="+tableName;
                throw new RuntimeException(message);
            }
        }
        super.preDelete(e,delete,edit,writeToWAL);
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c,Store store,List<StoreFile> candidates) throws IOException{
        super.preCompactSelection(c,store,candidates);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,
                                      InternalScanner scanner,ScanType scanType,CompactionRequest compactionRequest) throws IOException{
        if(tableEnvMatch){
            /*
             * We cannot perform transactional maintenance while Compactions are ongoing, because it can cause
             * nasty race conditions like the following:
             *
             * 1. add tombstone cell for Read resolution
             * 2. compaction
             * 3. Since the tombstone occurs below the MinimumActiveTransaction, compaction is able to delete the row
             * 4. Read off the queue, and add a commit timestamp cell.
             *
             * The end result is a commit timestamp cell with no associated user or tombstone cell, which is technically
             * a corrupted store. In many cases our read pipeline can handle this particular situation, but there's no
             * guarantee that that will always be the case, and this is not necessarily the only race condition of its
             * type--you could also end up with phantom Checkpoint cells due to checkpointing, for example. Thus,
             * in order to avoid this, we have to "pause" all region-level maintenance while compactions run,
             * and re-enable them afterwards. In most cases this doesn't make much difference, since compaction
             * will perform the same operation as all our background processors anyway.
             */
            region.pauseMaintenance();
            return region.compactionScanner(scanner,compactionRequest);
        }else{
            return super.preCompact(e,store,scanner,scanType,compactionRequest);
        }
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile){
        if(tableEnvMatch){
            region.resumeMaintenance();
        }
        super.postCompact(e,store,resultFile);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile){
        if(tableEnvMatch){
            region.resumeMaintenance();
        }
    }

    protected Filter makeSIFilter(TxnView txn,
                                  Filter currentFilter,
                                  EntryPredicateFilter predicateFilter,
                                  boolean countStar,
                                  boolean pack,
                                  int scannerBatchSize) throws IOException{

        TxnFilter<Cell> txnFilter;
        if(pack){
            txnFilter=region.packedFilter(txn,predicateFilter,countStar);
        }else{
            txnFilter = region.unpackedFilter(txn);
        }
        CheckpointFilter siFilter = new CheckpointFilter(txnFilter,SIConstants.checkpointSeekThreshold,region.getCheckpointResolver(),3*scannerBatchSize/4);
        if(needsCompositeFilter(currentFilter)){
            return composeFilters(orderFilters(currentFilter,siFilter));
        }else{
            return siFilter;
        }
    }
}

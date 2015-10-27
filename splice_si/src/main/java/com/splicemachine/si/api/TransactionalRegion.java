package com.splicemachine.si.api;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.checkpoint.CheckpointResolver;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a "Transactional Region", that is, a region in Hbase which is transactionally aware.
 *
 * @author Scott Fines
 *         Date: 7/1/14
 */
public interface TransactionalRegion extends AutoCloseable,TxnFilterFactory{

    /**
     * @return true if the underlying region is either closed or is closing
     */
    boolean isClosed();

    boolean rowInRange(byte[] row);

    boolean rowInRange(ByteSlice slice);

    boolean containsRange(byte[] start, byte[] stop);

    String getTableName();

    void updateWriteRequests(long writeRequests);

    void updateReadRequests(long readRequests);

    OperationStatus[] bulkWrite(TxnView txn,
                                byte[] family, byte[] qualifier,
                                ConstraintChecker constraintChecker,
                                Collection<KVPair> data) throws IOException;

    /**
     * Check for rowKey existence and update the FK row counter with given transaction's ID while holding
     * the row lock. Throw WriteConflict if referenced row has been concurrently deleted.
     *
     * @return true if the row exists.
     */
    boolean verifyForeignKeyReferenceExists(TxnView txnView, byte[] rowKey) throws IOException;

    String getRegionName();

    TxnSupplier getTxnSupplier();

    ReadResolver getReadResolver();

    DataStore getDataStore();

    void close();

    InternalScanner compactionScanner(InternalScanner scanner,CompactionRequest compactionRequest);

    CheckpointResolver getCheckpointResolver();

    Partition unwrapPartition();

    RollForward getRollForward();

    /**
     * Pause transaction and storage maintenance for this region. This involves stopping all background maintenance
     * tasks which may affect the underlying storage model (i.e. ReadResolution or Checkpointing); this avoids certain
     * race conditions between competing processes (for example, Checkpointing and ReadResolution) which may result
     * in corrupted data in the physical storage layer. if no such race condition is possible (e.g. if there is
     * no ongoing maintenance systems running) then this method will do nothing.
     *
     * Multiple calls to this without calling {@link #resumeMaintenance()} will have no functional effect. Once paused,
     * maintenance will remain paused until it is resumed again.
     */
    void pauseMaintenance();

    /**
     * Resume transaction and storage maintenance for this region. This allows background maintenance tasks to begin
     * operating again (in whatever manner makes sense for the particular maintenance operation).
     *
     * Multiple calls to this without calling {@link #pauseMaintenance()} will have no functional effect. Once resumed,
     * maintenance will continue operating until paused again.
     */
    void resumeMaintenance();
}

package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.procedures.LongProcedure;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.splicemachine.constants.SpliceConstants.*;

/**
 * Library of functions used by the SI module when accessing rows from data tables (data tables as opposed to the
 * transaction table).
 */
public class DataStore<Data, Mutation, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes, Scan, IHTable> {
    //    private static final Logger LOG = Logger.getLogger(DataStore.class);

    public final SDataLib<Data, Put, Delete, Get, Scan> dataLib;
    private final STableReader<IHTable, Get, Scan> reader;
    private final STableWriter<IHTable, Mutation, Put, Delete> writer;
    private final String siNeededAttribute;
    private final String deletePutAttribute;
    private final List<List<byte[]>> userFamilyColumnList;

    private final byte[] commitTimestampQualifier;
    private final byte[] tombstoneQualifier;
    private final byte[] siNull;
    private final byte[] siAntiTombstoneValue;
    private final byte[] siFail;
    private final byte[] userColumnFamily;

    private final CellTypeParser ctParser;
    public DataStore(SDataLib<Data, Put, Delete, Get, Scan> dataLib,
                     STableReader reader,
                     STableWriter writer,
                     String siNeededAttribute,
                     String deletePutAttribute,
                     byte[] siCommitQualifier,
                     byte[] siTombstoneQualifier,
                     byte[] siNull,
                     byte[] siAntiTombstoneValue,
                     byte[] siFail,
                     byte[] userColumnFamily,
                     CellTypeParser ctParser ) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
        this.ctParser = ctParser;
        this.siNeededAttribute = siNeededAttribute;
        this.deletePutAttribute = deletePutAttribute;
        this.commitTimestampQualifier = siCommitQualifier;
        this.tombstoneQualifier = siTombstoneQualifier;
        this.siNull = siNull;
        this.siAntiTombstoneValue = siAntiTombstoneValue;
        this.siFail = siFail;
        this.userColumnFamily = userColumnFamily;
        this.userFamilyColumnList = Arrays.asList(
                Arrays.asList(this.userColumnFamily, tombstoneQualifier),
                Arrays.asList(this.userColumnFamily, commitTimestampQualifier),
                Arrays.asList(this.userColumnFamily, FixedSpliceConstants.PACKED_COLUMN_BYTES),
                Arrays.asList(this.userColumnFamily, FixedSIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)
        );
    }

    public byte[] getSINeededAttribute(OperationWithAttributes operation) {
        return operation.getAttribute(siNeededAttribute);
    }

    public Boolean getDeletePutAttribute(OperationWithAttributes operation) {
        byte[] neededValue = operation.getAttribute(deletePutAttribute);
        if (neededValue == null) return false;
        return dataLib.decode(neededValue,Boolean.class);
    }

    public Delete copyPutToDelete(final Put put, LongOpenHashSet transactionIdsToDelete) {
        final Delete delete = dataLib.newDelete(dataLib.getPutKey(put));
        final Iterable<Data> cells = dataLib.listPut(put);
        transactionIdsToDelete.forEach(new LongProcedure() {
            @Override
            public void apply(long transactionId) {
                for (Data data : cells) {
                    dataLib.addDataToDelete(delete, data, transactionId);
                }
                dataLib.addFamilyQualifierToDelete(delete, userColumnFamily, tombstoneQualifier, transactionId);
                dataLib.addFamilyQualifierToDelete(delete, userColumnFamily, commitTimestampQualifier, transactionId);

            }
        });
        return delete;
    }

    public Result getCommitTimestampsAndTombstonesSingle(IHTable table, byte[] rowKey) throws IOException {
        Get get = dataLib.newGet(rowKey, null, userFamilyColumnList, null, 1); // Just Retrieve one per...
        suppressIndexing(get);
        checkBloom(get);
        return reader.get(table,get);
    }

    public void checkBloom(OperationWithAttributes operation) {
        operation.setAttribute(CHECK_BLOOM_ATTRIBUTE_NAME, userColumnFamily);
    }

    public CellTypeParser cellTypeParser(){
        return ctParser;
    }

    boolean isAntiTombstone(Data keyValue) {
        return dataLib.isAntiTombstone(keyValue,siAntiTombstoneValue);
    }

    public CellType getKeyValueType(Data keyValue) {
        return ctParser.parseCellType((Cell)keyValue);
    }

    public boolean isSINull(Data keyValue) {
        return dataLib.matchingValue(keyValue,siNull);
    }

    public boolean isSIFail(Data keyValue) {
        return dataLib.matchingValue(keyValue, siFail);
    }

    /**
     * When this new operation goes through the co-processor stack it should not be indexed (because it already has been
     * when the original operation went through).
     */
    public void suppressIndexing(OperationWithAttributes operation) {
        operation.setAttribute(SUPPRESS_INDEXING_ATTRIBUTE_NAME, SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
    }

    public boolean isSuppressIndexing(OperationWithAttributes operation) {
        return operation.getAttribute(SUPPRESS_INDEXING_ATTRIBUTE_NAME) != null;
    }

    public void setTombstoneOnPut(Put put, long transactionId) {
        dataLib.addKeyValueToPut(put, userColumnFamily, tombstoneQualifier, transactionId, siNull);
    }

    public void setTombstonesOnColumns(IHTable table, long timestamp, Put put) throws IOException {
        final Map<byte[], byte[]> userData = getUserData(table, dataLib.getPutKey(put));
        if (userData != null) {
            for (byte[] qualifier : userData.keySet()) {
                dataLib.addKeyValueToPut(put, userColumnFamily, qualifier, timestamp, siNull);
            }
        }
    }

    public void setAntiTombstoneOnPut(Put put, long transactionId) throws IOException {
        //if (LOG.isTraceEnabled()) LOG.trace(String.format("Flipping on anti-tombstone column: put = %s, txnId = %s, stackTrace = %s", put, transactionId, Arrays.toString(Thread.currentThread().getStackTrace()).replaceAll(", ", "\n\t")));
        dataLib.addKeyValueToPut(put, userColumnFamily, tombstoneQualifier, transactionId, siAntiTombstoneValue);
    }

    private Map<byte[], byte[]> getUserData(IHTable table, byte[] rowKey) throws IOException {
        final List<byte[]> families = Arrays.asList(userColumnFamily);
        Get get = dataLib.newGet(rowKey, families, null, null);
        dataLib.setGetMaxVersions(get, 1);
        Result result = reader.get(table, get);
        if (result != null) {
            return result.getFamilyMap(userColumnFamily);
        }
        return null;
    }

    public OperationStatus[] writeBatch(IHTable table, Pair<Mutation, SRowLock>[] mutationsAndLocks) throws IOException {
        return writer.writeBatch(table, mutationsAndLocks);
    }

    public String getTableName(IHTable table) {
        return reader.getTableName(table);
    }

    public SDataLib<Data, Put, Delete, Get, Scan> getDataLib() {
        return this.dataLib;
    }
}

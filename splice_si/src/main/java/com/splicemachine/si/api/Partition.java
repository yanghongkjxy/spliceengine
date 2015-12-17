package com.splicemachine.si.api;

import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 10/6/15
 */
public interface Partition{

    OperationStatus[] batchMutate(Mutation[] mutations) throws IOException;

    void mutate(Mutation mutation) throws IOException;

    Lock lock(byte[] rowKey) throws IOException;

    Collection<Cell> get(Get get) throws IOException;

    boolean rowInRange(byte[] row,int offset,int length);

    boolean rowInRange(ByteSlice slice);

    boolean isClosed();

    boolean containsRange(byte[] start,byte[] stop);

    String getTableName();

    void markWrites(long numWrites);

    void markReads(long numReads);
}

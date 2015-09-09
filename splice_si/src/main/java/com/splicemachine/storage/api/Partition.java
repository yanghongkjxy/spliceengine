package com.splicemachine.storage.api;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 10/6/15
 */
public interface Partition{


    OperationStatus[] batchMutate(Mutation[] mutations) throws IOException;

    void mutate(Mutation mutation) throws IOException;
}

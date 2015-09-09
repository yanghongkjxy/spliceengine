package com.splicemachine.storage.impl;

import com.splicemachine.storage.api.Partition;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 10/6/15
 */
public class Region implements Partition{
    private final HRegion region;

    public Region(HRegion region){
        this.region=region;
    }

    @Override
    public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException{
        return region.batchMutate(mutations);
    }

    @Override
    public void mutate(Mutation mutation) throws IOException{
        if(mutation instanceof Put)
            region.put((Put)mutation);
        else if(mutation instanceof Delete)
            region.delete((Delete)mutation);
    }
}

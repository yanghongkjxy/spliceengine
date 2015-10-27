package com.splicemachine.si.api;

import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * @author Scott Fines
 *         Date: 11/3/15
 */
public interface PartitionFactory{

    Partition get(HRegion region);
}

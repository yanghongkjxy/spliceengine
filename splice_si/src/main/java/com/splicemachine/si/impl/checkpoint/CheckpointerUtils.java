package com.splicemachine.si.impl.checkpoint;

import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Scott Fines
 *         Date: 11/3/15
 */
public class CheckpointerUtils{

    private CheckpointerUtils(){} //don't create a utility class

    public static Delete checkpointDelete(ByteSlice key, long timestamp){
       return checkpointDelete(key.array(),key.offset(),key.length(),timestamp);
    }

    public static Delete checkpointDelete(byte[] key, int keyOff,int keyLen, long timestamp){
        Delete d = new Delete(key,keyOff,keyLen);
        d=d.deleteColumns(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,timestamp)
                .deleteColumns(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,timestamp)
                .deleteColumns(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSpliceConstants.PACKED_COLUMN_BYTES,timestamp);
        d.setAttribute(FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        d.setDurability(Durability.SKIP_WAL);
        return d;
    }

    public static Put checkpointPut(ByteSlice key, long timestamp, byte[] value,long commitTimestamp){
        return checkpointPut(key.array(),key.offset(),key.length(),timestamp,value,commitTimestamp);
    }
    public static Put checkpointPut(byte[] key, int keyOff,int keyLen, long timestamp, byte[] value,long commitTimestamp){
        Put p = new Put(key,keyOff,keyLen);
        p.setAttribute(FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,FixedSpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        p.setDurability(Durability.SKIP_WAL);

        //add the data field
        p.add(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSpliceConstants.PACKED_COLUMN_BYTES,timestamp,value);

        byte[] checkpointCellValue;
        if(commitTimestamp>0){
            checkpointCellValue = Bytes.toBytes(commitTimestamp);
        }else{
            checkpointCellValue = FixedSpliceConstants.EMPTY_BYTE_ARRAY;
        }

        //add the checkpoint field
        p.add(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,timestamp,checkpointCellValue);
        return p;
    }
}

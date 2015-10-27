package com.splicemachine.si.impl.checkpoint;

import com.splicemachine.si.api.Checkpointer;
import com.splicemachine.si.api.Partition;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/3/15
 */
@ThreadSafe
public class SimpleCheckpointer implements Checkpointer{
    private final Partition partition;
    private final boolean issueDeletes;

    public SimpleCheckpointer(Partition p, boolean issueDeletes){
        this.partition=p;
        this.issueDeletes=issueDeletes;
    }

    @Override
    public void checkpoint(byte[] rowKey,int keyOff,int keyLen,byte[] checkpointValue,long timestamp,long commitTimestamp) throws IOException{
        if(issueDeletes){
            checkpointWithDelete(rowKey,keyOff,keyLen,checkpointValue,timestamp,commitTimestamp);
        }else
            checkpointWithoutDelete(rowKey,keyOff,keyLen,checkpointValue,timestamp,commitTimestamp);

    }

    @Override
    public void checkpoint(ByteSlice rowKey,byte[] checkpointValue,long timestamp,long commitTimestamp) throws IOException{
        if(issueDeletes){
            checkpointWithDelete(rowKey,checkpointValue,timestamp,commitTimestamp);
        }else
            checkpointWithoutDelete(rowKey,checkpointValue,timestamp,commitTimestamp);
    }

    @Override public void flush() throws IOException{ }
    @Override public boolean issuesDeletes(){ return issueDeletes; }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void checkpointWithDelete(byte[] rowKey, int keyOff,int keyLen,byte[] checkpointValue,long timestamp,long commitTimestamp) throws IOException{
        Delete d = checkpointDelete(rowKey,keyOff,keyLen,timestamp);
        Put p = checkpointPut(rowKey,keyOff,keyLen,checkpointValue,timestamp,commitTimestamp);

        Mutation[] mutations = new Mutation[]{p,d};

        partition.batchMutate(mutations);
    }

    private void checkpointWithoutDelete(byte[] rowKey,int rowoff,int rowLen,byte[] checkpointValue,long timestamp,long commitTimestamp) throws IOException{
        Put p = checkpointPut(rowKey,rowoff,rowLen,checkpointValue,timestamp,commitTimestamp);
        partition.mutate(p);
    }

    private void checkpointWithoutDelete(ByteSlice rowKey,byte[] checkpointValue,long timestamp,long commitTimestamp) throws IOException{
        Put p = checkpointPut(rowKey.array(),rowKey.offset(),rowKey.length(),checkpointValue,timestamp,commitTimestamp);
        partition.mutate(p);
    }
    private void checkpointWithDelete(ByteSlice rowKey,byte[] checkpointValue,long timestamp,long commitTimestamp) throws IOException{
        Delete d = checkpointDelete(rowKey.array(),rowKey.offset(),rowKey.length(),timestamp);
        Put p = checkpointPut(rowKey.array(),rowKey.offset(),rowKey.length(),checkpointValue,timestamp,commitTimestamp);

        Mutation[] mutations = new Mutation[]{p,d};

        partition.batchMutate(mutations);
    }


    private Put checkpointPut(byte[] rowKey, int rowOff,int rowLen,byte[] checkpointValue,long timestamp,long commitTimestamp){
        return CheckpointerUtils.checkpointPut(rowKey,rowOff,rowLen,timestamp,checkpointValue,commitTimestamp);
    }

    private Delete checkpointDelete(byte[] rowKey, int rowOff,int rowLen,long timestamp){
        return CheckpointerUtils.checkpointDelete(rowKey,rowOff,rowLen,timestamp-1);
    }
}

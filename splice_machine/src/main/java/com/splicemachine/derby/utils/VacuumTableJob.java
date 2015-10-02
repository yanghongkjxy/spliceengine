package com.splicemachine.derby.utils;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.VacuumTask;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.ActiveReadTxn;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 10/2/15
 */
public class VacuumTableJob implements CoprocessorJob,Externalizable{

    private long conglomerateId;
    private long minimumActiveTxn;
    private transient HTableInterface table;
    private String jobId="Vacuum-"+conglomerateId+SpliceDriver.driver().getUUIDGenerator().nextUUID();

    public VacuumTableJob(){ }

    public VacuumTableJob(HTableInterface table,long conglomerateId,long minimumActiveTxn){
        this.conglomerateId=conglomerateId;
        this.minimumActiveTxn=minimumActiveTxn;
        this.table = table;
    }


    @Override
    public String getJobId(){
        return jobId;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception{
        return Collections.singletonMap(new VacuumTask(getJobId(),minimumActiveTxn),Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
    }

    @Override
    public HTableInterface getTable(){
        return table;
    }

    @Override
    public byte[] getDestinationTable(){
        return Long.toString(conglomerateId).getBytes();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeLong(conglomerateId);
        out.writeLong(minimumActiveTxn);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        this.conglomerateId = in.readLong();
        this.minimumActiveTxn = in.readLong();
    }

    @Override
    public TxnView getTxn(){
        return new ActiveReadTxn(minimumActiveTxn);
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask,byte[] taskStartKey,byte[] taskEndKey) throws IOException{
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }
}

package com.splicemachine.storage;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.api.Partition;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 10/6/15
 */
public class InMemoryPartition implements Partition{
    private final SortedSet<Cell> data = new TreeSet<>(new KeyValue.KVComparator());
    private final Clock clock;

    public InMemoryPartition(Clock clock){
        this.clock=clock;
    }

    @Override
    public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException{
        OperationStatus[] status = new OperationStatus[mutations.length];
        for(int i=0;i<mutations.length;i++){
            status[i] = doMutate(mutations[i]);
        }
        return status;
    }

    @Override
    public void mutate(Mutation mutation) throws IOException{
        doMutate(mutation);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private OperationStatus doMutate(Mutation mutation){
        if(mutation instanceof Put)
            return doPut((Put)mutation);
        else if(mutation instanceof Delete)
            return doDelete((Delete)mutation);
        else
            throw new IllegalArgumentException("Unknown mutation type: "+ mutation);
    }

    private OperationStatus doDelete(Delete delete){
        NavigableMap<byte[], List<Cell>> familyCellMap=delete.getFamilyCellMap();
        for(List<Cell> data:familyCellMap.values()){
            for(Cell cell:data){
                Cell startCell = new KeyValue(cell.getRow(),cell.getFamily(),cell.getQualifier(),clock.currentTimeMillis(),KeyValue.Type.Put);
                Cell stopCell = new KeyValue(cell.getRow(),cell.getFamily(),cell.getQualifier(),0,KeyValue.Type.Put);
                SortedSet<Cell> positionPoint = this.data.subSet(startCell,stopCell);
                long cellTs = cell.getTimestamp();
                Iterator<Cell> iter = positionPoint.iterator();
                while(iter.hasNext()){
                    Cell c = iter.next();
                    if(c.getTimestamp()<cellTs)
                        iter.remove();
                }
            }
        }
        return new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
    }

    private OperationStatus doPut(Put put){
        NavigableMap<byte[], List<Cell>> familyCellMap=put.getFamilyCellMap();
        for(List<Cell> data:familyCellMap.values()){
            this.data.addAll(data);
        }
        return new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
    }
}

package com.splicemachine.si.impl.compaction;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.CellType;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 11/11/15
 */
public class CompactionTestData implements Comparable<CompactionTestData>{
    public byte[] rowKey;
    public Map<CellType,List<TxnView>> dataMap;
    public long mat;
    public int numCols;
    public Random random = new Random(0);

    public CompactionTestData copy(){
        CompactionTestData td = new CompactionTestData();
        td.rowKey = rowKey;
        td.dataMap = dataMap;
        td.mat = mat;
        td.numCols = numCols;
        dataMap = null;
        return td;
    }

    public CompactionTestData insert(TxnView txn){
        if(dataMap==null)
            dataMap = new EnumMap<>(CellType.class);

        List<TxnView> checkpoints=dataMap.get(CellType.CHECKPOINT);
        if(checkpoints==null){
            checkpoints = new ArrayList<>();
            dataMap.put(CellType.CHECKPOINT,checkpoints);
        }

        List<TxnView> users=dataMap.get(CellType.USER_DATA);
        if(users==null){
            users = new ArrayList<>();
            dataMap.put(CellType.USER_DATA,users);
        }

        checkpoints.add(txn);
        users.add(txn);

        return this;
    }

    public CompactionTestData update(TxnView updateTxn){
        dataMap.get(CellType.USER_DATA).add(updateTxn);
        return this;
    }

    public CompactionTestData delete(TxnView txn){
        List<TxnView> txnViews=dataMap.get(CellType.TOMBSTONE);
        if(txnViews==null){
            txnViews = new ArrayList<>();
            dataMap.put(CellType.TOMBSTONE,txnViews);
        }
        txnViews.add(txn);
        return this;
    }

    public CompactionTestData insertWithAntiTombstone(TxnView txn){
        insert(txn);

        List<TxnView> txnViews=dataMap.get(CellType.ANTI_TOMBSTONE);
        if(txnViews==null){
            txnViews = new ArrayList<>();
            dataMap.put(CellType.ANTI_TOMBSTONE,txnViews);
        }
        txnViews.add(txn);
        return this;
    }

    @Override
    public int compareTo(CompactionTestData o){
        return Bytes.compareTo(rowKey,o.rowKey);
    }

    public boolean isRolledBack(){
        for(List<TxnView> txnList:dataMap.values()){
            for(TxnView txn:txnList){
                if(txn.getEffectiveState()!=Txn.State.ROLLEDBACK) return false;
            }
        }
        return true;
    }

    public long highestVisibleTimestamp(CellType cellType){
        List<TxnView> txns = dataMap.get(cellType);
        Collections.sort(txns,CompactionTestUtils.reverseTxnComparator);

        for(TxnView txn:txns){
            if(txn.getEffectiveState()!=Txn.State.ROLLEDBACK){
                return txn.getBeginTimestamp();
            }
        }
        return -1l;
    }

    public long highestBelowMat(CellType cellType){
        List<TxnView> txns = dataMap.get(cellType);
        Collections.sort(txns,CompactionTestUtils.reverseTxnComparator);

        for(TxnView txn:txns){
            if(txn.getBeginTimestamp()>=mat)  continue;
            if(txn.getEffectiveState()!=Txn.State.ROLLEDBACK){
                return txn.getBeginTimestamp();
            }
        }
        return -1l;
    }

    @Override
    public String toString(){
        return "{"+Encoding.decodeString(rowKey)+"}";
    }

    public void cacheAllTxns(TxnSupplier txnStore){
        for(List<TxnView> txns : dataMap.values()){
            for(TxnView txn:txns){
                txnStore.cache(txn);
            }
        }
    }

    public List<Cell> build() throws IOException{
        List<Cell> c=CompactionTestUtils.buildCheckpointCells(dataMap.get(CellType.CHECKPOINT),rowKey);
        c.addAll(CompactionTestUtils.buildCommitTimestamps(dataMap.get(CellType.COMMIT_TIMESTAMP),rowKey));
        c.addAll(CompactionTestUtils.buildTombstoneCells(dataMap.get(CellType.TOMBSTONE), dataMap.get(CellType.ANTI_TOMBSTONE),rowKey));
        c.addAll(CompactionTestUtils.buildUserCells(dataMap.get(CellType.CHECKPOINT), dataMap.get(CellType.USER_DATA),rowKey,numCols,random));
        return c;
    }
}

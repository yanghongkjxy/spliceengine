package com.splicemachine.si.impl.compaction;

import com.google.common.collect.Iterators;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 11/11/15
 */
public class CompactionTestUtils{

    public static final Comparator<? super Cell> kvComparator=new KeyValue.KVComparator();
    public static final Comparator<? super TxnView> txnComparator=new Comparator<TxnView>(){
        @Override
        public int compare(TxnView o1,TxnView o2){
            return Long.compare(o1.getBeginTimestamp(),o2.getBeginTimestamp());
        }
    };

    public static final Comparator<? super TxnView> reverseTxnComparator=new Comparator<TxnView>(){
        @Override
        public int compare(TxnView o1,TxnView o2){
            return -1*txnComparator.compare(o1,o2);
        }
    };

    public static List<Cell> buildTombstoneCells(List<TxnView> tombstoneTxns,
                                                 List<TxnView> antiTombstoneTxns,byte[] rowKey){
        if(tombstoneTxns==null || tombstoneTxns.size()<=0){
            Assert.assertTrue("Programmer Error: Antitombstones without tombstones!",antiTombstoneTxns==null || antiTombstoneTxns.size()<=0);
            return Collections.emptyList();
        }
        List<Cell> tCells=new ArrayList<>();
        Collections.sort(tombstoneTxns,txnComparator);
        Collections.reverse(tombstoneTxns);
        if(antiTombstoneTxns!=null){
            Collections.sort(antiTombstoneTxns,txnComparator);
            Collections.reverse(antiTombstoneTxns);
        }

        Iterator<TxnView> tIter=tombstoneTxns.iterator();
        Iterator<TxnView> aTIter=antiTombstoneTxns!=null?antiTombstoneTxns.iterator():Iterators.<TxnView>emptyIterator();
        TxnView t=null;
        TxnView aT=null;
        do{
            if(t==null){
                if(tIter.hasNext())
                    t=tIter.next();
            }
            if(aT==null){
                if(aTIter.hasNext())
                    aT=aTIter.next();
            }
            if(t==null){
                if(aT==null) break;
                else{
                    tCells.add(antiTombstone(aT,rowKey));
                    aT=null;
                }
            }else if(aT==null){
                tCells.add(tombstone(t,rowKey));
                t=null;
            }else if(txnComparator.compare(t,aT)>=0){
                tCells.add(tombstone(t,rowKey));
                t=null;
            }else{
                tCells.add(antiTombstone(aT,rowKey));
                aT=null;
            }
        }while(t!=null || aT!=null);
        return tCells;
    }

    private static Cell tombstone(TxnView next,byte[] rowKey){
        return new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                next.getBeginTimestamp(),FixedSIConstants.EMPTY_BYTE_ARRAY);
    }

    private static Cell antiTombstone(TxnView next,byte[] rowKey){
        return new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                next.getBeginTimestamp(),FixedSIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES);
    }

    public static void assertSorted(List<Cell> actualData){
        Cell p=null;
        for(Cell c : actualData){
            if(p==null)
                p=c;
            else{
                int compare=kvComparator.compare(p,c);
                Assert.assertTrue("Not sorted properly!",compare<0);
            }
        }
    }

    public static List<List<Cell>> rowFormat(List<Cell> actualData){
        Map<Long, List<Cell>> rows=new TreeMap<Long, List<Cell>>(){
            @Override
            public List<Cell> get(Object key){
                List<Cell> cells=super.get(key);
                if(cells==null){
                    cells=new ArrayList<>(5);
                    super.put((Long)key,cells);
                }
                return cells;
            }
        };
        for(Cell c : actualData){
            rows.get(c.getTimestamp()).add(c);
        }

        List<List<Cell>> finalRows=new ArrayList<>(rows.size());
        for(List<Cell> v : rows.values()){
            Collections.sort(v,kvComparator);
            finalRows.add(v);
        }
        Collections.reverse(finalRows);
        return finalRows;
    }

    public static List<Cell> buildCommitTimestamps(List<TxnView> commitTsTxns,byte[] rowKey){
        if(commitTsTxns==null || commitTsTxns.size()<=0) return Collections.emptyList();
        List<Cell> cts=new ArrayList<>(commitTsTxns.size());
        for(TxnView view : commitTsTxns){
            cts.add(new KeyValue(rowKey,
                    FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                    view.getBeginTimestamp(),
                    Bytes.toBytes(view.getGlobalCommitTimestamp())));
        }
        Collections.sort(cts,kvComparator);
        return cts;
    }

    public static List<Cell> buildCheckpointCells(List<TxnView> checkpointTxns,byte[] rowKey){
        List<Cell> cts=new ArrayList<>(checkpointTxns.size());
        for(TxnView view : checkpointTxns){
            long gCT=view.getGlobalCommitTimestamp();
            byte[] data;
            if(gCT<=0)
                data=FixedSIConstants.EMPTY_BYTE_ARRAY;
            else
                data=Bytes.toBytes(gCT);
            cts.add(new KeyValue(rowKey,
                    FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,
                    view.getBeginTimestamp(),
                    data));
        }
        Collections.sort(cts,kvComparator);
        return cts;
    }

    public static List<Cell> buildUserCells(List<TxnView> checkpointTxns,List<TxnView> userTxns,byte[] rowKey,int numCols,Random random) throws IOException{
        Set<Long> checkpoints=new HashSet<>();
        for(TxnView checkpointTxn : checkpointTxns){
            checkpoints.add(checkpointTxn.getBeginTimestamp());
        }
        Collections.sort(userTxns,txnComparator);
        Collections.reverse(userTxns);

        List<Cell> userCells=new ArrayList<>(userTxns.size());
        for(TxnView userTxn : userTxns){
            if(checkpoints.contains(userTxn.getBeginTimestamp())){
                userCells.add(fullUserCell(userTxn,rowKey,numCols));
            }else
                userCells.add(partialUserCell(userTxn,rowKey,numCols,random));
        }

        Collections.sort(userCells,kvComparator);
        return userCells;
    }

    public static Cell partialUserCell(TxnView txn,byte[] rowKey,int numCols,BitSet filledColumns,String prefix) throws IOException{
        com.carrotsearch.hppc.BitSet cols=new com.carrotsearch.hppc.BitSet(numCols);
        for(int i=filledColumns.nextSetBit(0);i>=0;i=filledColumns.nextSetBit(i+1)){
           cols.set(i);
        }

        com.carrotsearch.hppc.BitSet empty=new com.carrotsearch.hppc.BitSet();
        EntryEncoder ee=EntryEncoder.create(new KryoPool(1),numCols,cols,empty,empty,empty);
        MultiFieldEncoder entryEncoder=ee.getEntryEncoder();
        int k =0;
        for(int i=0;i<filledColumns.cardinality();i++){
            k = filledColumns.nextSetBit(k);
            entryEncoder.encodeNext(prefix+"col"+k);
            k++;
        }
        byte[] value=ee.encode();

        return new KeyValue(rowKey,FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSpliceConstants.PACKED_COLUMN_BYTES,
                txn.getBeginTimestamp(),
                value);

    }

    public static Cell partialUserCell(TxnView txn,byte[] rowKey,int numCols,Random random) throws IOException{
        int toFill=random.nextInt(numCols);
        if(toFill==0) toFill=1;
        int dist=numCols/toFill;

        com.carrotsearch.hppc.BitSet cols=new com.carrotsearch.hppc.BitSet(numCols);
        for(int i=0;i<toFill;i++){
            cols.set(i*dist);
        }

        com.carrotsearch.hppc.BitSet empty=new com.carrotsearch.hppc.BitSet();
        EntryEncoder ee=EntryEncoder.create(new KryoPool(1),numCols,cols,empty,empty,empty);
        MultiFieldEncoder entryEncoder=ee.getEntryEncoder();
        for(int i=0;i<toFill;i++){
            entryEncoder.encodeNext("col"+(i*dist));
        }
        byte[] value=ee.encode();

        return new KeyValue(rowKey,FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSpliceConstants.PACKED_COLUMN_BYTES,
                txn.getBeginTimestamp(),
                value);
    }

    public static Cell fullUserCell(TxnView txn,byte[] rowKey,int numCols) throws IOException{
        com.carrotsearch.hppc.BitSet cols=new com.carrotsearch.hppc.BitSet(numCols);
        cols.set(0,numCols);

        com.carrotsearch.hppc.BitSet empty=new com.carrotsearch.hppc.BitSet();

        EntryEncoder ee=EntryEncoder.create(new KryoPool(1),numCols,cols,empty,empty,empty);
        MultiFieldEncoder entryEncoder=ee.getEntryEncoder();
        for(int i=0;i<numCols;i++){
            entryEncoder.encodeNext("col"+i);
        }
        byte[] value=ee.encode();

        return new KeyValue(rowKey,FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSpliceConstants.PACKED_COLUMN_BYTES,
                txn.getBeginTimestamp(),
                value);
    }
}

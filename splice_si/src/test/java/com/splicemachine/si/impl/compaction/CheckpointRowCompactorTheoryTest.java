package com.splicemachine.si.impl.compaction;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.*;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.index.BitIndex;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 11/10/15
 */
@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
@RunWith(Theories.class)
public class CheckpointRowCompactorTheoryTest{
    private static final SDataLib dataLib=new TestDataLib();
    private static final CellTypeParser ctParser=DefaultCellTypeParser.INSTANCE;


    @DataPoints public static CompactionTestData[] aboveMatTests(){
        /*Above the MAT tests */
        CompactionTestData td = new CompactionTestData();
        td.numCols = 4;
        td.rowKey = Encoding.encode("row");

        td.mat = 1l;

        List<CompactionTestData> data=loadTestScenarios(td);

        return data.toArray(new CompactionTestData[data.size()]);
    }

    @DataPoints public static CompactionTestData[] belowMatTest(){
        /*Below the MAT tests */
        CompactionTestData td = new CompactionTestData();
        td.numCols = 4;
        td.rowKey = Encoding.encode("row");

        td.mat = 100l;

        List<CompactionTestData> data=loadTestScenarios(td);

        return data.toArray(new CompactionTestData[data.size()]);
    }

    @DataPoints public static CompactionTestData[] middleMat(){
        /*Below the MAT tests */
        CompactionTestData td = new CompactionTestData();
        td.numCols = 4;
        td.rowKey = Encoding.encode("row");

        List<CompactionTestData> d = new ArrayList<>();
        for(int i = 2;i<10;i++){
            td.mat=i;
            d.addAll(loadTestScenarios(td));
        }

        return d.toArray(new CompactionTestData[d.size()]);
    }

    private static List<CompactionTestData> loadTestScenarios(CompactionTestData td){
        List<CompactionTestData> data = new ArrayList<>();
        //just a single insert
        TxnView uTxn = new CommittedTxn(2l,3l);
        td.insert(uTxn);
        data.add(td.copy());

        //single insert with commit timestamp
        td.insert(uTxn);
        td.dataMap.put(CellType.COMMIT_TIMESTAMP,Arrays.asList(uTxn));
        data.add(td.copy());

        TxnView cTxn4l=new CommittedTxn(4l,5l);
        TxnView rb = new RolledBackTxn(6l);
        //insert and update
        td.insert(uTxn).update(cTxn4l);
        data.add(td.copy());

        td.insert(uTxn).update(cTxn4l);
        td.dataMap.put(CellType.COMMIT_TIMESTAMP,Arrays.asList(uTxn));
        data.add(td.copy());

        td.insert(uTxn).update(cTxn4l);
        td.dataMap.put(CellType.COMMIT_TIMESTAMP,Arrays.asList(uTxn,cTxn4l));
        data.add(td.copy());

        //insert rolled back
        td.insert(rb);
        data.add(td.copy());

        //insert then update rolled back
        td.insert(uTxn).update(rb);
        data.add(td.copy());

        td.insert(uTxn).update(rb);
        data.add(td.copy());

        td.insert(uTxn).update(rb);
        td.dataMap.put(CellType.COMMIT_TIMESTAMP,Arrays.asList(uTxn));
        data.add(td.copy());

        //insert then delete
        td.insert(uTxn).delete(cTxn4l);
        data.add(td.copy());

        //insert then delete rolled back
        td.insert(uTxn).delete(rb);
        data.add(td.copy());

        //insert then delete then insert again
        TxnView ri = new CommittedTxn(7l,8l);
        td.insert(uTxn).delete(cTxn4l).insertWithAntiTombstone(ri);
        data.add(td.copy());

        //insert then delete then insert again rolled back
        td.insert(uTxn).delete(cTxn4l).insertWithAntiTombstone(rb);
        data.add(td.copy());

        return data;
    }

    @Theory
    public void checkData(CompactionTestData testData) throws Exception{
        TxnSupplier txnStore = new TestTxnSupplier();
        RowAccumulator<Cell> accumulator =new HRowAccumulator<>(dataLib,EntryPredicateFilter.EMPTY_PREDICATE,new EntryDecoder(),false);
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(testData.mat,DefaultCellTypeParser.INSTANCE,accumulator,txnStore);

        Map<CellType,List<TxnView>> dataMap = testData.dataMap;
        for(List<TxnView> txnList:dataMap.values()){
            for(TxnView txn:txnList){
                txnStore.cache(txn);
            }
        }
        byte[] rk = testData.rowKey;
        List<Cell> checkpointCells = CompactionTestUtils.buildCheckpointCells(dataMap.get(CellType.CHECKPOINT),rk);
        List<Cell> commitTimestampCells = CompactionTestUtils.buildCommitTimestamps(dataMap.get(CellType.COMMIT_TIMESTAMP),rk);
        List<Cell> tombstoneCells = CompactionTestUtils.buildTombstoneCells(dataMap.get(CellType.TOMBSTONE),dataMap.get(CellType.ANTI_TOMBSTONE),rk);
        List<Cell> userDataCells = CompactionTestUtils.buildUserCells(dataMap.get(CellType.CHECKPOINT),dataMap.get(CellType.USER_DATA),rk,testData.numCols,testData.random);

        Assert.assertTrue("Did not register as empty!",compactor.isEmpty());
        for(Cell c:checkpointCells){
            compactor.addCell(c);
        }
        for(Cell c:commitTimestampCells){
            compactor.addCell(c);
        }
        for(Cell c:tombstoneCells){
            compactor.addCell(c);
        }
        for(Cell c:userDataCells){
            compactor.addCell(c);
        }

        List<Cell> actualData = new ArrayList<>();
        compactor.reverse();
        compactor.placeCells(actualData,Integer.MAX_VALUE);

        if(actualData.size()<=0){
            /*
             * There are two possibilities:
             *
             * 1. There are tombstones below the MAT causing the row to be removed
             * 2. The entire data set is rolled back, in which case the row doesn't exist
             */
            if(testData.isRolledBack())
                return; //life is good, it did what it was supposed to

            assertTombstoneHighestBelowMAT(dataMap.get(CellType.TOMBSTONE),dataMap.get(CellType.USER_DATA),testData.mat);
            return; //we are done
        }
        //Rule 0: Data is returned in sorted order
        assertSorted(actualData);

        List<List<Cell>> actualRowData = CompactionTestUtils.rowFormat(actualData);

        //Rule 0: If we return a row, at least one version shouldn't be rolled back
        long highTimestamp = testData.highestVisibleTimestamp(CellType.USER_DATA);
        Assert.assertTrue("Returned data when everything is rolled back!",highTimestamp>0);

        long highBelowMat = testData.highestBelowMat(CellType.USER_DATA);
        //highBelowMat<=0 implies that all data is above the mat

        if(highBelowMat>0){
            //Rule 1: All data below first insert below MAT was discarded
            assertNothingBelow(highBelowMat,actualRowData);
            //Rule 2: No data between lowest insert cell and MAT
            assertNothingBetween(highBelowMat,actualRowData,testData.mat);
        }
        //Rule 4: All Committed rows have a commit timestamp, and they are accompanied by a least one other cell
        assertTransactionallyCorrect(actualRowData,txnStore);

        //Rule 6: There is at least one checkpointCell
        assertContainsCheckpointCell(actualRowData,testData.numCols);
        //Rule 8: All Tombstones have a user cell before them
        assertTombstonesHavePriorUserData(actualRowData);
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void assertTombstoneHighestBelowMAT(List<TxnView> tombstoneTxns,List<TxnView> userTxns,long mat){
        Assert.assertTrue("No tombstones present!",tombstoneTxns.size()>0);
        Collections.sort(tombstoneTxns,CompactionTestUtils.txnComparator);
        Collections.reverse(tombstoneTxns);

        Collections.sort(userTxns,CompactionTestUtils.txnComparator);
        Collections.reverse(userTxns);

        long highestTombstoneId = -1l;
        for(TxnView n : tombstoneTxns){
            if(n.getEffectiveState()!=Txn.State.ROLLEDBACK){
                highestTombstoneId=n.getTxnId();
                break;
            }
        }
        Assert.assertTrue("All tombstones were rolled back, so row should be present!",highestTombstoneId>0);
        Assert.assertTrue("the Highest tombstone cell is not below the MAT!",highestTombstoneId<mat);

        for(TxnView userTxn:userTxns){
            if(userTxn.getEffectiveState()==Txn.State.ROLLEDBACK) continue;
            Assert.assertTrue("User cell not below mat!",userTxn.getBeginTimestamp()<mat);
            Assert.assertTrue("User cell is not below the highest tombstone id!",userTxn.getBeginTimestamp()<highestTombstoneId);
        }
    }

    private void assertTombstonesHavePriorUserData(List<List<Cell>> actualRowData){
        for(int i=0;i<actualRowData.size();i++){
            List<Cell> row = actualRowData.get(i);
            for(Cell c:row){
                CellType ct = ctParser.parseCellType(c);
                if(ct==CellType.TOMBSTONE){
                    boolean foundUserData = false;
                    for(int j=i+1;j<actualRowData.size();j++){
                        List<Cell> priorRow = actualRowData.get(j);
                        for(Cell p:priorRow){
                            CellType pt = ctParser.parseCellType(p);
                            if(pt==CellType.USER_DATA){
                                foundUserData=true;
                                break;
                            }
                        }
                    }
                    Assert.assertTrue("Did not find a prior user aboveMatTests for tombstone!",foundUserData);
                }
            }
        }
    }

    private void assertContainsCheckpointCell(List<List<Cell>> actualRowData,int numCols){
        /*
         * Check that:
         *
         * 1. At least one insert exists
         * 2. No Anti-tombstones are present
         * 3. No Tombstones are present on the same row as a insert
         * 4. Every insert row also has a fully populated User data row
         *
         */
        boolean foundCheckpoint = false;
        for(List<Cell> row:actualRowData){
            Cell c = row.get(0);
            CellType ct = ctParser.parseCellType(c);
            if(ct==CellType.CHECKPOINT){
                foundCheckpoint=true;
                boolean foundUserData = false;
                for(int i=1;i<row.size();i++){
                    Cell u = row.get(i);
                    CellType ut = ctParser.parseCellType(u);
                    if(ut==CellType.USER_DATA){
                        foundUserData=true;
                        assertHasAllUserColumns(u,numCols);
                        break;
                    }else if(ut==CellType.TOMBSTONE){
                        Assert.fail("Tombstone found on a checkpointed row!");
                    }else if(ut==CellType.ANTI_TOMBSTONE){
                        Assert.fail("Anti-Tombstone found on a checkpointed row!");
                    }
                }
                Assert.assertTrue("No User aboveMatTests found!",foundUserData);
            }
        }
        Assert.assertTrue("Did not find a insert cell!",foundCheckpoint);
    }

    private void assertHasAllUserColumns(Cell u,int numCols){
        EntryDecoder ed = new EntryDecoder();
        ed.set(u.getValueArray(),u.getValueOffset(),u.getValueLength());
        BitIndex currentIndex=ed.getCurrentIndex();
        for(int i=0;i<numCols;i++){
            Assert.assertTrue("Missing an entry("+i+") in the index!",currentIndex.isSet(i));
        }
    }

    private void assertTransactionallyCorrect(List<List<Cell>> actualRowData,TxnSupplier txnStore) throws IOException{
        /*
         * Make sure that
         *
         * 1. If the transaction for the row is committed, then the row contains a proper Commit timestamp. If there
         * is a Checkpoint, then the insert is also properly constructed
         * 2. No rolled back data is present
         * 3. Active data does NOT have a Commit timestamp, and any insert is empty
         */
        for(List<Cell> row:actualRowData){
            long ts = row.get(0).getTimestamp();
            TxnView txn = txnStore.getTransaction(ts);
            if(txn.getEffectiveState()==Txn.State.COMMITTED){
                Assert.assertTrue("Commit timestamp not present!",row.size()>=2);
                //either the first or second entry is a commit timestamp
                Cell c = row.get(0);
                CellType ct = ctParser.parseCellType(c);
                if(ct!=CellType.COMMIT_TIMESTAMP){
                    Assert.assertEquals("Neither insert nor Commit Timestamp is the first cell!!",CellType.CHECKPOINT,ct);
                }
                assertCorrectGlobalCommitTimestamp(txn,c);
            }else if(txn.getEffectiveState()==Txn.State.ROLLEDBACK){
                Assert.fail("Rolled back aboveMatTests is present in the returned results!");
            } else{
                //there should be no commit timestamp present
                for(Cell c:row){
                    CellType ct= ctParser.parseCellType(c);
                    if(ct==CellType.CHECKPOINT){
                        Assert.assertEquals("Checkpoint is not empty!",0,c.getValueLength());
                    }
                    Assert.assertNotEquals("Contains a commit timestamp accidentally!",CellType.COMMIT_TIMESTAMP,ct);
                }
            }
        }
    }

    private void assertCorrectGlobalCommitTimestamp(TxnView txn,Cell c){
        long gCt = Bytes.toLong(c.getValueArray(),c.getValueOffset(),c.getValueLength());
        Assert.assertNotEquals("Global Timestamp not set!",-1l,gCt);
        Assert.assertEquals("Incorrect global commit timestamp!",txn.getGlobalCommitTimestamp(),gCt);
    }

    private void assertNothingBetween(long ts,List<List<Cell>> actualRowData,long mat){
        for(List<Cell> row:actualRowData){
            long timestamp=row.get(0).getTimestamp();
            if(timestamp>=mat) continue;
            if(timestamp==ts) continue;
            Assert.fail("Timestamp "+ timestamp+" Falls in the forbidden range of ("+ts+","+mat+")");
        }
    }

    private void assertNothingBelow(long discardPoint,List<List<Cell>> actualRowData){
        for(List<Cell> row:actualRowData){
            Assert.assertTrue("Data below discard point is present!",row.get(0).getTimestamp()>=discardPoint);
        }
    }

    private void assertSorted(List<Cell> actualData){
        Comparator<Cell> comparator = new KeyValue.KVComparator();
        Cell p = null;
        for(Cell c:actualData){
            if(p==null)
                p = c;
            else{
                int compare = comparator.compare(p,c);
                Assert.assertTrue("Not sorted properly!",compare<0);
            }
        }
    }
}

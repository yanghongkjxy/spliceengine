package com.splicemachine.si.impl.compaction;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 11/11/15
 */
@SuppressWarnings("unchecked")
@RunWith(Theories.class)
public class CheckpointCompactionScannerTest{
    private static final SDataLib dataLib=new TestDataLib();
    private static final CellTypeParser ctParser=DefaultCellTypeParser.INSTANCE;

    private static final int numRows = 400;

    @BeforeClass
    public static void setUp() throws Exception{
        //disable info logging to save time
       Logger.getLogger(CheckpointCompactionScanner.class).setLevel(Level.WARN);
    }

    @SuppressWarnings("unchecked")
    @DataPoints
    public static List<CompactionTestData>[] committedAboveMAT(){
        List<List<CompactionTestData>> data=new ArrayList<>();
        CompactionTestData t=new CompactionTestData();
        t.mat=1l;

        //multiple committed inserts, all above the MAT
        data.add(insertCommitted(numRows,t));

        //a committed delete every other row, all above the MAT
        data.add(insertAndDeleteCommitted(numRows,t));

        //a committed update on every 3rd row, all above MAT
        data.add(insertAndUpdateCommitted(numRows,t));

        data.add(insertLotsOfUpdatesCommitted(numRows,t));

        //every 2nd row is updated, every 3rd row is deleted, all above the MAT, all committed
        data.add(insertUpdateDeleteCommitted(numRows,t));

        data.add(insertDeleteInsertCommitted(numRows,t));

        return data.toArray(new List[data.size()]);
    }

    @SuppressWarnings("unchecked")
    @DataPoints
    public static List<CompactionTestData>[] committedMiddleMAT(){
        List<List<CompactionTestData>> data=new ArrayList<>();
        CompactionTestData t=new CompactionTestData();
        t.mat=5l;

        //multiple committed inserts, all above the MAT
        data.add(insertCommitted(numRows,t));

        //a committed delete every other row, all above the MAT
        data.add(insertAndDeleteCommitted(numRows,t));

        //a committed update on every 3rd row, all above MAT
        data.add(insertAndUpdateCommitted(numRows,t));

        data.add(insertLotsOfUpdatesCommitted(numRows,t));

        //every 2nd row is updated, every 3rd row is deleted, all above the MAT, all committed
        data.add(insertUpdateDeleteCommitted(numRows,t));

        data.add(insertDeleteInsertCommitted(numRows,t));

        return data.toArray(new List[data.size()]);
    }

    @SuppressWarnings("unchecked")
    @DataPoints
    public static List<CompactionTestData>[] committedBelowMAT(){
        List<List<CompactionTestData>> data=new ArrayList<>();
        CompactionTestData t=new CompactionTestData();
        t.mat=1000l;

        //multiple committed inserts, all above the MAT
        data.add(insertCommitted(numRows,t));

        //a committed delete every other row, all above the MAT
        data.add(insertAndDeleteCommitted(numRows,t));

        //a committed update on every 3rd row, all above MAT
        data.add(insertAndUpdateCommitted(numRows,t));

        data.add(insertLotsOfUpdatesCommitted(numRows,t));

        //every 2nd row is updated, every 3rd row is deleted, all above the MAT, all committed
        data.add(insertUpdateDeleteCommitted(numRows,t));

        return data.toArray(new List[data.size()]);
    }

    @SuppressWarnings("unchecked")
    @DataPoints
    public static List<CompactionTestData>[] activeAboveMAT(){
        List<List<CompactionTestData>> data=new ArrayList<>();
        CompactionTestData t=new CompactionTestData();
        t.mat=1l;

        //every 4th row is active, not committed, all above the MAT
        data.add(insertSomeActive(numRows,t));

        //some active inserts and deletes above MAT
        data.add(insertAndDeleteSomeActive(numRows,t));

        //some active inserts and updates above MAT
        data.add(insertAndUpdateSomeActive(numRows,t));


        //some active inserts, updates, and deletes, above MAT
        data.add(insertUpdateDeleteSomeActive(numRows,t));

        return data.toArray(new List[data.size()]);
    }

    @SuppressWarnings("unchecked")
    @DataPoints
    public static List<CompactionTestData>[] rolledBackAboveMAT(){
        List<List<CompactionTestData>> data=new ArrayList<>();
        CompactionTestData t=new CompactionTestData();
        t.mat=1l;

        //some rolled back data, above MAT
        data.add(insertSomeRolledBack(numRows,t));
        data.add(insertAndUpdateSomeRolledBack(numRows,t));
        data.add(insertAndDeleteSomeRolledBack(numRows,t));
        data.add(insertUpdateDeleteSomeRolledBack(numRows,t));

        return data.toArray(new List[data.size()]);
    }

    @SuppressWarnings("unchecked")
    @DataPoints
    public static List<CompactionTestData>[] rolledBackBelowMAT(){
        List<List<CompactionTestData>> data=new ArrayList<>();
        CompactionTestData t=new CompactionTestData();
        t.mat=1000l;

        //some rolled back data, above MAT
        data.add(insertSomeRolledBack(numRows,t));
        data.add(insertAndUpdateSomeRolledBack(numRows,t));
        data.add(insertAndDeleteSomeRolledBack(numRows,t));
        data.add(insertUpdateDeleteSomeRolledBack(numRows,t));

        return data.toArray(new List[data.size()]);
    }

    @SuppressWarnings("unchecked")
    @DataPoints
    public static List<CompactionTestData>[] rolledBackMiddleMAT(){
        List<List<CompactionTestData>> data=new ArrayList<>();
        CompactionTestData t=new CompactionTestData();

        for(int i=2;i<6;i++){
            t.mat=i;

            //some rolled back data, above MAT
            data.add(insertSomeRolledBack(numRows,t));
            data.add(insertAndUpdateSomeRolledBack(numRows,t));
            data.add(insertAndDeleteSomeRolledBack(numRows,t));
            data.add(insertUpdateDeleteSomeRolledBack(numRows,t));
        }

        return data.toArray(new List[data.size()]);
    }


    /* ****************************************************************************************************************/
    /*Tests*/

    @Theory
    public void checkRowsNoLimit(List<CompactionTestData> rows) throws Exception{
        int limit = -1;
        checkDataCorrectness(rows,limit);
    }

    @Theory
    public void checkRowsWithLimit(List<CompactionTestData> rows) throws Exception{
        int limit = 10;
        checkDataCorrectness(rows,limit);
    }

    private void checkDataCorrectness(List<CompactionTestData> rows,int limit) throws IOException{
        TxnSupplier txnStore=new TestTxnSupplier();

        long mat=Long.MAX_VALUE;
        for(CompactionTestData row : rows){
            long m=row.mat;
            if(m<mat)
                mat=m;
            row.cacheAllTxns(txnStore);
        }
        Collections.sort(rows);
        TScanner scanner=new TScanner(rows);

        try(CheckpointCompactionScanner ccs=CheckpointCompactionScanner.majorScanner(txnStore,dataLib,ctParser,
                scanner,NoopRollForward.INSTANCE,mat)){


            List<Cell> results=new ArrayList<>();
            Iterator<CompactionTestData> rowIter=rows.iterator();
            boolean shouldContinue;
            do{
                if(limit>0){
                    List<Cell> r=new ArrayList<>(limit);
                    ByteSlice currKey=new ByteSlice();
                    do{
                        shouldContinue=ccs.next(r,limit);
                        if(r.size()<=0) break;

                        Cell cell=r.get(0);
                        if(currKey.length()>0 && !currKey.equals(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength())){
                            processRow(results,rowIter,txnStore);
                            results.clear();
                        }
                        currKey.set(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                        results.addAll(r);
                        r.clear();
                    }while(shouldContinue);
                }else
                    shouldContinue=ccs.next(results);

                processRow(results,rowIter,txnStore);
                results.clear();
            }while(shouldContinue);
            Assert.assertFalse("Data is still remaining!",ccs.next(results));
            Assert.assertEquals("Data was returned after it shouldn't be!",0,results.size());
            while(rowIter.hasNext()){
                CompactionTestData row = rowIter.next();
                assertShouldBeEmptyRow(row);
            }
        }
    }

    private void processRow(List<Cell> results,Iterator<CompactionTestData> rowIter,TxnSupplier txnStore) throws IOException{
        if(results.size()<=0){
            //make sure that all remaining rows should be empty
            while(rowIter.hasNext()){
                CompactionTestData ctd = rowIter.next();
                assertShouldBeEmptyRow(ctd);
            }
        }else{
            /*
             * Iterate until we find the test data at or immediately past it. If we pass the row,
             * that's an error, but for every row we skip, make sure it should be empty
             */
            Cell c = results.get(0);
            while(rowIter.hasNext()){
                CompactionTestData ctd = rowIter.next();
                int compare = Bytes.compareTo(ctd.rowKey,0,ctd.rowKey.length,c.getRowArray(),c.getRowOffset(),c.getRowLength());
                if(compare<0){
                    assertShouldBeEmptyRow(ctd);
                }else if(compare==0){
                    assertRowCorrect(ctd,txnStore,results);
                    break;
                }else{
                    Assert.fail("Missing row "+ctd);
                }
            }
        }
    }

    private void assertRowCorrect(CompactionTestData correct,
                                  TxnSupplier txnSupplier,
                                  List<Cell> results) throws IOException{

        CompactionTestUtils.assertSorted(results);

        List<List<Cell>> actualRowData=CompactionTestUtils.rowFormat(results);

        long highTimestamp=correct.highestVisibleTimestamp(CellType.USER_DATA);
        Assert.assertTrue("Returned data when everything is rolled back!",highTimestamp>0);

        long highBelowMat=correct.highestBelowMat(CellType.USER_DATA);

        if(highBelowMat>0){
            //make sure everything below the MAT is correct
            assertCorrectBelowMat(actualRowData,highBelowMat,correct.mat);
        }

        //Rule 4: All committed rows have a commit timestamp, and are accompanied by either a User or a tombstone cell
        boolean foundCheckpoint=assertTransactionallyCorrect(actualRowData,txnSupplier,correct);
        Assert.assertTrue("At least one checkpoint must be present on a returned row!",foundCheckpoint);

        //Rule 5: All tombstones must have at least one USER_DATA cell prior to them
        assertTombstonesHavePriorUserData(actualRowData);
    }

    private void assertTombstonesHavePriorUserData(List<List<Cell>> actualRowData){
        for(int i=0;i<actualRowData.size();i++){
            List<Cell> row=actualRowData.get(i);
            for(Cell c : row){
                CellType ct=ctParser.parseCellType(c);
                if(ct==CellType.TOMBSTONE){
                    boolean foundUserData=false;
                    for(int j=i+1;j<actualRowData.size();j++){
                        List<Cell> priorRow=actualRowData.get(j);
                        for(Cell p : priorRow){
                            CellType pt=ctParser.parseCellType(p);
                            if(pt==CellType.USER_DATA){
                                foundUserData=true;
                                break;
                            }
                        }
                    }
                    Assert.assertTrue("Did not find USER_DATA prior to a tombstone!",foundUserData);
                }
            }
        }
    }

    private boolean assertTransactionallyCorrect(List<List<Cell>> actualRowData,
                                                 TxnSupplier txnSupplier,
                                                 CompactionTestData correctData) throws IOException{
       /*
        * Make sure that
        *
        * 1. If the transaction for the row is committed:
        *  A. A commit timestamp is present and properly constructed
        *  B. If the row has a checkpoint, then the checkpoint contains the commit timestamp
        * 2. No rolled back data is present
        * 3. Active data does NOT have a commit timestamp, and any checkpoint has empty CTs
        */
        boolean checkpointPresent=false;
        for(List<Cell> row : actualRowData){
            long ts=row.get(0).getTimestamp();
            TxnView txn=txnSupplier.getTransaction(ts);
            switch(txn.getEffectiveState()){
                case COMMITTED:
                    assertProperCommittedRow(row,txn,correctData.numCols);
                    break;
                case ROLLEDBACK:
                    Assert.fail("Rolled back transaction has results present in the row!");
                    break;
                default:
                    assertProperActiveRow(row,correctData.numCols);
            }
            if(ctParser.parseCellType(row.get(0))==CellType.CHECKPOINT){
                checkpointPresent=true;
            }
        }
        return checkpointPresent;
    }

    private void assertProperActiveRow(List<Cell> row,int numCols){
        Set<CellType> foundTypes=EnumSet.noneOf(CellType.class);
        for(Cell c : row){
            switch(ctParser.parseCellType(c)){
                case CHECKPOINT:
                    Assert.assertFalse("Too many checkpoints found!",foundTypes.contains(CellType.CHECKPOINT));
                    foundTypes.add(CellType.CHECKPOINT);
                    Assert.assertEquals("Contains data!",0,c.getValueLength());
                    break;
                case COMMIT_TIMESTAMP:
                    Assert.fail("Found a commit timestamp on an active row!");
                    break;
                case TOMBSTONE:
                    Assert.assertFalse("Too many Tombstones found!",foundTypes.contains(CellType.TOMBSTONE));
                    foundTypes.add(CellType.TOMBSTONE);
                    break;
                case ANTI_TOMBSTONE:
                    Assert.assertTrue("Saw an AntiTombstone, but no checkpoint!",foundTypes.contains(CellType.CHECKPOINT));
                    Assert.fail("Should not have an AntiTombstone: should have a Checkpoint instead");
                    break;
                case USER_DATA:
                    Assert.assertFalse("User data and tombstone present on same row!",foundTypes.contains(CellType.TOMBSTONE));
                    Assert.assertFalse("Too many User data cells found!",foundTypes.contains(CellType.USER_DATA));
                    foundTypes.add(CellType.USER_DATA);
                    if(foundTypes.contains(CellType.CHECKPOINT))
                        assertAllUserColumnsPresent(c,numCols);
                    break;
            }
        }
    }

    private void assertAllUserColumnsPresent(Cell u,int numCols){
        EntryDecoder ed=new EntryDecoder();
        ed.set(u.getValueArray(),u.getValueOffset(),u.getValueLength());
        BitIndex currentIndex=ed.getCurrentIndex();
        for(int i=0;i<numCols;i++){
            Assert.assertTrue("Missing an entry("+i+") in the index!",currentIndex.isSet(i));
        }
    }

    private void assertProperCommittedRow(List<Cell> row,TxnView txn,int numCols){
        Set<CellType> foundTypes=EnumSet.noneOf(CellType.class);
        LongOpenHashSet expectedInfoData = new LongOpenHashSet();
        for(Cell c : row){
            expectedInfoData.clear();
            switch(ctParser.parseCellType(c)){
                case CHECKPOINT:
                    Assert.assertFalse("Too many checkpoints found!",foundTypes.contains(CellType.CHECKPOINT));
                    foundTypes.add(CellType.CHECKPOINT);
                    assertCorrectCommittedValue(c,txn);
                    break;
                case COMMIT_TIMESTAMP:
                    Assert.assertFalse("Too many commit timestamps found!",foundTypes.contains(CellType.COMMIT_TIMESTAMP));
                    foundTypes.add(CellType.COMMIT_TIMESTAMP);
                    assertCorrectCommittedValue(c,txn);
                    expectedInfoData.add(c.getTimestamp());
                    break;
                case TOMBSTONE:
                    Assert.assertFalse("Too many Tombstone cells found!",foundTypes.contains(CellType.TOMBSTONE));
                    foundTypes.add(CellType.TOMBSTONE);
                    expectedInfoData.remove(c.getTimestamp());
                    break;
                case ANTI_TOMBSTONE:
                    Assert.assertTrue("Saw an AntiTombstone, but no checkpoint!",foundTypes.contains(CellType.CHECKPOINT));
                    Assert.fail("Should not have an AntiTombstone: should have a Checkpoint instead");
                case USER_DATA:
                    Assert.assertFalse("User data and tombstone present on same row!",foundTypes.contains(CellType.TOMBSTONE));
                    Assert.assertFalse("Too many User data cells found!",foundTypes.contains(CellType.USER_DATA));
                    foundTypes.add(CellType.USER_DATA);
                    if(foundTypes.contains(CellType.CHECKPOINT))
                        assertAllUserColumnsPresent(c,numCols);
                    expectedInfoData.remove(c.getTimestamp());
                    break;
            }
        }
        Assert.assertTrue("A row has a commit timestamp but no tombstone or user data",expectedInfoData.isEmpty());
    }

    private void assertCorrectCommittedValue(Cell c,TxnView txn){
        Assert.assertEquals("Incorrect value length!",8,c.getValueLength());
        long ts=Bytes.toLong(c.getValueArray(),c.getValueOffset(),c.getValueLength());
        Assert.assertTrue("Commit timestamp should be positive!",ts>0);
        Assert.assertEquals("incorrect global commit timestamp!",txn.getGlobalCommitTimestamp(),ts);
    }

    private void assertCorrectBelowMat(List<List<Cell>> actualRowData,long highBelowMat,long mat){
        boolean foundCheckpoint=false;
        for(List<Cell> row : actualRowData){
            //this is sufficient, because each List has the same timestamp
            Cell cell=row.get(0);
            long ts=cell.getTimestamp();
            //Rule 1: all data below first insert below MAT was discarded
            Assert.assertFalse("Data below discard point is present!",ts<highBelowMat);

            if(ts<mat){
                //Rule 2: There is no data between the highBelowMat and the mat
                Assert.assertFalse("Data is in the forbidden range ("+highBelowMat+","+mat+")",ts>highBelowMat);
                //Rule 3: only one checkpoint below the mat is possible
                if(ctParser.parseCellType(cell).equals(CellType.CHECKPOINT)){
                    Assert.assertFalse("Too many checkpoints below the mat found!",foundCheckpoint);
                    foundCheckpoint=true;
                }
            }
        }
        Assert.assertTrue("Did not find a checkpoint below the mat!",foundCheckpoint);
    }


    private void assertShouldBeEmptyRow(CompactionTestData correct){
        /*
         * Two valid possibilities:
         *
         * 1. there was a tombstone below the MAT causing the row to be
         * removed
         * 2. the entire data set is rolled back, so the row doesn't actually exist
         */
        if(correct.isRolledBack())
            return; //life is good, we behaved correctly

        List<TxnView> tombstoneTxns=correct.dataMap.get(CellType.TOMBSTONE);
        Assert.assertTrue("There are no tombstone cells, and the row is not rolled back!",
                tombstoneTxns!=null && tombstoneTxns.size()>0);
        Collections.sort(tombstoneTxns,CompactionTestUtils.txnComparator);
        Collections.reverse(tombstoneTxns);

        long highestTombstoneId=tombstoneTxns.get(0).getTxnId();
        for(TxnView n : tombstoneTxns){
            if(n.getEffectiveState()!=Txn.State.ROLLEDBACK){
                highestTombstoneId=n.getTxnId();
                break;
            }
        }
        Assert.assertTrue("All tombstones were rolled back, so row should be present!",highestTombstoneId>0);
        Assert.assertTrue("the Highest tombstone cell is not below the MAT!",highestTombstoneId<correct.mat);

        List<TxnView> userTxns=correct.dataMap.get(CellType.USER_DATA);
        Collections.sort(userTxns,CompactionTestUtils.txnComparator);
        Collections.reverse(userTxns);
        for(TxnView userTxn : userTxns){
            if(userTxn.getEffectiveState()==Txn.State.ROLLEDBACK) continue;
            Assert.assertTrue("User transaction not below mat!",userTxn.getBeginTimestamp()<correct.mat);
            Assert.assertTrue("User cell is not below highest tombstone id!",userTxn.getBeginTimestamp()<highestTombstoneId);
        }
    }

        /* ****************************************************************************************************************/
    /*Data Generators*/

    /*Insertions only*/
    private static List<CompactionTestData> insertCommitted(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView txn1=new CommittedTxn(2l,3l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;

            template=template.insert(txn1);
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    private static List<CompactionTestData> insertSomeActive(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView txn1=new CommittedTxn(2l,3l);
        TxnView activeTxn=new ActiveWriteTxn(4l,4l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            if(i%4==0)
                template=template.insert(activeTxn);
            else
                template=template.insert(txn1);
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    private static List<CompactionTestData> insertSomeRolledBack(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView committedTxn=new CommittedTxn(2l,3l);
        TxnView rolledBackTxn=new RolledBackTxn(4l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            if(i%4==0)
                template=template.insert(rolledBackTxn);
            else
                template=template.insert(committedTxn);
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }


    /*Insert and Update*/
    private static List<CompactionTestData> insertAndUpdateCommitted(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView txn1=new CommittedTxn(2l,3l);
        TxnView updateTxn=new CommittedTxn(5l,6l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            template=template.insert(txn1);
            if(i%3==0)
                template=template.update(updateTxn);
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    private static List<CompactionTestData> insertLotsOfUpdatesCommitted(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView txn1=new CommittedTxn(2l,3l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            template=template.insert(txn1);
            if(i%3==0){
                for(int j=0;j<200;j++){
                    TxnView updateTxn = new CommittedTxn(2*j+5,2*j+6);
                    template=template.update(updateTxn);
                }
            }
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    private static List<CompactionTestData> insertAndUpdateSomeActive(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView committedInsertTxn=new CommittedTxn(1l,2l);
        TxnView activeUserTxn=new CommittedTxn(3l,4l);
        TxnView committedUpdateTxn=new CommittedTxn(5l,6l);
        TxnView activeUpdateTxn=new CommittedTxn(7l,8l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            if(i%3==0){
                template=template.insert(activeUserTxn);
                if(i%2==0) template=template.update(activeUpdateTxn);
            }else{
                template=template.insert(committedInsertTxn);
                if(i%2==0){
                    if(i%4==0)
                        template=template.update(activeUpdateTxn);
                    else
                        template=template.update(committedUpdateTxn);
                }
            }
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    private static List<CompactionTestData> insertAndUpdateSomeRolledBack(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView committedInsertTxn=new CommittedTxn(1l,2l);
        TxnView rolledBackInsertTxn=new RolledBackTxn(3l);
        TxnView committedUpdateTxn=new CommittedTxn(5l,6l);
        TxnView rolledBackUpdateTxn=new RolledBackTxn(7l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            if(i%3==0){
                template=template.insert(rolledBackInsertTxn);
                if(i%2==0) template=template.update(rolledBackUpdateTxn);
            }else{
                template=template.insert(committedInsertTxn);
                if(i%2==0){
                    if(i%4==0)
                        template=template.update(rolledBackUpdateTxn);
                    else
                        template=template.update(committedUpdateTxn);
                }
            }
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    /*Insert and Delete*/
    private static List<CompactionTestData> insertAndDeleteCommitted(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView txn1=new CommittedTxn(2l,3l);
        TxnView deleteTxn=new CommittedTxn(5l,6l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            template=template.insert(txn1);
            if(i%2==0)
                template=template.delete(deleteTxn);
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }


    private static List<CompactionTestData> insertAndDeleteSomeActive(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView committedInsertTxn=new CommittedTxn(1l,2l);
        TxnView activeUserTxn=new ActiveWriteTxn(3l,3l);
        TxnView committedDeleteTxn=new CommittedTxn(5l,6l);
        TxnView activeDeleteTxn=new ActiveWriteTxn(7l,7l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            if(i%3==0){
                template=template.insert(activeUserTxn);
                if(i%2==0) template=template.delete(activeDeleteTxn);
            }else{
                template=template.insert(committedInsertTxn);
                if(i%2==0){
                    if(i%4==0)
                        template=template.delete(activeDeleteTxn);
                    else
                        template=template.delete(committedDeleteTxn);
                }
            }
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    private static List<CompactionTestData> insertAndDeleteSomeRolledBack(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView committedInsertTxn=new CommittedTxn(1l,2l);
        TxnView rolledBackInsertTxn=new RolledBackTxn(3l);
        TxnView committedDeleteTxn=new CommittedTxn(5l,6l);
        TxnView rolledBackDeleteTxn=new RolledBackTxn(7l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            if(i%3==0){
                template=template.insert(rolledBackInsertTxn);
                if(i%2==0) template=template.delete(rolledBackDeleteTxn);
            }else{
                template=template.insert(committedInsertTxn);
                if(i%2==0){
                    if(i%4==0)
                        template=template.delete(rolledBackDeleteTxn);
                    else
                        template=template.delete(committedDeleteTxn);
                }
            }
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    /*Insert Update and Delete*/
    private static List<CompactionTestData> insertUpdateDeleteCommitted(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView txn1=new CommittedTxn(2l,3l);
        TxnView deleteTxn=new CommittedTxn(5l,6l);
        TxnView updateTxn=new CommittedTxn(7l,8l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            template=template.insert(txn1);
            if(i%2==0)
                template=template.delete(deleteTxn);
            else if(i%3==0)
                template=template.update(updateTxn);
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    private static List<CompactionTestData> insertUpdateDeleteSomeActive(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView committedInsertTxn=new CommittedTxn(1l,2l);
        TxnView activeInsertTxn=new ActiveWriteTxn(3l,3l);
        TxnView committedUpdateTxn=new CommittedTxn(5l,6l);
        TxnView activeUpdateTxn=new ActiveWriteTxn(7l,7l);
        TxnView committedDeleteTxn=new CommittedTxn(9l,10l);
        TxnView activeDeleteTxn=new ActiveWriteTxn(11l,11l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            if(i%3==0){
                template=template.insert(activeInsertTxn);
                if(i%2==0)
                    template=template.update(activeUpdateTxn);
                else if(i%5==0)
                    template = template.delete(activeDeleteTxn);
            }else{
                template=template.insert(committedInsertTxn);
                if(i%2==0){
                    if(i%4==0)
                        template = template.update(activeUpdateTxn);
                    else if(i%5==0)
                        template = template.update(committedUpdateTxn);
                    else if(i%7==0)
                        template = template.delete(activeDeleteTxn);
                    else if(i%9==0)
                        template = template.delete(committedDeleteTxn);
                }
            }
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    private static List<CompactionTestData> insertUpdateDeleteSomeRolledBack(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView committedInsertTxn=new CommittedTxn(1l,2l);
        TxnView activeInsertTxn=new RolledBackTxn(3l);
        TxnView committedUpdateTxn=new CommittedTxn(5l,6l);
        TxnView activeUpdateTxn=new RolledBackTxn(7l);
        TxnView committedDeleteTxn=new CommittedTxn(9l,10l);
        TxnView activeDeleteTxn=new RolledBackTxn(11l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            if(i%3==0){
                template=template.insert(activeInsertTxn);
                if(i%2==0)
                    template=template.update(activeUpdateTxn);
                else if(i%5==0)
                    template = template.delete(activeDeleteTxn);
            }else{
                template=template.insert(committedInsertTxn);
                if(i%2==0){
                    if(i%4==0)
                        template = template.update(activeUpdateTxn);
                    else if(i%5==0)
                        template = template.update(committedUpdateTxn);
                    else if(i%7==0)
                        template = template.delete(activeDeleteTxn);
                    else if(i%9==0)
                        template = template.delete(committedDeleteTxn);
                }
            }
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    /*Insert Delete Insert*/
    private static List<CompactionTestData> insertDeleteInsertCommitted(int numRows,CompactionTestData template){
        List<CompactionTestData> newDataSet=new ArrayList<>(numRows);
        TxnView insertTxn1=new CommittedTxn(2l,3l);
        TxnView deleteTxn=new CommittedTxn(5l,6l);
        TxnView insertTxn2=new CommittedTxn(7l,8l);
        for(int i=0;i<numRows;i++){
            template.rowKey=Encoding.encode("row"+i);
            template.numCols=4;
            template=template.insert(insertTxn1);
            if(i%2==0){
                template=template.delete(deleteTxn);
                if(i%3==0)
                    template = template.insert(insertTxn2);
            }
            newDataSet.add(template.copy());
        }
        return newDataSet;
    }

    //test internal scanner implementation
    private class TScanner implements InternalScanner{
        private Iterator<CompactionTestData> rows;
        private Iterator<Cell> currentRow;

        public TScanner(List<CompactionTestData> rows){ this.rows=rows.iterator(); }

        @Override public void close() throws IOException{ }
        @Override public boolean next(List<Cell> results) throws IOException{ return next(results,Integer.MAX_VALUE); }

        @Override
        public boolean next(List<Cell> result,int limit) throws IOException{
            if(currentRow==null){
                if(!rows.hasNext()) return false;
                CompactionTestData td=rows.next();
                currentRow=buildNext(td).iterator();
            }

            if(limit>0){
                while(currentRow.hasNext() && result.size()<limit){
                    result.add(currentRow.next());
                }
            }else{
                while(currentRow.hasNext()){
                    result.add(currentRow.next());
                }
            }
            if(!currentRow.hasNext()){
                currentRow=null;
            }
            return currentRow!=null || rows.hasNext();
        }


        private List<Cell> buildNext(CompactionTestData td) throws IOException{
            List<Cell> c=CompactionTestUtils.buildCheckpointCells(td.dataMap.get(CellType.CHECKPOINT),td.rowKey);
            c.addAll(CompactionTestUtils.buildCommitTimestamps(td.dataMap.get(CellType.COMMIT_TIMESTAMP),td.rowKey));
            c.addAll(CompactionTestUtils.buildTombstoneCells(td.dataMap.get(CellType.TOMBSTONE),
                    td.dataMap.get(CellType.ANTI_TOMBSTONE),td.rowKey));
            c.addAll(CompactionTestUtils.buildUserCells(td.dataMap.get(CellType.CHECKPOINT),
                    td.dataMap.get(CellType.USER_DATA),td.rowKey,td.numCols,td.random));
            return c;
        }
    }
}
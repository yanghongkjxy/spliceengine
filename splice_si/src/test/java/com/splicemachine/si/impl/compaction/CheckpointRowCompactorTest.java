package com.splicemachine.si.impl.compaction;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.*;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 10/23/15
 */
public class CheckpointRowCompactorTest{

    private static final SDataLib dataLib=new TestDataLib();

    /* ****************************************************************************************************************/
    /*Misc. Regression scenario tests*/

    @Test
    public void tombStoneBelowCheckpointAboveMAT() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(2000000l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView txn = new CommittedTxn(3027690l,3027691l);
        txnStore.cache(txn);
        Cell cp = emptyCheckpointCell(txn);
        TxnView tombTxn = new CommittedTxn(1874090l,1874091l);
        txnStore.cache(tombTxn);

        Cell tombCell = tombstoneCell(tombTxn);
        Cell usert = fullValueCell(txn);

        compactor.addCell(cp);
        compactor.addCell(tombCell);
        compactor.addCell(usert);

        List<Cell>data = new ArrayList<>(5);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect number of returned cells!",2,data.size());
        Cell fullCp = fullCheckpointCell(txn);
        Assert.assertEquals("Incorrect checkpoint cell!",fullCp,data.get(0));
//        Cell ctCp = commitTimestampCell(txn);
//        Assert.assertEquals("Incorrect commit timestamp!",ctCp,data.get(1));
        Assert.assertEquals("Incorrect user data!",usert,data.get(1));

        Assert.assertTrue("Still has data!",compactor.isEmpty());
    }

    @Test
    public void tombStoneBelowCheckpointBelowMAT() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(4000000l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView txn = new CommittedTxn(3027690l,3027691l);
        txnStore.cache(txn);
        Cell cp = emptyCheckpointCell(txn);
        TxnView tombTxn = new CommittedTxn(1874090l,1874091l);
        txnStore.cache(tombTxn);

        Cell tombCell = tombstoneCell(tombTxn);
        Cell usert = fullValueCell(txn);

        compactor.addCell(cp);
        compactor.addCell(tombCell);
        compactor.addCell(usert);

        List<Cell>data = new ArrayList<>(5);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect number of returned cells!",2,data.size());
        Cell fullCp = fullCheckpointCell(txn);
        Assert.assertEquals("Incorrect checkpoint cell!",fullCp,data.get(0));
//        Cell ctCp = commitTimestampCell(txn);
//        Assert.assertEquals("Incorrect commit timestamp!",ctCp,data.get(1));
        Assert.assertEquals("Incorrect user data!",usert,data.get(1));

        Assert.assertTrue("Still has data!",compactor.isEmpty());
    }

    /* ****************************************************************************************************************/
    /*Above the MAT tests*/
    @Test
    public void doesNotDeleteUserDataAboveMAT() throws Exception{
        TxnView toFetch = new ActiveWriteTxn(2l,2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell userDataCell=fullValueCell(toFetch);
        compactor.addCell(userDataCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",1,data.size());
        Assert.assertEquals("Incorrect data!",userDataCell,data.get(0));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void doesNotDeleteForeignKeyCounterAboveMAT() throws Exception{
        TxnView toFetch = new ActiveWriteTxn(2l,2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell userDataCell=fkCounterCell();
        compactor.addCell(userDataCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",1,data.size());
        Assert.assertEquals("Incorrect data!",userDataCell,data.get(0));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }


    @Test
    public void doesNotDeleteTombstoneAboveMAT() throws Exception{
        TxnView toFetch = new ActiveWriteTxn(2l,2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell tombstoneCell =tombstoneCell(toFetch);
        compactor.addCell(tombstoneCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",1,data.size());
        Assert.assertEquals("Incorrect data!",tombstoneCell,data.get(0));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }


    @Test
    public void doesNotRemoveAntiTombstoneWithNoCheckpointAboveMAT() throws Exception{
        TxnView toFetch = new ActiveWriteTxn(2l,2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell atCell =antiTombstoneCell(toFetch);
        compactor.addCell(atCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",1,data.size());
        Assert.assertEquals("Incorrect data!",atCell,data.get(0));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    private KeyValue antiTombstoneCell(TxnView toFetch){
        return new KeyValue(Bytes.toBytes("key"),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                toFetch.getTxnId(),
                FixedSIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES
        );
    }

    @Test
    public void doesNotRemoveCommitTimestampAboveMAT() throws Exception{
        TxnView toFetch = new CommittedTxn(2l,3l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell tombstoneCell =commitTimestampCell(toFetch);
        compactor.addCell(tombstoneCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",1,data.size());
        Assert.assertEquals("Incorrect data!",tombstoneCell,data.get(0));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void doesNotRemoveCheckpointAboveMATWithCommitTimestamp() throws Exception{
        TxnView toFetch = new CommittedTxn(2l,3l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell tombstoneCell = new KeyValue(Bytes.toBytes("key"),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,
                2l,
                Bytes.toBytes(3l)
        );
        compactor.addCell(tombstoneCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",1,data.size());
        Assert.assertEquals("Incorrect data!",tombstoneCell,data.get(0));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void removesCheckpointAboveMATWithoutCommitTimestampAndNoOtherData() throws Exception{
        TxnView toFetch = new ActiveWriteTxn(2l,2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell tombstoneCell = new KeyValue(Bytes.toBytes("key"),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,
                2l,
                FixedSIConstants.EMPTY_BYTE_ARRAY
        );
        compactor.addCell(tombstoneCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",0,data.size());
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void removesAntiTombstoneAboveMATWhenCheckpointIsPresent() throws Exception{
        TxnView toFetch = new ActiveWriteTxn(2l,2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell atCell =antiTombstoneCell(toFetch);
        Cell checkpointCell=fullCheckpointCell(toFetch);
        compactor.addCell(checkpointCell);
        compactor.addCell(atCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",1,data.size());
        Assert.assertEquals("Incorrect data!",checkpointCell,data.get(0));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }


    @Test
    public void generatesCommitTimestampForUserDataAboveMAT() throws Exception{
        TxnView toFetch = new CommittedTxn(2l,3l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell dataCell=fullValueCell(toFetch);
        compactor.addCell(dataCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Cell ctCell =commitTimestampCell(toFetch);
        Assert.assertEquals("Incorrect returned number of cells!",2,data.size());
        Assert.assertEquals("Incorrect Commit timestamp!",ctCell,data.get(0));
        Assert.assertEquals("Incorrect user data!",dataCell,data.get(1));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }


    @Test
    public void generatesCommitTimestampAndCheckpointForEmptyCheckpointAndUserDataAboveMAT() throws Exception{
        TxnView toFetch = new CommittedTxn(2l,3l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell dataCell=fullValueCell(toFetch);

        Cell checkpointCell=emptyCheckpointCell(toFetch);
        compactor.addCell(checkpointCell);
        compactor.addCell(dataCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

//        Cell ctCell =commitTimestampCell(toFetch);
        Cell cpCell =fullCheckpointCell(toFetch);
        Assert.assertEquals("Incorrect returned number of cells!",2,data.size());
        Assert.assertEquals("Incorrect Checkpoint!",cpCell,data.get(0));
//        Assert.assertEquals("Incorrect CommitTimestamp!",ctCell,data.get(1));
        Assert.assertEquals("Incorrect user data!",dataCell,data.get(1));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void keepsEmptyCheckpointForActiveUserDataAboveMAT() throws Exception{
        TxnView toFetch = new ActiveWriteTxn(2l,2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell dataCell=fullValueCell(toFetch);

        Cell checkpointCell=emptyCheckpointCell(toFetch);
        compactor.addCell(checkpointCell);
        compactor.addCell(dataCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",2,data.size());
        Assert.assertEquals("Incorrect Checkpoint!",checkpointCell,data.get(0));
        Assert.assertEquals("Incorrect user data!",dataCell,data.get(1));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void keepsCheckpointTombstoneAndUserDataForActiveUserDataAboveMAT() throws Exception{
        TxnView toFetch = new ActiveWriteTxn(2l,2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        TxnView cpTxn=new ActiveWriteTxn(3l,3l);
        txnStore.cache(cpTxn);
        Cell checkpointCell=emptyCheckpointCell(cpTxn);
        Cell tombstoneCell = tombstoneCell(toFetch);
        Cell dataCell=fullValueCell(cpTxn);
        compactor.addCell(checkpointCell);
        compactor.addCell(tombstoneCell);
        compactor.addCell(dataCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",3,data.size());
        Assert.assertEquals("Incorrect Checkpoint!",checkpointCell,data.get(0));
        Assert.assertEquals("Incorrect Tombstone!",tombstoneCell,data.get(1));
        Assert.assertEquals("Incorrect user data!",dataCell,data.get(2));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampToCommittedTombstoneAboveMAT() throws Exception{
        TxnView toFetch = new CommittedTxn(2l,3l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell tombstoneCell = tombstoneCell(toFetch);
        compactor.addCell(tombstoneCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Cell ctCell = commitTimestampCell(toFetch);
        Assert.assertEquals("Incorrect returned number of cells!",2,data.size());
        Assert.assertEquals("Incorrect CommitTimestamp!",ctCell,data.get(0));
        Assert.assertEquals("Incorrect Tombstone!",tombstoneCell,data.get(1));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    /* ****************************************************************************************************************/
    /*Commit timestamp gap tests*/

    @Test
    public void addsCommitTimestampForMultipleUserVersions() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView firstTxn = new CommittedTxn(5l,6l);
        txnStore.cache(firstTxn);
        BitSet bs = new BitSet(); bs.set(0,2);
        Cell firstDataCell = partialValueCell(firstTxn,bs);
        compactor.addCell(firstDataCell);

        TxnView secondTxn = new CommittedTxn(3l,4l);
        txnStore.cache(secondTxn);
        Cell secondDataCell = partialValueCell(secondTxn,bs);
        compactor.addCell(secondDataCell);

        List<Cell> data = new ArrayList<>(2);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",4,data.size());

        Assert.assertEquals("Incorrect first commit timestamp!",commitTimestampCell(firstTxn),data.get(0));
        Assert.assertEquals("Incorrect second commit timestamp!",commitTimestampCell(secondTxn),data.get(1));
        Assert.assertEquals("Incorrect first data cell!",firstDataCell,data.get(2));
        Assert.assertEquals("Incorrect second data cell!",secondDataCell,data.get(3));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampForMultipleUserVersionsWithCommitTimestampAlreadyPresentBefore() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView highCtTxn = new CommittedTxn(7l,8l);
        Cell highCt = commitTimestampCell(highCtTxn);
        txnStore.cache(highCtTxn);
        compactor.addCell(highCt);

        TxnView firstTxn = new CommittedTxn(5l,6l);
        txnStore.cache(firstTxn);
        BitSet bs = new BitSet(); bs.set(0,2);
        Cell firstDataCell = partialValueCell(firstTxn,bs);
        compactor.addCell(firstDataCell);

        TxnView secondTxn = new CommittedTxn(3l,4l);
        txnStore.cache(secondTxn);
        Cell secondDataCell = partialValueCell(secondTxn,bs);
        compactor.addCell(secondDataCell);

        List<Cell> data = new ArrayList<>(2);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",5,data.size());

        Assert.assertEquals("Incorrect high commit timestamp!",highCt,data.get(0));
        Assert.assertEquals("Incorrect first commit timestamp!",commitTimestampCell(firstTxn),data.get(1));
        Assert.assertEquals("Incorrect second commit timestamp!",commitTimestampCell(secondTxn),data.get(2));
        Assert.assertEquals("Incorrect first data cell!",firstDataCell,data.get(3));
        Assert.assertEquals("Incorrect second data cell!",secondDataCell,data.get(4));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampForMultipleUserVersionsWithCommitTimestampAlreadyPresentAfter() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView lowCtTxn = new CommittedTxn(1l,2l);
        Cell highCt = commitTimestampCell(lowCtTxn);
        txnStore.cache(lowCtTxn);
        compactor.addCell(highCt);

        TxnView firstTxn = new CommittedTxn(5l,6l);
        txnStore.cache(firstTxn);
        BitSet bs = new BitSet(); bs.set(0,2);
        Cell firstDataCell = partialValueCell(firstTxn,bs);
        compactor.addCell(firstDataCell);

        TxnView secondTxn = new CommittedTxn(3l,4l);
        txnStore.cache(secondTxn);
        Cell secondDataCell = partialValueCell(secondTxn,bs);
        compactor.addCell(secondDataCell);

        List<Cell> data = new ArrayList<>(2);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",5,data.size());

        Assert.assertEquals("Incorrect first commit timestamp!",commitTimestampCell(firstTxn),data.get(0));
        Assert.assertEquals("Incorrect second commit timestamp!",commitTimestampCell(secondTxn),data.get(1));
        Assert.assertEquals("Incorrect low commit timestamp!",highCt,data.get(2));
        Assert.assertEquals("Incorrect first data cell!",firstDataCell,data.get(3));
        Assert.assertEquals("Incorrect second data cell!",secondDataCell,data.get(4));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampForMultipleUserVersionsWithCommitTimestampAlreadyPresentInBetween() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView lowCtTxn = new CommittedTxn(3l,4l);
        Cell midCt = commitTimestampCell(lowCtTxn);
        txnStore.cache(lowCtTxn);
        compactor.addCell(midCt);

        TxnView firstTxn = new CommittedTxn(5l,6l);
        txnStore.cache(firstTxn);
        BitSet bs = new BitSet(); bs.set(0,2);
        Cell firstTs = partialValueCell(firstTxn,bs);
        compactor.addCell(firstTs);

        TxnView secondTxn = new CommittedTxn(1l,2l);
        txnStore.cache(secondTxn);
        Cell secondTs = partialValueCell(secondTxn,bs);
        compactor.addCell(secondTs);

        List<Cell> data = new ArrayList<>(2);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",5,data.size());

        Assert.assertEquals("Incorrect first commit timestamp!",commitTimestampCell(firstTxn),data.get(0));
        Assert.assertEquals("Incorrect mid commit timestamp!",midCt,data.get(1));
        Assert.assertEquals("Incorrect second commit timestamp!",commitTimestampCell(secondTxn),data.get(2));
        Assert.assertEquals("Incorrect first data cell!",firstTs,data.get(3));
        Assert.assertEquals("Incorrect second data cell!",secondTs,data.get(4));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampForMultipleTombstoneVersions() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView firstTxn = new CommittedTxn(5l,6l);
        txnStore.cache(firstTxn);
        Cell firstTs = tombstoneCell(firstTxn);
        compactor.addCell(firstTs);

        TxnView secondTxn = new CommittedTxn(3l,4l);
        txnStore.cache(secondTxn);
        Cell secondTs = tombstoneCell(secondTxn);
        compactor.addCell(secondTs);

        List<Cell> data = new ArrayList<>(2);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",4,data.size());

        Assert.assertEquals("Incorrect first commit timestamp!",commitTimestampCell(firstTxn),data.get(0));
        Assert.assertEquals("Incorrect second commit timestamp!",commitTimestampCell(secondTxn),data.get(1));
        Assert.assertEquals("Incorrect first tombstone!",firstTs,data.get(2));
        Assert.assertEquals("Incorrect second tombstone!",secondTs,data.get(3));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampForMultipleTombstoneVersionsWithCommitTimestampAlreadyPresentBefore() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView highCtTxn = new CommittedTxn(7l,8l);
        Cell highCt = commitTimestampCell(highCtTxn);
        txnStore.cache(highCtTxn);
        compactor.addCell(highCt);

        TxnView firstTxn = new CommittedTxn(5l,6l);
        txnStore.cache(firstTxn);
        Cell firstTs = tombstoneCell(firstTxn);
        compactor.addCell(firstTs);

        TxnView secondTxn = new CommittedTxn(3l,4l);
        txnStore.cache(secondTxn);
        Cell secondTs = tombstoneCell(secondTxn);
        compactor.addCell(secondTs);

        List<Cell> data = new ArrayList<>(2);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",5,data.size());

        Assert.assertEquals("Incorrect high commit timestamp!",highCt,data.get(0));
        Assert.assertEquals("Incorrect first commit timestamp!",commitTimestampCell(firstTxn),data.get(1));
        Assert.assertEquals("Incorrect second commit timestamp!",commitTimestampCell(secondTxn),data.get(2));
        Assert.assertEquals("Incorrect first tombstone!",firstTs,data.get(3));
        Assert.assertEquals("Incorrect second tombstone!",secondTs,data.get(4));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampForMultipleTombstoneVersionsWithCommitTimestampAlreadyPresentAfter() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView lowCtTxn = new CommittedTxn(1l,2l);
        Cell highCt = commitTimestampCell(lowCtTxn);
        txnStore.cache(lowCtTxn);
        compactor.addCell(highCt);

        TxnView firstTxn = new CommittedTxn(5l,6l);
        txnStore.cache(firstTxn);
        Cell firstTs = tombstoneCell(firstTxn);
        compactor.addCell(firstTs);

        TxnView secondTxn = new CommittedTxn(3l,4l);
        txnStore.cache(secondTxn);
        Cell secondTs = tombstoneCell(secondTxn);
        compactor.addCell(secondTs);

        List<Cell> data = new ArrayList<>(2);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",5,data.size());

        Assert.assertEquals("Incorrect first commit timestamp!",commitTimestampCell(firstTxn),data.get(0));
        Assert.assertEquals("Incorrect second commit timestamp!",commitTimestampCell(secondTxn),data.get(1));
        Assert.assertEquals("Incorrect low commit timestamp!",highCt,data.get(2));
        Assert.assertEquals("Incorrect first tombstone!",firstTs,data.get(3));
        Assert.assertEquals("Incorrect second tombstone!",secondTs,data.get(4));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampForMultipleTombstoneVersionsWithCommitTimestampAlreadyPresentInBetween() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView lowCtTxn = new CommittedTxn(3l,4l);
        Cell midCt = commitTimestampCell(lowCtTxn);
        txnStore.cache(lowCtTxn);
        compactor.addCell(midCt);

        TxnView firstTxn = new CommittedTxn(5l,6l);
        txnStore.cache(firstTxn);
        Cell firstTs = tombstoneCell(firstTxn);
        compactor.addCell(firstTs);

        TxnView secondTxn = new CommittedTxn(1l,2l);
        txnStore.cache(secondTxn);
        Cell secondTs = tombstoneCell(secondTxn);
        compactor.addCell(secondTs);

        List<Cell> data = new ArrayList<>(2);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",5,data.size());

        Assert.assertEquals("Incorrect first commit timestamp!",commitTimestampCell(firstTxn),data.get(0));
        Assert.assertEquals("Incorrect mid commit timestamp!",midCt,data.get(1));
        Assert.assertEquals("Incorrect second commit timestamp!",commitTimestampCell(secondTxn),data.get(2));
        Assert.assertEquals("Incorrect first tombstone!",firstTs,data.get(3));
        Assert.assertEquals("Incorrect second tombstone!",secondTs,data.get(4));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsOnlyOneCommitTimestampWithBothTombstoneAndUserData() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView txn = new CommittedTxn(5l,6l);
        txnStore.cache(txn);
        Cell tombstone = tombstoneCell(txn); //avoid adding the checkpoint cell
        compactor.addCell(tombstone);
        Cell uData = partialValueCell(txn,new BitSet());
        compactor.addCell(uData);

        List<Cell> data = new ArrayList<>(2);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",3,data.size());

        Assert.assertEquals("Incorrect commit timestamp!",commitTimestampCell(txn),data.get(0));
        Assert.assertEquals("Incorrect tombstone!",tombstone,data.get(1));
        Assert.assertEquals("Incorrect user data!",uData,data.get(2));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampsWithTombstoneHigherThanUserData() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView txn = new CommittedTxn(5l,6l);
        txnStore.cache(txn);
        Cell tombstone = tombstoneCell(txn); //avoid adding the checkpoint cell
        compactor.addCell(tombstone);

        TxnView uTxn = new CommittedTxn(3l,4l);
        txnStore.cache(uTxn);
        Cell uData = partialValueCell(uTxn,new BitSet());
        compactor.addCell(uData);

        List<Cell> data = new ArrayList<>(4);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",4,data.size());

        Assert.assertEquals("Incorrect tombstone commit timestamp!",commitTimestampCell(txn),data.get(0));
        Assert.assertEquals("Incorrect uData commit timestamp!",commitTimestampCell(uTxn),data.get(1));
        Assert.assertEquals("Incorrect tombstone!",tombstone,data.get(2));
        Assert.assertEquals("Incorrect user data!",uData,data.get(3));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampsWithTombstoneLowerThanUserData() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView txn = new CommittedTxn(3l,4l);
        txnStore.cache(txn);
        Cell tombstone = tombstoneCell(txn); //avoid adding the checkpoint cell
        compactor.addCell(tombstone);

        TxnView uTxn = new CommittedTxn(5l,6l);
        txnStore.cache(uTxn);
        Cell uData = partialValueCell(uTxn,new BitSet());
        compactor.addCell(uData);

        List<Cell> data = new ArrayList<>(4);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",4,data.size());

        Assert.assertEquals("Incorrect uData commit timestamp!",commitTimestampCell(uTxn),data.get(0));
        Assert.assertEquals("Incorrect tombstone commit timestamp!",commitTimestampCell(txn),data.get(1));
        Assert.assertEquals("Incorrect tombstone!",tombstone,data.get(2));
        Assert.assertEquals("Incorrect user data!",uData,data.get(3));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampsWithTombstoneLowerThanUserDataAndTombstoneCtAlreadyPresent() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView txn = new CommittedTxn(3l,4l);
        txnStore.cache(txn);
        Cell tombstone = tombstoneCell(txn); //avoid adding the checkpoint cell
        Cell tombstoneCt = commitTimestampCell(txn);
        compactor.addCell(tombstoneCt);
        compactor.addCell(tombstone);

        TxnView uTxn = new CommittedTxn(5l,6l);
        txnStore.cache(uTxn);
        Cell uData = partialValueCell(uTxn,new BitSet());
        compactor.addCell(uData);

        List<Cell> data = new ArrayList<>(4);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",4,data.size());

        Assert.assertEquals("Incorrect uData commit timestamp!",commitTimestampCell(uTxn),data.get(0));
        Assert.assertEquals("Incorrect tombstone commit timestamp!",tombstoneCt,data.get(1));
        Assert.assertEquals("Incorrect tombstone!",tombstone,data.get(2));
        Assert.assertEquals("Incorrect user data!",uData,data.get(3));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void addsCommitTimestampsWithTombstoneLowerThanUserDataAndUserCtAlreadyPresent() throws Exception{
        TxnSupplier txnStore = new TestSupplier();
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l, DefaultCellTypeParser.INSTANCE, accumulator, txnStore);

        TxnView txn = new CommittedTxn(3l,4l);
        TxnView uTxn = new CommittedTxn(5l,6l);
        txnStore.cache(txn);
        txnStore.cache(uTxn);

        Cell userCt = commitTimestampCell(uTxn);
        Cell tombstone = tombstoneCell(txn); //avoid adding the checkpoint cell
        compactor.addCell(userCt);
        compactor.addCell(tombstone);

        Cell uData = partialValueCell(uTxn,new BitSet());
        compactor.addCell(uData);

        List<Cell> data = new ArrayList<>(4);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",4,data.size());

        Assert.assertEquals("Incorrect uData commit timestamp!",userCt,data.get(0));
        Assert.assertEquals("Incorrect tombstone commit timestamp!",commitTimestampCell(txn),data.get(1));
        Assert.assertEquals("Incorrect tombstone!",tombstone,data.get(2));
        Assert.assertEquals("Incorrect user data!",uData,data.get(3));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    /* ****************************************************************************************************************/
    /*rolled back tests*/

    @Test
    public void discardsRolledbackCheckpoints() throws Exception{
        TxnView toFetch = new RolledBackTxn(2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell userData = emptyCheckpointCell(toFetch);
        compactor.addCell(userData);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Too many rows!",0,data.size());
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void discardsRolledbackUserData() throws Exception{
        TxnView toFetch = new RolledBackTxn(2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell userData = fullValueCell(toFetch);
        compactor.addCell(userData);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Too many rows!",0,data.size());
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void discardsRolledbackTombstone() throws Exception{
        TxnView toFetch = new RolledBackTxn(2l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(1l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell userData = tombstoneCell(toFetch);
        compactor.addCell(userData);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Too many rows!",0,data.size());
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    /* ****************************************************************************************************************/
    /*Below MAT discard point tests*/

    @Test
    public void discardsUserDataBelowFirstCheckpointBelowMAT() throws Exception{
        /*
         * Check that any data below the highest checkpoint < MAT will be discarded
         */
        TxnView toFetch = new CommittedTxn(3l,4l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(5l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell cpCell = fullCheckpointCell(toFetch);
        compactor.addCell(cpCell);

        Cell userCell = fullValueCell(new CommittedTxn(1l,2l));
        compactor.addCell(userCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);
        Assert.assertEquals("Incorrect number of returned cells!",1,data.size());
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void discardsCommitTimestampsBelowFirstCheckpointBelowMAT() throws Exception{
        /*
         * Check that any data below the highest checkpoint < MAT will be discarded
         */
        TxnView toFetch = new CommittedTxn(3l,4l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(5l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell cpCell = fullCheckpointCell(toFetch);
        compactor.addCell(cpCell);

        Cell userCell = commitTimestampCell(new CommittedTxn(1l,2l));
        compactor.addCell(userCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);
        Assert.assertEquals("Incorrect number of returned cells!",1,data.size());
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void discardsTombstonesBelowFirstCheckpointBelowMAT() throws Exception{
        /*
         * Check that any data below the highest checkpoint < MAT will be discarded
         */
        TxnView toFetch = new CommittedTxn(3l,4l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(5l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell cpCell = fullCheckpointCell(toFetch);
        compactor.addCell(cpCell);

        Cell userCell = tombstoneCell(new CommittedTxn(1l,2l));
        compactor.addCell(userCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);
        Assert.assertEquals("Incorrect number of returned cells!",1,data.size());
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void discardsAntiTombstonesBelowFirstCheckpointBelowMAT() throws Exception{
        /*
         * Check that any data below the highest checkpoint < MAT will be discarded
         */
        TxnView toFetch = new CommittedTxn(3l,4l);
        TxnSupplier txnStore = new TestSupplier(toFetch);
        RowAccumulator<Cell> accumulator = new NoopAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(5l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell cpCell = fullCheckpointCell(toFetch);
        compactor.addCell(cpCell);

        Cell userCell = antiTombstoneCell(new CommittedTxn(1l,2l));
        compactor.addCell(userCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);
        Assert.assertEquals("Incorrect number of returned cells!",1,data.size());
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void movesDiscardPointUpOnTombstoneBelowMAT() throws Exception{
        TxnView tombstoneTxn = new CommittedTxn(5l,6l);
        TxnSupplier txnStore = new TestSupplier(tombstoneTxn);
        RowAccumulator<Cell> accumulator = new FailAccumulator();
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(10l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        TxnView insertTxn=new CommittedTxn(1l,2l);
        txnStore.cache(insertTxn);
        Cell checkpointCell=fullCheckpointCell(insertTxn);
        Cell tombstoneCell = tombstoneCell(tombstoneTxn);
        TxnView middleTxn = new CommittedTxn(3l,4l);
        txnStore.cache(middleTxn);
        Cell dataCell=fullValueCell(middleTxn);
        compactor.addCell(checkpointCell);
        compactor.addCell(tombstoneCell);
        compactor.addCell(dataCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);

        Assert.assertEquals("Incorrect returned number of cells!",0,data.size());
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void keepsUserDataAtDiscardPointBelowMAT() throws Exception{
        /*
         * Check that any data below the highest checkpoint < MAT will be discarded
         */
        TxnView toFetch = new CommittedTxn(3l,4l);
        TxnSupplier txnStore = new TestSupplier(toFetch);

        RowAccumulator<Cell> accumulator =new HRowAccumulator<>(dataLib,
                EntryPredicateFilter.EMPTY_PREDICATE, new EntryDecoder(),false);
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(5l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        Cell cpCell = fullCheckpointCell(toFetch);
        compactor.addCell(cpCell);

        Cell userCell = fullValueCell(toFetch);
        compactor.addCell(userCell);

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);
        Assert.assertEquals("Incorrect number of returned cells!",2,data.size());
        Assert.assertEquals("Incorrect Checkpoint cell!",cpCell,data.get(0));
//        Cell ctCell = commitTimestampCell(toFetch);
//        Assert.assertEquals("Incorrect CommitTimestamp cell!",ctCell,data.get(1));
        Assert.assertEquals("Incorrect User cell!",userCell,data.get(1));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }

    @Test
    public void accumulatesUserDataBelowMAT() throws Exception{
        /*
         * Check that any data below the highest checkpoint < MAT will be discarded
         */
        TxnView toFetch = new CommittedTxn(3l,4l);
        TxnSupplier txnStore = new TestSupplier(toFetch);

        RowAccumulator<Cell> accumulator =new HRowAccumulator<>(dataLib,
                EntryPredicateFilter.EMPTY_PREDICATE, new EntryDecoder(),false);
        CheckpointRowCompactor compactor = new CheckpointRowCompactor(5l,
                DefaultCellTypeParser.INSTANCE,
                accumulator,
                txnStore);

        BitSet topField = new BitSet();
        topField.set(1);
        Cell userCell = partialValueCell(toFetch,topField);
        compactor.addCell(userCell);

        TxnView lower = new CommittedTxn(1l,2l);
        txnStore.cache(lower);

        BitSet lowField = new BitSet();
        topField.set(0);
        userCell = partialValueCell(toFetch,lowField);
        compactor.addCell(userCell);

        Assert.assertFalse("Should not be empty!",compactor.isEmpty());

        List<Cell> data = new ArrayList<>(1);
        compactor.reverse();
        compactor.placeCells(data,10);
        Assert.assertEquals("Incorrect number of returned cells!",3,data.size());
        Cell cpCell = fullCheckpointCell(toFetch);
        Assert.assertEquals("Incorrect Checkpoint cell!",cpCell,data.get(0));
        Cell ctCell = commitTimestampCell(toFetch);
        Assert.assertEquals("Incorrect CommitTimestamp cell!",ctCell,data.get(1));
        userCell = fullValueCell(toFetch);
        Assert.assertEquals("Incorrect User cell!",userCell,data.get(2));
        Assert.assertTrue("Does not report empty!",compactor.isEmpty());
    }


    /* ***************************************************************************************************************/
    /*private helper methods*/
    private class NoopAccumulator implements RowAccumulator<Cell>{
        @Override public boolean isOfInterest(Cell value){ return false; }
        @Override public boolean accumulate(Cell value) throws IOException{ return false; }
        @Override public boolean isFinished(){ return true; }
        @Override public boolean hasAccumulated(){ return false; }
        @Override public byte[] result(){ return new byte[0]; }
        @Override public long getBytesVisited(){ return 0; }
        @Override public boolean isCountStar(){ return false; }
        @Override public void reset(){ }
        @Override public EntryAccumulator getEntryAccumulator(){ return null; }
        @Override public void close() throws IOException{ }
    }

    private class FailAccumulator implements RowAccumulator<Cell>{
        @Override public boolean isOfInterest(Cell value){
            Assert.fail("Should not even try accumulating!");
            return false;
        }
        @Override public boolean accumulate(Cell value) throws IOException{
            Assert.fail("Should not accumulate!");
            return false;
        }
        @Override public boolean isFinished(){ return true; }
        @Override public boolean hasAccumulated(){ return false; }
        @Override public byte[] result(){ return new byte[0]; }
        @Override public long getBytesVisited(){ return 0; }
        @Override public boolean isCountStar(){ return false; }
        @Override public void reset(){ }
        @Override public EntryAccumulator getEntryAccumulator(){ return null; }
        @Override public void close() throws IOException{ }
    }

    private class TestSupplier implements TxnSupplier{
        private Map<Long,TxnView> data;

        public TestSupplier(){
            this.data = new HashMap<>();
        }
        public TestSupplier(TxnView toFetch){
            this();
            data.put(toFetch.getTxnId(),toFetch);
        }

        @Override
        public TxnView getTransaction(long txnId) throws IOException{
            return getTransactionFromCache(txnId);
        }

        @Override
        public TxnView getTransaction(long txnId,boolean getDestinationTables) throws IOException{
            return getTransaction(txnId);
        }

        @Override
        public boolean transactionCached(long txnId){
            return getTransactionFromCache(txnId)!=null;
        }

        @Override
        public void cache(TxnView toCache){
            data.put(toCache.getTxnId(),toCache);
        }

        @Override
        public TxnView getTransactionFromCache(long txnId){
            return data.get(txnId);
        }
    }

    private KeyValue commitTimestampCell(TxnView committedTxn){
        return new KeyValue(Bytes.toBytes("key"),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                committedTxn.getTxnId(),
                Bytes.toBytes(committedTxn.getGlobalCommitTimestamp())
        );
    }

    private Cell fullValueCell(TxnView txn) throws IOException{
        BitSet nonNullFields = new BitSet(2);
        nonNullFields.set(0,2);
        return partialValueCell(txn,nonNullFields);
    }

    private Cell partialValueCell(TxnView txn,BitSet occupiedFields) throws IOException{
        EntryEncoder ee = EntryEncoder.create(new KryoPool(10),3,occupiedFields,null,null,null);
        MultiFieldEncoder entryEncoder=ee.getEntryEncoder();
        for(int i=occupiedFields.nextSetBit(0);i>=0;i=occupiedFields.nextSetBit(i+1)){
            entryEncoder.encodeNext("field"+i);
        }
        return new KeyValue(Bytes.toBytes("key"),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSpliceConstants.PACKED_COLUMN_BYTES,
                txn.getTxnId(),
                ee.encode()
        );
    }

    private Cell fullCheckpointCell(TxnView toFetch){
        return new KeyValue(Bytes.toBytes("key"),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,
                toFetch.getTxnId(),
                Bytes.toBytes(toFetch.getGlobalCommitTimestamp())
        );
    }

    private Cell emptyCheckpointCell(TxnView toFetch){
        return new KeyValue(Bytes.toBytes("key"),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES,
                toFetch.getTxnId(),
                FixedSIConstants.EMPTY_BYTE_ARRAY
        );
    }

    private KeyValue tombstoneCell(TxnView toFetch){
        return new KeyValue(Bytes.toBytes("key"),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                toFetch.getTxnId(),
                FixedSIConstants.EMPTY_BYTE_ARRAY
        );
    }

    private Cell fkCounterCell(){
        return new KeyValue(Bytes.toBytes("key"),
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,
                Bytes.toBytes(1l)
        );
    }
}
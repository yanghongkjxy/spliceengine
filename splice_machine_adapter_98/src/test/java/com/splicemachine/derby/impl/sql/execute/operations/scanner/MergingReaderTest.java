package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.tools.IteratorRegionScanner;
import com.splicemachine.derby.tools.TestDataEncoder;
import com.splicemachine.derby.tools.TestRowEncoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.V2SerializerMap;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.KeyValueType;
import com.splicemachine.storage.*;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 9/24/15
 */
public class MergingReaderTest{
    private static final KryoPool kp = new KryoPool(1);
    private static final V2SerializerMap serializers = new V2SerializerMap(true,kp);
    private static final SDataLib<Cell,Put,Delete,Get,Scan> dataLib= new HDataLib();

    @Test
    public void noRowsWhenEmptyAlwaysAccept() throws Exception{
        List<List<Cell>> rows = Collections.emptyList();

        RowAccumulator<Cell> accumulator=alwaysAcceptAccumulator();
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        Assert.assertFalse("Read success when it shouldn't have!",reader.readNext());
        Assert.assertFalse("Should not have accumulated!",accumulator.hasAccumulated());
    }

    @Test
    public void noRowsWhenEmptySparse() throws Exception{
        List<List<Cell>> rows = Collections.emptyList();

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        Assert.assertFalse("Read success when it shouldn't have!",reader.readNext());
        Assert.assertFalse("Should not be finished!",accumulator.isFinished());
        Assert.assertFalse("Should not have accumulated!",accumulator.hasAccumulated());
    }

    @Test
    public void singleRowKeyedNonFullBatchInsertThenDelete() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has no updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        int[] pks = new int[]{1,0};
        int[] keyColumnTypes = new int[pks.length];
        for(int i=0;i<pks.length;i++){
            keyColumnTypes[i] = row.getRowArray()[pks[i]].getTypeFormatId();
        }
        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(pks,row,kp);
        TestRowEncoder encoder=tde.encoder();
        encoder.insert(1,row);
        encoder.delete(2);

        List<List<Cell>> rows = buildRowResults(10,tde);

        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);

        EntryPredicateFilter predicateFilter=new EntryPredicateFilter(bs,new ObjectArrayList<Predicate>());
        RowAccumulator<Cell> accumulator =  new HRowAccumulator<>(dataLib, predicateFilter,new EntryDecoder(),false);
        MergingReader<Cell> reader=buildReader(rows,accumulator,predicateFilter,pks,keyColumnTypes,new V2SerializerMap(true,kp));

        //check the always accept accumulator
        assertCorrectReads(row,tde,accumulator,reader);

        predicateFilter = new EntryPredicateFilter(null,new ObjectArrayList<Predicate>());
        accumulator =new HRowAccumulator<>(dataLib, predicateFilter,new EntryDecoder(),false);
        reader = buildReader(rows,accumulator,predicateFilter,pks,keyColumnTypes,new V2SerializerMap(true,kp));
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyNonFullBatch() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has no updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2,new SQLBoolean(true));
        row.setColumn(3,new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        tde.encoder().insert(1,row);

        List<List<Cell>> rows = buildRowResults(10,tde);

        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        //check the always accept accumulator
        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyNonFullBatchAlwaysAccept() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has no updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        tde.encoder().insert(1,row);

        List<List<Cell>> rows = buildRowResults(10,tde);

        //check the always accept accumulator
        RowAccumulator<Cell> accumulator = alwaysAcceptAccumulator();
        MergingReader<Cell> reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyNonFullBatchWithUpdate() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has one updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row);
        row.resetRowArray();
        row.getColumn(3).setValue("update");
        encoder.update(2,row,new int[]{-1,-1,2});

        List<List<Cell>> rows = buildRowResults(10,tde);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }
    @Test
    public void singleRowNoKeyNonFullBatchWithNullUpdate() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has one updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row);
        row.resetRowArray();
        row.getColumn(3).setToNull();
        int[] explicitNulls = new int[]{-1,-1,2};
        encoder.update(2,row,explicitNulls);

        List<List<Cell>> rows = buildRowResults(10,tde);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyNonFullBatchInsertThenDelete() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has one updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row);
        encoder.delete(4);

        List<List<Cell>> rows = buildRowResults(10,tde);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyNonFullBatchInsertDeleteInsert() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has one updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row);
        encoder.delete(4);
        encoder.insert(5,row);

        List<List<Cell>> rows = buildRowResults(10,tde);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyNonFullBatchUpdateThenDelete() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has one updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row);
        row.resetRowArray();
        row.getColumn(3).setValue("update");
        encoder.update(2,row);
        encoder.delete(4);

        List<List<Cell>> rows = buildRowResults(10,tde);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyFullBatch() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. exactly fills a batch
         * B. Has no keyed columns
         * C. Has the same number of updates as a full batch
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row);
        int batchSize = 5;
        row.resetRowArray();
        for(int i=1;i<=batchSize-1;i++){
            row.getColumn(3).setValue("update"+i); encoder.update(i+1,row,new int[]{-1,-1,2});
        }

        List<List<Cell>> rows = buildRowResults(batchSize,tde);
        Assume.assumeTrue("Incorrect row size for test",rows.size()==1);
        Assume.assumeTrue("Incorrect Single size for test",rows.get(0).size()==batchSize);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyMultipleBatches() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. runs over multiple batches
         * B. Has no keyed columns
         * C. Has the same number of updates as a full batch
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row);
        int batchSize = 5;
        row.resetRowArray();
        for(int i=1;i<=2*batchSize;i++){
            row.getColumn(3).setValue("update"+i); encoder.update(i+1,row,new int[]{-1,-1,2});
        }

        List<List<Cell>> rows = buildRowResults(batchSize,tde);
        Assume.assumeTrue("Incorrect row size for test",rows.size()==3);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyMultipleBatchesReturnEarly() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. runs over multiple batches
         * B. Has no keyed columns
         * C. Has the same number of updates as a full batch
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row);
        int batchSize = 5;
        row.resetRowArray();
        for(int i=1;i<=2*batchSize;i++){
            row.getColumn(3).setValue("update"+i); encoder.update(i+1,row,new int[]{-1,-1,2});
        }

        List<List<Cell>> rows = buildRowResults(batchSize,tde);
        Assume.assumeTrue("Incorrect row size for test",rows.size()==3);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(2);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader,bs);
    }

    @Test
    public void singleRowNoKeyMultipleBatchesWithCommitTs() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. runs over multiple batches
         * B. Has no keyed columns
         * C. Has the same number of updates as a full batch
         * D. has some commit timestamps
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row).commit(1,2);
        int batchSize = 5;
        row.resetRowArray();
        for(int i=1;i<=2*batchSize;i++){
            row.getColumn(3).setValue("update"+i); encoder.update(2*i+1,row,new int[]{-1,-1,2});
            if(i%2==0)
                encoder.commit(2*i+1,2*i+2);
        }

        List<List<Cell>> rows = buildRowResults(batchSize,tde);
        Assume.assumeTrue("Incorrect row size for test",rows.size()==4);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyNonFullBatchWithUpdateAndCommitTimestamps() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has one update
         * D. has a commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row).commit(1,2);
        row.resetRowArray();
        row.getColumn(3).setValue("update");
        encoder.update(3,row,new int[]{-1,-1,2}).commit(3,4);

        List<List<Cell>> rows = buildRowResults(10,tde);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);

        //check the always accept accumulator
        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void singleRowNoKeyNonFullBatchWithCommitTs() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has no updates
         * D. has a commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        tde.encoder().insert(1,row).commit(1,2);

        List<List<Cell>> rows = buildRowResults(10,tde);

        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);

        assertCorrectReads(row,tde,accumulator,reader);

        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void multipleRowsNoKeyNonFullBatch() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has no updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row); encoder.reset();
        row.getColumn(1).setValue(2); row.getColumn(3).setValue("hello2");
        encoder.insert(1,row); encoder.reset();

        List<List<Cell>> rows = buildRowResults(10,tde);
        Assume.assumeTrue("Incorrect row result size for test",rows.size()==2);

        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);

        //check the always accept accumulator
        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void multipleRowsNoKeyNonFullBatchDeleteFirst() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has no updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row).delete(2); encoder.reset();
        row.getColumn(1).setValue(2); row.getColumn(3).setValue("hello2");
        encoder.insert(1,row); encoder.reset();

        List<List<Cell>> rows = buildRowResults(10,tde);
        Assume.assumeTrue("Incorrect row result size for test",rows.size()==2);

        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);

        //check the always accept accumulator
        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void multipleRowsNoKeyNonFullBatchNullEntry() throws Exception{
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row); encoder.reset();
        row.getColumn(1).setToNull(); row.getColumn(3).setValue("hello2");
        encoder.insert(1,row); encoder.reset();

        List<List<Cell>> rows = buildRowResults(10,tde);
        Assume.assumeTrue("Incorrect row result size for test",rows.size()==2);

        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);

        //check the always accept accumulator
        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void multipleRowsNoKeyNonFullBatchNullEntryWithNonNullFilter() throws Exception{
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        Collection<ExecRow> expectedData = new ArrayList<>(1);
        expectedData.add(row.getClone());
        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row); encoder.reset();
        row.getColumn(1).setToNull(); row.getColumn(3).setValue("hello2");
        encoder.insert(1,row); encoder.reset();

        List<List<Cell>> rows = buildRowResults(10,tde);
        Assume.assumeTrue("Incorrect row result size for test",rows.size()==2);

        Predicate nonNullP = new NullPredicate(true,false,0,false,false);
        ObjectArrayList<Predicate> oal = new ObjectArrayList<>();
        oal.add(nonNullP);
        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator = new HRowAccumulator<>(dataLib, new EntryPredicateFilter(bs,oal),new EntryDecoder(),false);
        MergingReader<Cell> reader=buildReader(rows,accumulator);
        assertCorrectReads(row,accumulator,reader,expectedData);

        //check the always accept accumulator
        accumulator = new HRowAccumulator<>(dataLib, new EntryPredicateFilter(null,oal),new EntryDecoder(),false);
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,accumulator,reader,expectedData);
    }

    @Test
    public void multipleRowsNoKeyNonFullBatchOneNull() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has no updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row); encoder.reset();
        row.getColumn(1).setToNull(); row.getColumn(3).setValue("hello2");
        encoder.insert(1,row); encoder.reset();

        List<List<Cell>> rows = buildRowResults(10,tde);
        Assume.assumeTrue("Incorrect row result size for test",rows.size()==2);

        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);

        //check the always accept accumulator
        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void multipleRowsNoKeyNonFullBatchOneNullReadOneColOnly() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has no updates
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row); encoder.reset();
        row.resetRowArray();
        row.getColumn(1).setToNull(); row.getColumn(3).setValue("hello2");
        encoder.insert(1,row); encoder.reset();

        List<List<Cell>> rows = buildRowResults(10,tde);
        Assume.assumeTrue("Incorrect row result size for test",rows.size()==2);

        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader,bs);
    }

    @Test
    public void multipleRowsNoKeyNonFullBatchWithUpdate() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has an update on one row
         * D. has no commit timestamp
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row);
        row.resetRowArray();
        row.getColumn(3).setValue("update");
        encoder.update(2,row,new int[]{-1,-1,2});
        encoder.reset();

        row.getColumn(1).setValue(2); row.getColumn(3).setValue("hello2");
        encoder.insert(1,row); encoder.reset();


        List<List<Cell>> rows = buildRowResults(10,tde);
        Assume.assumeTrue("Incorrect row result size for test",rows.size()==2);

        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);

        //now repeat the read with an always accept accumulator
        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }

    @Test
    public void multipleRowsNoKeyNonFullBatchWithUpdateAndCommitTs() throws Exception{
        /*
         * Tests that we properly read a row which
         * A. does not fill a batch
         * B. Has no keyed columns
         * C. Has an update on one row
         * D. has commit timestamps
         */
        ExecRow row = new ValueRow(3);
        row.setColumn(1, new SQLInteger(1));
        row.setColumn(2, new SQLBoolean(true));
        row.setColumn(3, new SQLVarchar("hello"));

        TestDataEncoder tde = new TestDataEncoder().primaryKeyMap(null,row,kp);
        TestRowEncoder encoder=tde.encoder().insert(1,row).commit(1,2);
        row.resetRowArray();
        row.getColumn(3).setValue("update");
        encoder.update(3,row,new int[]{-1,-1,2}).commit(3,4);
        encoder.reset();

        row.getColumn(1).setValue(2); row.getColumn(3).setValue("hello2");
        encoder.insert(1,row).commit(1,2); encoder.reset();


        List<List<Cell>> rows = buildRowResults(10,tde);
        Assume.assumeTrue("Incorrect row result size for test",rows.size()==2);

        //check the sparse accumulator
        com.carrotsearch.hppc.BitSet bs = new BitSet(2);
        bs.set(0,3);
        RowAccumulator<Cell> accumulator=sparseAccumulator(bs);
        MergingReader<Cell> reader=buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);

        //now repeat the read with an always accept accumulator
        accumulator = alwaysAcceptAccumulator();
        reader = buildReader(rows,accumulator);
        assertCorrectReads(row,tde,accumulator,reader);
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/
    private MergingReader<Cell> buildReader(List<List<Cell>> rows,RowAccumulator<Cell> accumulator) throws IOException{
        return buildReader(rows,accumulator,null,null,null,null);
    }

    private MergingReader<Cell> buildReader(List<List<Cell>> rows,
                                            final RowAccumulator<Cell> accumulator,
                                            final EntryPredicateFilter epf,
                                            final int[] keyColumns,
                                            int[] keyColumnTypes,
                                            TypeProvider typeProvider ) throws IOException{
        SIFilter<Cell> siFilter = buildSIFilter(accumulator);
        Supplier<SIFilter<Cell>> sup =Suppliers.ofInstance(siFilter);
        MergingReader.RowKeyFilter keyFilter;
        if(keyColumns!=null){
            final MultiFieldDecoder keyDecoder= MultiFieldDecoder.create();
            final KeyIndex keyIndex = new KeyIndex(keyColumns,keyColumnTypes,typeProvider);
            final Supplier<MultiFieldDecoder> fieldSup = Suppliers.ofInstance(keyDecoder);
            final EntryAccumulator dAccum = accumulator.getEntryAccumulator();
            final EntryAccumulator eAccum = new EntryAccumulator(){
                @Override
                public void add(int position,byte[] data,int offset,int length){
                    dAccum.add(keyColumns[position],data,offset,length);
                }

                @Override
                public void addScalar(int position,byte[] data,int offset,int length){
                    dAccum.addScalar(keyColumns[position],data,offset,length);
                }

                @Override
                public void addFloat(int position,byte[] data,int offset,int length){
                    dAccum.addFloat(keyColumns[position],data,offset,length);
                }

                @Override
                public void addDouble(int position,byte[] data,int offset,int length){
                    dAccum.addDouble(keyColumns[position],data,offset,length);
                }

                @Override public boolean checkFilterAfter(){ return dAccum.checkFilterAfter(); }

                @Override public BitSet getRemainingFields(){ return dAccum.getRemainingFields(); }
                @Override public boolean isFinished(){ return dAccum.isFinished(); }
                @Override public byte[] finish(){ return dAccum.finish(); }
                @Override public void reset(){ dAccum.finish(); }
                @Override public void complete(){ dAccum.complete(); }
                @Override public boolean hasAccumulated(){ return dAccum.hasAccumulated(); }

                @Override
                public boolean isInteresting(BitIndex potentialIndex){
                    return dAccum.isInteresting(potentialIndex);
                }
            };
            keyFilter = new MergingReader.RowKeyFilter(){
                @Override
                public boolean filter(ByteSlice rowKey) throws IOException{
                    keyIndex.reset();
                    keyDecoder.set(rowKey.array(),rowKey.offset(),rowKey.length());
                    return epf.match(keyIndex,fieldSup,eAccum);
                }

                @Override public boolean applied(){ return true; }
                @Override public void reset(){ keyIndex.reset(); }
            };

        }else{
            keyFilter =MergingReader.RowKeyFilter.NOOPFilter;
        }
        return new MergingReader<>(accumulator,
                buildScanner(rows),
                dataLib,
                keyFilter,
                sup,
                Metrics.noOpMetricFactory());
    }

    private void assertCorrectReads(ExecRow row,
                                    TestDataEncoder tde,
                                    RowAccumulator<Cell> accumulator,
                                    MergingReader<Cell> reader) throws Exception{
        assertCorrectReads(row, tde, accumulator, reader,null);
    }

    private void assertCorrectReads(ExecRow row,
                                    TestDataEncoder tde,
                                    RowAccumulator<Cell> accumulator,
                                    MergingReader<Cell> reader,
                                    com.carrotsearch.hppc.BitSet setCols) throws Exception{
        assertCorrectReads(row,accumulator,reader,setCols,tde.expectedData());
    }

    private void assertCorrectReads(ExecRow row,
                                    RowAccumulator<Cell> accumulator,
                                    MergingReader<Cell> reader,
                                    Collection<ExecRow> expectedData) throws Exception{
        assertCorrectReads(row,accumulator,reader,null,expectedData);
    }

    private void assertCorrectReads(ExecRow row,
                                    RowAccumulator<Cell> accumulator,
                                    MergingReader<Cell> reader,
                                    com.carrotsearch.hppc.BitSet setCols,
                                    Collection<ExecRow> expectedData) throws Exception{
        KeyHashDecoder khd =new EntryDataHash(null,null,kp,serializers.getSerializers(row)).getDecoder();
        Collection<ExecRow> decoded = readAndDecode(accumulator,reader,row.getClone(),khd);
        assertCollectionsMatch("Incorrect decoded data!",expectedData,decoded,setCols);

        Assert.assertFalse("Read success when it shouldn't have!",reader.readNext());
        Assert.assertFalse("Should not be finished!",accumulator.isFinished());
        Assert.assertFalse("Should not have accumulated!",accumulator.hasAccumulated());

    }

    private void assertCollectionsMatch(String errorPrefix,
                                        Collection<ExecRow> expectedData,
                                        Collection<ExecRow> decoded,
                                        com.carrotsearch.hppc.BitSet setCols){
        Assert.assertEquals(errorPrefix+" Lengths do not match",expectedData.size(),decoded.size());
        Iterator<ExecRow> expectedIter = expectedData.iterator();
        Iterator<ExecRow> actualIter = decoded.iterator();
        int c = 0;
        while(expectedIter.hasNext()){
            Assert.assertTrue(errorPrefix+"Incorrect iteration at count "+ c+"!",actualIter.hasNext());
            ExecRow expected = expectedIter.next();
            ExecRow actual = actualIter.next();
            if(setCols!=null){
                DataValueDescriptor[] expectedCols=expected.getRowArray();
                DataValueDescriptor[] actualCols=actual.getRowArray();
                for(int i=setCols.nextSetBit(0);i>=0;i=setCols.nextSetBit(i+1)){
                    DataValueDescriptor eDvd=expectedCols[i];
                    DataValueDescriptor aDvd=actualCols[i];
                    Assert.assertEquals(errorPrefix+" Incorrect at row ("+c+"),col("+i+"):",eDvd,aDvd);
                }
            }else{
                Assert.assertEquals(errorPrefix+" Incorrect at row ("+c+"):",expected,actual);
            }

            c++;
        }
        Assert.assertFalse(errorPrefix+"Actual iterator still has some left!",actualIter.hasNext());
    }

    private Collection<ExecRow> readAndDecode(RowAccumulator<Cell> accumulator,
                                              MergingReader reader,
                                              ExecRow outputTemplate,
                                              KeyHashDecoder decoder) throws Exception{
        Collection<ExecRow> rows = new LinkedList<>();
        while(reader.readNext()){
            outputTemplate.resetRowArray();
            byte[] accumulated = accumulator.result();
            decoder.set(accumulated,0,accumulated.length);
            decoder.decode(outputTemplate);
            rows.add(outputTemplate.getClone());
        }
        return rows;
    }

    private RowAccumulator<Cell> alwaysAcceptAccumulator(){
        return new HRowAccumulator<>(dataLib,
                EntryPredicateFilter.EMPTY_PREDICATE,new EntryDecoder(),false);
    }

    private RowAccumulator<Cell> sparseAccumulator(com.carrotsearch.hppc.BitSet occupiedFields){
        return new HRowAccumulator<>(dataLib,
                new EntryPredicateFilter(occupiedFields,new ObjectArrayList<Predicate>()),new EntryDecoder(),false);
    }

    private List<List<Cell>> buildRowResults(int batchSize,TestDataEncoder tde){
        Map<byte[], List<Cell>> listMap=tde.encodedData();
        List<List<Cell>> data = new ArrayList<>(listMap.size());
        for(List<Cell> values : listMap.values()){
            List<List<Cell>> partitions =Lists.partition(values,batchSize);
            data.addAll(partitions);
        }
        return data;
    }

    private SIFilter<Cell> buildSIFilter(final RowAccumulator<Cell> accumulator){
        final Set<Long> tombstoneTimestamps = new TreeSet<>();
        final Set<Long> antiTombstoneTimestamps = new TreeSet<>();
        return new SIFilter<Cell>(){
            @Override public void nextRow(){
                accumulator.reset();
                tombstoneTimestamps.clear();
            }
            @Override public RowAccumulator getAccumulator(){ return accumulator; }

            @Override
            public Filter.ReturnCode filterKeyValue(Cell kv) throws IOException{
                KeyValueType type=getKeyValueType(kv);
                if(type==KeyValueType.COMMIT_TIMESTAMP){
                    return Filter.ReturnCode.SKIP;
                }
                if(type==KeyValueType.FOREIGN_KEY_COUNTER){
                        /* Transactional reads always ignore this column, no exceptions. */
                    return Filter.ReturnCode.SKIP;
                }

                switch(type){
                    case TOMBSTONE:
                        tombstoneTimestamps.add(kv.getTimestamp());
                        return Filter.ReturnCode.SKIP;
                    case ANTI_TOMBSTONE:
                        antiTombstoneTimestamps.add(kv.getTimestamp());
                        return Filter.ReturnCode.SKIP;
                    case USER_DATA:
                        long thisTs = kv.getTimestamp();
                        for(Long tt : tombstoneTimestamps){
                            if(thisTs < tt) return Filter.ReturnCode.SKIP;
                        }
                        for(Long at : antiTombstoneTimestamps){
                            if(thisTs < at) return Filter.ReturnCode.SKIP;
                        }
                        if (!accumulator.isFinished() && accumulator.isOfInterest(kv)) {
                            if (!accumulator.accumulate(kv)) {
                                return Filter.ReturnCode.NEXT_ROW;
                            }
                        }
                        return Filter.ReturnCode.INCLUDE;
                    default:
                        //TODO -sf- do better with this?
                        throw new AssertionError("Unexpected Data type: "+type);
                }
            }
        };
    }

    private MeasuredRegionScanner<Cell> buildScanner(List<List<Cell>> testData) throws IOException{
        return new IteratorRegionScanner(testData);
    }

    private KeyValueType getKeyValueType(Cell keyValue) {
        if (CellUtils.singleMatchingQualifier(keyValue,FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES)) {
            return KeyValueType.COMMIT_TIMESTAMP;
        } else if (CellUtils.singleMatchingQualifier(keyValue, FixedSpliceConstants.PACKED_COLUMN_BYTES)) {
            return KeyValueType.USER_DATA;
        } else if (CellUtils.singleMatchingQualifier(keyValue, FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES)) {
            if (CellUtils.matchingValue(keyValue, FixedSIConstants.EMPTY_BYTE_ARRAY)) {
                return KeyValueType.TOMBSTONE;
            } else if (CellUtils.matchingValue(keyValue,FixedSIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES)) {
                return KeyValueType.ANTI_TOMBSTONE;
            }
        } else if (CellUtils.singleMatchingQualifier(keyValue,FixedSIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return KeyValueType.FOREIGN_KEY_COUNTER;
        }
        return KeyValueType.OTHER;
    }
}

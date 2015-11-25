package com.splicemachine.db.iapi.types;

import org.junit.*;

/**
 * For BLOB( length K) - if specified, indicates that the length value is in multiples of 1024 (kilobytes).
 * Created by akorotenko on 11/23/15.
 */
public class SqlBlobKIT extends SqlBlobIT {

    public static final int BLOB_MIN_LEN = 1;
    public static final int BLOB_MAX_LEN_K = 1024*2;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlBlobKIT it = new SqlBlobKIT();
        it.createTable();
    }

    @Override
    protected int getBlobMaxLength() {
        return BLOB_MAX_LEN_K;
    }

    @Override
    protected String getTableName() {
        return "blob_k_it";
    }

    @Override
    protected String getSqlDataType() {
        return "BLOB(2K)";
    }

    @Test
    @Override
    public void testBiggerMaxNegative() throws Exception {
        super.testBiggerMaxNegative();
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        super.testBiggerMaxNegative();
    }
}

package com.splicemachine.db.iapi.types;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 *  For BLOB( length M) - if specified, indicates that the length value is in multiples of  1024*1024 (megabytes).
 * Created by akorotenko on 11/23/15.
 */
public class SqlBlobMIT extends SqlBlobIT {

    public static final int BLOB_MIN_LEN = 1;
    public static final int BLOB_MAX_LEN_M = 1024*1024*2;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlBlobMIT it = new SqlBlobMIT();
        it.createTable();
    }

    @Override
    protected int getBlobMaxLength() {
        return BLOB_MAX_LEN_M;
    }

    @Override
    protected String getTableName() {
        return "blob_m_it";
    }

    @Override
    protected String getSqlDataType() {
        return "BLOB(2M)";
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

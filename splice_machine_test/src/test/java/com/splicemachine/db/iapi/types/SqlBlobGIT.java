package com.splicemachine.db.iapi.types;

import org.junit.BeforeClass;

/**
 *  For BLOB( length M) - if specified, indicates that the length value is in multiples of 1024*1024*1024 (gigabytes).
 * Created by akorotenko on 11/23/15.
 */
public class SqlBlobGIT extends SqlBlobIT {

    public static final int BLOB_MIN_LEN = 1;
    public static final int BLOB_MAX_LEN_G = 1024*1024*1024*1;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlBlobGIT it = new SqlBlobGIT();
        it.createTable();
    }

    @Override
    protected int getBlobMaxLength() {
        return BLOB_MAX_LEN_G;
    }

    @Override
    protected String getTableName() {
        return "blob_g_it";
    }

    @Override
    protected String getSqlDataType() {
        return "BLOB(1G)";
    }

}

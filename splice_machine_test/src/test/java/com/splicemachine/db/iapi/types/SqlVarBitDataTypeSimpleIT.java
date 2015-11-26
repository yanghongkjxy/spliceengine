package com.splicemachine.db.iapi.types;

import org.junit.BeforeClass;

/**
 * Created by apaslavsky on 26.11.15.
 */
public class SqlVarBitDataTypeSimpleIT extends AbstractSqlVarbitDataTypeIT {
    public SqlVarBitDataTypeSimpleIT() throws Exception {
        super(2048);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlVarBitDataTypeSimpleIT it = new SqlVarBitDataTypeSimpleIT();
        it.createTable();
    }
}

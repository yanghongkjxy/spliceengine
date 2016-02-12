package com.splicemachine.db.iapi.types;

import org.junit.BeforeClass;

/**
 * Created by apaslavsky on 26.11.15.
 */
public class SqlVarcharDataTypeSimpleIT extends AbstractSqlVarcharDataTypeIT {
    public SqlVarcharDataTypeSimpleIT() throws Exception {
        super(2048);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlVarcharDataTypeSimpleIT it = new SqlVarcharDataTypeSimpleIT();
        it.createTable();
    }
}

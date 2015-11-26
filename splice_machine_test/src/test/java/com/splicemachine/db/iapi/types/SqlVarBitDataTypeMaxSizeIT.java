package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.reference.Limits;
import org.junit.BeforeClass;

/**
 * Created by apaslavsky on 26.11.15.
 */
public class SqlVarBitDataTypeMaxSizeIT extends AbstractSqlVarbitDataTypeIT {
    public SqlVarBitDataTypeMaxSizeIT() throws Exception {
        super(Limits.DB2_VARCHAR_MAXWIDTH);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlVarBitDataTypeMaxSizeIT it = new SqlVarBitDataTypeMaxSizeIT();
        it.createTable();
    }
}

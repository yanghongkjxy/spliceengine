package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by dmustafin on 11/25/15.
 */
public class SqlRealIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlRealIT it = new SqlRealIT();
        it.createTable();
    }


    @AfterClass
    public static void afterClass() {
        watcher.closeAll();
    }


    @Before
    public void beforeTest() throws Exception {
        runDelete();    // clear table before each test
    }


    private void checkValue(Float value) throws Exception {
        runInsert(value);
        ResultSet rs = runSelect();
        assert rs.next() : "result set is empty";
        Float res = rs.getFloat(1);
        if (rs.wasNull()) {
            res = null;
        }

        if (value == null && res == null) {
            return;
        }

        assert Float.compare(value, res) == 0 : "written value (" + value + ") is not equals read value (" + res + ")";
    }


    @Override
    protected String getTableName() {
        return "real_it";
    }

    @Override
    protected String getSqlDataType() {
        return "real";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(paramIndex, Types.FLOAT);
            return;
        }

        if (value instanceof Float) {
            preparedStatement.setFloat(paramIndex, (Float) value);
            return;
        }

        throwNotSupportedType(value);
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        checkValue(Float.MIN_VALUE);
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        checkValue(Float.MAX_VALUE);
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue(new Float(3.14));
    }

    @Ignore
    @Test
    @Override
    public void testBiggerMaxNegative() throws Exception {
        // nothing to do here
    }


    @Ignore
    @Test
    @Override
    public void testSmallerMinNegative() throws Exception {
        // nothing to do here
    }


    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }


    @Test
    @Override
    public void testUpdateValue() throws Exception {
        Float d1 = new Float(11.1);
        runInsert(d1);

        Float d2 = new Float(3.14);
        runUpdate(d2);

        ResultSet rs = runSelect();
        assert rs.next();
        Float res = rs.getFloat(1);
        if (rs.wasNull()) {
            res = null;
        }
        assert Float.compare(d2, res) == 0 : "written value (" + d2 + ") is not equals read value (" + res + ")";
    }
}

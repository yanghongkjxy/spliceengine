package com.splicemachine.db.iapi.types;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by dmustafin on 11/23/15.
 */
public class SqlDoubleIT extends DataTypeIT {

    public static final double DOUBLE_MIN = -1.79769E+308d;
    public static final double DOUBLE_MAX = 1.79769E+308d;
    public static final double DOUBLE_SMALLEST_POS = 2.225E-307d;
    public static final double DOUBLE_LARGEST_NEG = -2.225E-307d;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlDoubleIT it = new SqlDoubleIT();
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


    private void checkValue(Double value) throws Exception {
        runInsert(value);
        ResultSet rs = runSelect();
        assert rs.next();
        Double res = rs.getDouble(1);
        if (rs.wasNull()) {
            res = null;
        }

        if (value == null && res == null) {
            return;
        }

        assert Double.compare(value, res) == 0 : "written value (" + value + ") is not equals read value (" + res + ")";
    }

    @Override
    protected String getTableName() {
        return "double_it";
    }

    @Override
    protected String getSqlDataType() {
        return "double";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(paramIndex, Types.DOUBLE);
            return;
        }

        if (value instanceof Double) {
            preparedStatement.setDouble(paramIndex, (Double) value);
            return;
        }

        throwNotSupportedType(value);
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        checkValue(DOUBLE_MIN);
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        checkValue(DOUBLE_MAX);
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue(new Double(1.1));
    }

    @Test(expected = Exception.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        checkValue(DOUBLE_MAX + 1);
    }

    @Test(expected = Exception.class)
    @Override
    public void testSmallerMinNegative() throws Exception {
        checkValue(DOUBLE_MIN - 1);
    }

    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }

    @Override
    public void testUpdateValue() throws Exception {

    }
}

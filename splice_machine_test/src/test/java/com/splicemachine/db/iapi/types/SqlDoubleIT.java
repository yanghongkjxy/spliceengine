package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.*;


/**
 * Created by dmustafin on 11/23/15.
 */
public class SqlDoubleIT extends DataTypeIT {

    public static final double DOUBLE_MIN = Double.MIN_VALUE; // -1.79769E+308d;
    public static final double DOUBLE_MAX = Double.MAX_VALUE; // 1.79769E+308d;
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
        assert rs.next() : "result set is empty";
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


    @Ignore
    @Test(expected = Exception.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        checkValue(DOUBLE_MAX + 1.0);
    }

    @Ignore
    @Test(expected = Exception.class)
    @Override
    public void testSmallerMinNegative() throws Exception {
        checkValue(DOUBLE_MIN - 1.0);
    }

    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }

    @Override
    public void testUpdateValue() throws Exception {
        Double d1 = new Double(11.1);
        runInsert(d1);

        Double d2 = new Double(3.14);
        runUpdate(d2);

        ResultSet rs = runSelect();
        assert rs.next();
        Double res = rs.getDouble(1);
        if (rs.wasNull()) {
            res = null;
        }
        assert Double.compare(d2, res) == 0 : "written value (" + d2 + ") is not equals read value (" + res + ")";
    }

    @Test(expected = SQLDataException.class)
    public void testNan() throws Exception {
        checkValue(Double.NaN);
    }

    @Test(expected = SQLDataException.class)
    public void testNegInf() throws Exception {
        checkValue(Double.NEGATIVE_INFINITY);
    }


    @Test(expected = SQLDataException.class)
    public void testPosInf() throws Exception {
        checkValue(Double.POSITIVE_INFINITY);
    }


    @Test
    public void testSmallestPos() throws Exception {
        checkValue(DOUBLE_SMALLEST_POS);
    }


    @Test
    public void testLargestNeg() throws Exception {
        checkValue(DOUBLE_LARGEST_NEG);
    }

}

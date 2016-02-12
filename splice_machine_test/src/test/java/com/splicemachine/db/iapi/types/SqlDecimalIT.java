package com.splicemachine.db.iapi.types;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;

/**
 * Created by dmustafin on 11/24/15.
 */
public class SqlDecimalIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlDecimalIT it = new SqlDecimalIT();
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


    private void checkValue(BigDecimal value) throws Exception {
        runInsert(value);
        ResultSet rs = runSelect();
        assert rs.next() : "result set is empty";
        BigDecimal res = rs.getBigDecimal(1);
        if (rs.wasNull()) {
            res = null;
        }

        if (value == null && res == null) {
            return;
        }

        assert value.compareTo(res) == 0 : "written value (" + value + ") is not equals read value (" + res + ")";
    }


    @Override
    protected String getTableName() {
        return "decimal_it";
    }

    @Override
    protected String getSqlDataType() {
        return "decimal(8, 3)";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(paramIndex, Types.DECIMAL);
            return;
        }

        if (value instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value;
            preparedStatement.setBigDecimal(paramIndex, decimal);
            return;
        }

        throwNotSupportedType(value);
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        checkValue(createBigDecimal(-99999.999));
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        checkValue(createBigDecimal(99999.999));
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        BigDecimal d = createBigDecimal(12345.123);
        checkValue(d);
    }

    private BigDecimal createBigDecimal(Double value) {
        BigDecimal d = new BigDecimal(value);
        d = d.setScale(3, RoundingMode.DOWN);
        return d;
    }

    @Test(expected = SQLDataException.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        checkValue(createBigDecimal(99999.999 + 1));
    }

    @Test(expected = SQLDataException.class)
    @Override
    public void testSmallerMinNegative() throws Exception {
        checkValue(createBigDecimal(-99999.999 - 1));
    }

    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        BigDecimal d1 = createBigDecimal(12.3);
        runInsert(d1);

        BigDecimal d2 = createBigDecimal(3.14);
        runUpdate(d2);

        ResultSet rs = runSelect();
        assert rs.next();
        BigDecimal res = rs.getBigDecimal(1);
        if (rs.wasNull()) {
            res = null;
        }
        assert d2.compareTo(res) == 0 : "written value (" + d2 + ") is not equals read value (" + res + ")";
    }


    @Test
    public void testOkTable1() throws Exception {
        watcher.executeUpdate("create table decimal_1 (col1 decimal(31, 0))");
    }

    @Test
    public void testOkTable2() throws Exception {
        watcher.executeUpdate("create table decimal_2 (col1 decimal(31, 30))");
    }

    @Test
    public void testOkTable3() throws Exception {
        watcher.executeUpdate("create table decimal_3 (col1 decimal(31, 31))");
    }


    @Test(expected = SQLSyntaxErrorException.class)
    public void testWrongTable1() throws Exception {
        watcher.executeUpdate("create table wrong_decimal_1 (col1 decimal(0, 0))");
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testWrongTable2() throws Exception {
        watcher.executeUpdate("create table wrong_decimal_1 (col1 decimal(-2, 0))");
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testWrongTable3() throws Exception {
        watcher.executeUpdate("create table wrong_decimal_1 (col1 decimal(0, -3))");
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testWrongTable4() throws Exception {
        watcher.executeUpdate("create table wrong_decimal_1 (col1 decimal(32, 0))");
    }


    @Test(expected = SQLSyntaxErrorException.class)
    public void testWrongTable5() throws Exception {
        watcher.executeUpdate("create table wrong_decimal_1 (col1 decimal(-1, -90))");
    }


    @Test(expected = SQLSyntaxErrorException.class)
    public void testWrongTable6() throws Exception {
        watcher.executeUpdate("create table wrong_decimal_1 (col1 decimal(5, 10))");
    }


}

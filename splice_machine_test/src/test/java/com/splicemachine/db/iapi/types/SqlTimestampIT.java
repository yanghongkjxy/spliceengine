package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.*;

/**
 * Created by dmustafin on 11/25/15.
 */
public class SqlTimestampIT extends DataTypeIT {

    private static final long MICROS_IN_DAY = 24 * 60 * 60 * 1000 * 1000;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlTimestampIT it = new SqlTimestampIT();
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


    private void checkValue(Timestamp value) throws Exception {
        runInsert(value);
        ResultSet rs = runSelect();
        assert rs.next() : "result set is empty";
        Timestamp res = rs.getTimestamp(1);
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
        return "timestamp_it";
    }

    @Override
    protected String getSqlDataType() {
        return "timestamp";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(paramIndex, Types.TIMESTAMP);
            return;
        }

        if (value instanceof Timestamp) {
            preparedStatement.setTimestamp(paramIndex, (Timestamp) value);
            return;
        }

        throwNotSupportedType(value);
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        checkValue(new Timestamp(SQLTimestamp.MIN_TIMESTAMP + MICROS_IN_DAY));
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        checkValue(new Timestamp(SQLTimestamp.MAX_TIMESTAMP - MICROS_IN_DAY));
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue(new Timestamp(System.currentTimeMillis()));
    }


    @Test(expected = SQLException.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        checkValue(new Timestamp(SQLTimestamp.MAX_TIMESTAMP + 1));
    }



    @Test(expected = SQLException.class)
    @Override
    public void testSmallerMinNegative() throws Exception {
        checkValue(new Timestamp(SQLTimestamp.MIN_TIMESTAMP - 1));
    }


    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }


    @Test
    @Override
    public void testUpdateValue() throws Exception {
        Timestamp d1 = new Timestamp(System.currentTimeMillis() - 10);
        runInsert(d1);

        Timestamp d2 = new Timestamp(System.currentTimeMillis());
        runUpdate(d2);

        ResultSet rs = runSelect();
        assert rs.next();
        Timestamp res = rs.getTimestamp(1);
        if (rs.wasNull()) {
            res = null;
        }
        assert d2.compareTo(res) == 0 : "written value (" + d2 + ") is not equals read value (" + res + ")";
    }
}

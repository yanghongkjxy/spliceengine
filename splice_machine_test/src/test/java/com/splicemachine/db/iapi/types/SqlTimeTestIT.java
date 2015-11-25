package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.*;

/**
 * Integration tests for {@link java.sql.Types#TIME}
 */
public class SqlTimeTestIT extends DataTypeIT {

    public static final int ONE_HOUR = 60 * 60 * 1000;

    private static Time getMax() {
        return Time.valueOf("23:59:59");
    }

    private static Time getMin() {
        return Time.valueOf("00:00:00");
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlTimeTestIT it = new SqlTimeTestIT();
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

    @Override
    protected String getTableName() {
        return "time_it";
    }

    @Override
    protected String getSqlDataType() {
        return "time";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.TIME);
            return;
        }

        if (value instanceof Time) {
            preparedStatement.setTime(paramIndex, (Time)value);
            return;
        }

        throwNotSupportedType(value);
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        checkValue(getMin());
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        checkValue(getMax());
    }

    private void checkValue(Time time) throws Exception {
        runInsert(time);
        ResultSet rs = runSelect();
        Assert.assertTrue("result set is empty", rs.next());
        Time result = rs.getTime(1);
        if (rs.wasNull()) {
            result = null;
        }
        assertEqualsTime(time, result);
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue(new Time(System.currentTimeMillis()));
    }

    @Override
    public void testBiggerMaxNegative() throws Exception {
    }

    @Override
    public void testSmallerMinNegative() throws Exception {
    }


    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }

    @Override
    public void testUpdateValue() throws Exception {
        Time t1 = new Time(System.currentTimeMillis());
        runInsert(t1);

        Time t2 = new Time(System.currentTimeMillis() + ONE_HOUR);
        runInsert(t2);

        ResultSet rs = runSelect();
        Assert.assertTrue("result set is empty", rs.next());
        Time result = rs.getTime(1);
        if (rs.wasNull()) {
            result = null;
        }
        assertEqualsTime(t2, result);
    }

    private void assertEqualsTime(Time expected, Time result) {
        if (expected == null) {
            Assert.assertNull(result);
        } else {
            Assert.assertNotNull(result);
            Assert.assertEquals(expected.toString(), result.toString());
        }
    }
}
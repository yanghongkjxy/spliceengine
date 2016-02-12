package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.*;


/**
 * INTEGER provides 4 bytes of storage for integer values.
 * Created by akorotenko on 11/24/15.
 */
public class SqlIntegerIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlIntegerIT it = new SqlIntegerIT();
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
        return "integer_it";
    }

    @Override
    protected String getSqlDataType() {
        return "INT";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.INTEGER);
        } else if (value instanceof Integer) {
            preparedStatement.setInt(1, (Integer) value);
        } else {
            throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
        }
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        runInsert(Integer.valueOf(Integer.MIN_VALUE));

        ResultSet rs = runSelect();
        assert rs.next();
        Integer res = rs.getInt(1);
        Assert.assertEquals(Integer.MIN_VALUE, res.intValue());
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        runInsert(Integer.valueOf(Integer.MAX_VALUE));

        ResultSet rs = runSelect();
        assert rs.next();
        Integer res = rs.getInt(1);
        Assert.assertEquals(Integer.MAX_VALUE, res.intValue());
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        runInsert(Integer.valueOf(Integer.MAX_VALUE / 2));

        ResultSet rs = runSelect();
        assert rs.next();
        Integer res = rs.getInt(1);
        Assert.assertEquals(Integer.MAX_VALUE / 2, res.intValue());
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
        runInsert(null);

        ResultSet rs = runSelect();
        assert rs.next();
        Integer res = rs.getInt(1);
        Assert.assertTrue(rs.wasNull());

    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        runInsert(Integer.MAX_VALUE / 2);
        runUpdate(Integer.MAX_VALUE / 3);


        ResultSet rs = runSelect();
        assert rs.next();
        Integer res = rs.getInt(1);
        Assert.assertEquals(Integer.MAX_VALUE / 3, res.intValue());
    }
}

package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;


/**
 * BIGINT provides 8 bytes of storage for integer values.
 * Created by akorotenko on 11/24/15.
 */
public class SqlLongIntegerIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlLongIntegerIT it = new SqlLongIntegerIT();
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
        return "bigint_it";
    }

    @Override
    protected String getSqlDataType() {
        return "BIGINT";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.BIGINT);
        } else if (value instanceof Long) {
            preparedStatement.setLong(1, (Long) value);
        } else {
            throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
        }
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        runInsert(Long.MIN_VALUE);

        ResultSet rs = runSelect();
        assert rs.next();
        Long res = rs.getLong(1);
        Assert.assertEquals(Long.MIN_VALUE, res.longValue());
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        runInsert(Long.MAX_VALUE);

        ResultSet rs = runSelect();
        assert rs.next();
        Long res = rs.getLong(1);
        Assert.assertEquals(Long.MAX_VALUE, res.longValue());
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        runInsert(Long.MAX_VALUE / 2);

        ResultSet rs = runSelect();
        assert rs.next();
        Long res = rs.getLong(1);
        Assert.assertEquals(Long.MAX_VALUE / 2, res.longValue());
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
        Long res = rs.getLong(1);
        Assert.assertTrue(rs.wasNull());

    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        runInsert(Long.MAX_VALUE / 2);
        runUpdate(Long.MAX_VALUE / 3);


        ResultSet rs = runSelect();
        assert rs.next();
        Long res = rs.getLong(1);
        Assert.assertEquals(Long.MAX_VALUE / 3, res.longValue());
    }
}

package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by ochnev on 11/23/15.
 */
public class SqlSmallIntTestIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlSmallIntTestIT it = new SqlSmallIntTestIT();
        it.createTable();
    }

    @AfterClass
    public static void afterClass() {
        watcher.closeAll();
    }

    @Before
    public void beforeTest() throws Exception {
        runDelete(); // clear the table before each test
    }

    @Override
    protected String getTableName() {
        return "smallint_it";
    }

    @Override
    protected String getSqlDataType() {
        return "smallint";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.SMALLINT);
            return;
        }

        if (value instanceof Short) {
            Short s = (Short) value;
            preparedStatement.setShort(paramIndex, s);
            return;
        }

        throwNotSupportedType(value);
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        checkValue(Short.MIN_VALUE);
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        checkValue(Short.MAX_VALUE);
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue((short) 15000);
    }

    private void checkValue(Short value) throws Exception {
        runInsert(value);

        ResultSet rs = runSelect();
        assert rs.next();
        Short res = rs.getShort(1);
        if (rs.wasNull()) {
            res = null;
        }

        if (value == null && res == null) {
            return;
        }

        assert Short.compare(value, res) == 0 : "the value written (" + value + ") is not equal to the value read (" + res + ")";
    }

    @Test(expected = IllegalArgumentException.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        runInsert(Short.MAX_VALUE + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    @Override
    public void testSmallerMinNegative() throws Exception {
        runInsert(Short.MIN_VALUE - 1);
    }

    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        runInsert((short) 12000);
        runUpdate((short) 17000);

        ResultSet rs = runSelect();
        assert rs.next();
        Short actualValue = rs.getShort(1);
        Assert.assertEquals((short) 17000, actualValue.shortValue());
    }

}

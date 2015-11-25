package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by ochnev on 11/23/15.
 */
public class SqlBooleanTestIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlBooleanTestIT it = new SqlBooleanTestIT();
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
        return "boolean_it";
    }

    @Override
    protected String getSqlDataType() {
        return "boolean";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.BOOLEAN);
            return;
        }

        if (value instanceof Boolean) {
            Boolean b = (Boolean) value;
            preparedStatement.setBoolean(paramIndex, b);
            return;
        }

        throwNotSupportedType(value);
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        checkValue(Boolean.FALSE);
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        checkValue(Boolean.TRUE);
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue(Boolean.TRUE);
    }

    private void checkValue(Boolean value) throws Exception {
        runInsert(value);

        ResultSet rs = runSelect();
        assert rs.next();
        Boolean res = rs.getBoolean(1);
        if (rs.wasNull()) {
            res = null;
        }

        if (value == null && res == null) {
            return;
        }

        assert Boolean.compare(value, res) == 0 : "the value written (" + value + ") is not equal to the value read (" + res + ")";
    }

    @Ignore
    @Test
    @Override
    public void testBiggerMaxNegative() throws Exception {
    }

    @Ignore
    @Test
    @Override
    public void testSmallerMinNegative() throws Exception {
    }

    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        runInsert(Boolean.TRUE);
        runUpdate(Boolean.FALSE);
        ResultSet rs = runSelect();
        assert rs.next();
        Boolean actual = rs.getBoolean(1);
        if (rs.wasNull()) {
            actual = null;
        }
        Assert.assertEquals(Boolean.FALSE, actual);
    }

}

package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by ochnev on 11/23/15.
 */
public class SqlCharTestIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlCharTestIT it = new SqlCharTestIT();
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
        return "char_it";
    }

    @Override
    protected String getSqlDataType() {
        return "char";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.CHAR);
            return;
        }

        if (value instanceof String) {
            String str = (String) value;
            preparedStatement.setString(paramIndex, str);
            return;
        }

        throwNotSupportedType(value);
    }

    @Ignore
    @Test
    @Override
    public void testMinValue() throws Exception {
        checkValue("");
    }

    @Ignore
    @Test
    @Override
    public void testMaxValue() throws Exception {
        // The maximum length of the CHAR data type is the value of java.lang.Integer.MAX_VALUE
        // which is too large to test on a local machine
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue("a");
    }

    private void checkValue(String value) throws Exception {
        runInsert(value);

        ResultSet rs = runSelect();
        assert rs.next();
        String res = rs.getString(1);
        if (rs.wasNull()) {
            res = null;
        }

        if (value == null && res == null) {
            return;
        }

        if(value == null || res == null) {
            Assert.fail("Either the expected value or the actual value is NULL but not both, so they are not equal to each other");
        }

        Assert.assertEquals("the value written (" + value + ") is not equal to the value read (" + res + ")", value, res);
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
        runInsert("a");
        runUpdate("b");

        ResultSet rs = runSelect();
        assert rs.next();
        String actualValue = rs.getString(1);
        Assert.assertEquals("b", actualValue);
    }

}

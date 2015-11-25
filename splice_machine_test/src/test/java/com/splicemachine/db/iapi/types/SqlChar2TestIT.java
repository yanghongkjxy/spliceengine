package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.*;

/**
 * Created by ochnev on 11/23/15.
 */
public class SqlChar2TestIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlChar2TestIT it = new SqlChar2TestIT();
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
        return "char2_it";
    }

    @Override
    protected String getSqlDataType() {
        return "char(10)";
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

    @Test
    @Override
    public void testMaxValue() throws Exception {
        checkValue("abc");
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue("abc");
    }

    @Test(expected = SQLDataException.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        runInsert("aaabbbccc no no");
    }

    private void checkValue(String value) throws Exception {
        if (value == null) {
            runInsert(null);
        } else {
            runInsert(value.trim());
        }

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

        Assert.assertEquals("the value written (" + value.trim() + ") is not equal to the value read (" + res.trim() + ")", value.trim(), res.trim());
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
        runInsert("foo");
        runUpdate("bar");

        ResultSet rs = runSelect();
        assert rs.next();
        String actualValue = rs.getString(1);
        Assert.assertEquals("bar", actualValue.trim());
    }

}

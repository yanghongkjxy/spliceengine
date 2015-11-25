package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.*;

/**
 * Created by dmustafin on 11/24/15.
 */
public class LongvarcharIT extends DataTypeIT {

    private final int MAX_LENGTH = 32700;

    @BeforeClass
    public static void beforeClass() throws Exception {
        LongvarcharIT it = new LongvarcharIT();
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


    private void checkValue(String value) throws Exception {
        runInsert(value);
        ResultSet rs = runSelect();
        assert rs.next() : "result set is empty";
        String res = rs.getString(1);
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
        return "longvarchar_it";
    }

    @Override
    protected String getSqlDataType() {
        return "long varchar";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(paramIndex, Types.LONGNVARCHAR);
            return;
        }

        if (value instanceof String) {
            preparedStatement.setString(paramIndex, value.toString());
            return;
        }

        throwNotSupportedType(value);
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        checkValue("");
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        String s = generateString(MAX_LENGTH);
        checkValue(s);
    }

    private String generateString(int length) {
        StringBuilder sb = new StringBuilder(length);
        while (sb.length() < length) {
            sb.append("qwertyuiop");
        }

        if (sb.length() > length) {
            sb.delete(length, sb.length());
        }

        return sb.toString();
    }


    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue("Normal Long String");
    }

    @Test(expected = SQLException.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        String s = generateString(MAX_LENGTH + 1);
        checkValue(s);
    }

    @Ignore
    @Test
    @Override
    public void testSmallerMinNegative() throws Exception {
        // nothing to do here
    }

    @Ignore  // bug in Derby
    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        String d1 = "first value";
        runInsert(d1);

        String d2 = "second value";
        runUpdate(d2);

        ResultSet rs = runSelect();
        assert rs.next();
        String res = rs.getString(1);
        if (rs.wasNull()) {
            res = null;
        }
        assert d2.compareTo(res) == 0 : "written value (" + d2 + ") is not equals read value (" + res + ")";
    }
}

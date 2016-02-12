package com.splicemachine.db.iapi.types;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;

/**
 * Created by akorotenko on 11/24/15.
 */
public class SqlBitIT extends DataTypeIT {

    private static final String BASE_NAME = "char_bit_it";

    public static final int CHAR_FOR_BIT_DATA_MIN_LEN = 1;
    public static final int CHAR_FOR_BIT_DATA_MAX_LEN = 254;

    private String length = "1";
    private String tableName = BASE_NAME;

    @AfterClass
    public static void afterClass() {
        watcher.closeAll();
    }


    @Override
    protected String getTableName() {
        return tableName;
    }

    @Override
    protected String getSqlDataType() {
        return "char(" + length + ") for bit data";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.BINARY);
            return;
        }

        if (value instanceof byte[]) {
            preparedStatement.setBytes(1, (byte[]) value);
            return;
        }

        throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        createTable();
        runInsert(RandomUtils.nextBytes(CHAR_FOR_BIT_DATA_MIN_LEN));

        ResultSet rs = runSelect();
        assert rs.next();
        byte[] res = rs.getBytes(1);
        Assert.assertEquals(CHAR_FOR_BIT_DATA_MIN_LEN, res.length);
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        length = String.valueOf(CHAR_FOR_BIT_DATA_MAX_LEN);
        tableName = BASE_NAME + length;
        createTable();
        runInsert(RandomUtils.nextBytes(CHAR_FOR_BIT_DATA_MAX_LEN));

        ResultSet rs = runSelect();
        assert rs.next();
        byte[] res = rs.getBytes(1);
        Assert.assertEquals(CHAR_FOR_BIT_DATA_MAX_LEN, res.length);
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        length = String.valueOf(CHAR_FOR_BIT_DATA_MAX_LEN / 2);
        tableName = BASE_NAME + length;
        createTable();
        runInsert(RandomUtils.nextBytes(CHAR_FOR_BIT_DATA_MAX_LEN / 2));

        ResultSet rs = runSelect();
        assert rs.next();
        byte[] res = rs.getBytes(1);
        Assert.assertEquals(CHAR_FOR_BIT_DATA_MAX_LEN / 2, res.length);
    }

    @Test(expected = SQLException.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        // Create table with char(255)
        length = String.valueOf(CHAR_FOR_BIT_DATA_MAX_LEN + 1);
        tableName = BASE_NAME + length;
        try {
            createTable();
            throw new AssertionError("Table creation should produce an exception");
        } catch (SQLSyntaxErrorException e) {
            assert e.getSQLState().equals("42611");
        }

        // Create table with char(254)
        length = String.valueOf(CHAR_FOR_BIT_DATA_MAX_LEN);
        createTable();

        // and insert char[255]
        runInsert(RandomUtils.nextBytes(CHAR_FOR_BIT_DATA_MAX_LEN + 1));
    }

    @Test
    @Override
    public void testSmallerMinNegative() throws Exception {
        // Create table with char(0)
        length = String.valueOf(CHAR_FOR_BIT_DATA_MIN_LEN - 1);
        tableName = BASE_NAME + length;
        try {
            createTable();
            throw new AssertionError("Table creation should produce an exception");
        } catch (SQLSyntaxErrorException e) {
            assert e.getSQLState().equals("42X44");
        }
    }

    @Test
    @Override
    public void testNullValue() throws Exception {
        runDelete();
        runInsert(null);

        ResultSet rs = runSelect();
        assert rs.next();
        byte[] res = rs.getBytes(1);
        Assert.assertNull(res);
    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        length = String.valueOf(20);
        tableName = BASE_NAME + "update";
        createTable();

        runInsert(RandomUtils.nextBytes(20));
        byte[] updateValue = RandomUtils.nextBytes(20);
        runUpdate(updateValue);

        ResultSet rs = runSelect();
        assert rs.next();
        byte[] res = rs.getBytes(1);
        Assert.assertArrayEquals(updateValue, res);
    }
}

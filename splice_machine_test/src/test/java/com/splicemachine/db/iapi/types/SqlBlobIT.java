package com.splicemachine.db.iapi.types;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.log4j.Logger;
import org.junit.*;

import java.io.*;
import java.sql.*;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

/**
 * Created by akorotenko on 11/23/15.
 */
public class SqlBlobIT extends DataTypeIT {

    private static final Logger log = Logger.getLogger(SqlBlobIT.class);

    public static final int BLOB_MIN_LEN = 1;
    public static final int BLOB_MAX_LEN = 2147483647;

    // Service methods
    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlBlobIT it = new SqlBlobIT();
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

    protected int getBlobMaxLength() {
        return BLOB_MAX_LEN;
    }

    @Override
    protected String getTableName() {
        return "blob_it";
    }

    @Override
    protected String getSqlDataType() {
        return "BLOB";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.BLOB);
            return;
        }

        if (value instanceof InputStream) {
            preparedStatement.setBlob(1, (InputStream) value);
            return;
        }

        throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        insertStream(BLOB_MIN_LEN);

        ResultSet rs = runSelect();
        assert rs.next();
        Blob res = rs.getBlob(1);
        Assert.assertEquals(BLOB_MIN_LEN, res.length());
    }

    @Override
    public void testMaxValue() throws Exception {
        insertStream(getBlobMaxLength());

        ResultSet rs = runSelect();
        assert rs.next();
        Blob res = rs.getBlob(1);
        Assert.assertEquals(getBlobMaxLength(), res.length());
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        insertStream(getBlobMaxLength() / 10);

        ResultSet rs = runSelect();
        assert rs.next();
        Blob res = rs.getBlob(1);
        Assert.assertEquals(getBlobMaxLength() / 10, res.length());
    }

    @Override
    public void testBiggerMaxNegative() throws Exception {
        try {
            insertStream(getBlobMaxLength() + 1);
        } catch (SQLException e) {
            //e.getSQLState();
            return;
        }
        throw new AssertionError("Expected exception is missed!");
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
        Blob res = rs.getBlob(1);
        Assert.assertNull(res);
    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        runInsert(new ByteArrayInputStream(new byte[] {10, 12, -10}));
        byte[] updateValue = new byte[] {100, 120, -100, -128};
        runUpdate(new ByteArrayInputStream(updateValue));

        ResultSet rs = runSelect();
        assert rs.next();
        Blob res = rs.getBlob(1);
        Assert.assertArrayEquals(updateValue, IOUtils.toByteArray(res.getBinaryStream()));
    }

    private void insertStream(int length) throws Exception {
        InputStream io = null;
        BufferedInputStream bis = null;
        try {
            io = new ByteArrayInputStream(RandomUtils.nextBytes(length));
            bis = new BufferedInputStream(io);
            runInsert(bis);
        } finally {
            if (bis != null) {
                bis.close();
            }
        }
    }
}

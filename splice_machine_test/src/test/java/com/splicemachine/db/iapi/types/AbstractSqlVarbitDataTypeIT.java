package com.splicemachine.db.iapi.types;

import org.apache.commons.lang3.RandomUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by apaslavsky on 25.11.15.
 */
public abstract class AbstractSqlVarbitDataTypeIT extends DataTypeIT {
    public static final int MIN_LENGTH = 1;
    private int size;

    protected AbstractSqlVarbitDataTypeIT(int size) throws Exception {
        this.size = size;
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
        return "varbit" + size + "_it";
    }

    @Override
    protected String getSqlDataType() {
        return "VARCHAR (" + size + ") FOR BIT DATA";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.VARBINARY);
        } else if (value instanceof byte[]) {
            preparedStatement.setBytes(1, (byte[]) value);
        } else {
            throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
        }
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        doInsertValidateTest(MIN_LENGTH);
    }

    private void doInsertValidateTest(int length) throws Exception {
        byte[] generated = RandomUtils.nextBytes(length);
        runInsert(generated);
        validate(generated);
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        doInsertValidateTest(size);
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        doInsertValidateTest(size / 2);
    }

    @Test(expected = SQLException.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        doInsertValidateTest(size * 2);
    }

    @Override
    public void testSmallerMinNegative() throws Exception {}

    @Test
    @Override
    public void testNullValue() throws Exception {
        runInsert(null);
        validate(null);
    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        byte[] data1 = RandomUtils.nextBytes(size);
        runInsert(data1);

        byte[] data2 = RandomUtils.nextBytes(size);
        runUpdate(data2);

        validate(data2);
    }

    private void validate(byte[] generated) throws Exception {
        ResultSet rs = runSelect();
        Assert.assertTrue(rs.next());
        Assert.assertArrayEquals(generated, getValue(rs));
    }

    private byte[] getValue(ResultSet rs) throws SQLException {
        byte[] bytes = rs.getBytes(1);
        if (rs.wasNull()) {
            return null;
        }
        return bytes;
    }
}

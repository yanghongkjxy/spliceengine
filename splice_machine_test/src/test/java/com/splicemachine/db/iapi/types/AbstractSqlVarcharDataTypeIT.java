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
import java.util.Random;

/**
 * Created by apaslavsky on 25.11.15.
 */
public abstract class AbstractSqlVarcharDataTypeIT extends DataTypeIT {
    public static final int MIN_LENGTH = 1;
    private int size;
    private static  Random random = new Random();

    protected AbstractSqlVarcharDataTypeIT(int size) throws Exception {
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
        return "varchar" + size + "_it";
    }

    @Override
    protected String getSqlDataType() {
        return "VARCHAR (" + size + ")";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.VARCHAR);
        } else if (value instanceof String) {
            preparedStatement.setString(1, (String) value);
        } else {
            throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
        }
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        doInsertValidateTest(MIN_LENGTH);
    }

    private String nextRandomString(int size){
        char[] string = new char[size];
        for(int pos=0;pos<string.length;pos++){
            string[pos] = (char)random.nextInt(255);
        }
        return new String(string);
    }

    private void doInsertValidateTest(int length) throws Exception {
        String generated = nextRandomString(length);
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
        String data1 = nextRandomString(size);
        runInsert(data1);

        String data2 = nextRandomString(size);
        runUpdate(data2);

        validate(data2);
    }

    private void validate(String generated) throws Exception {
        ResultSet rs = runSelect();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(generated, getValue(rs));
    }

    private String getValue(ResultSet rs) throws SQLException {
        String value = rs.getString(1);
        if (rs.wasNull()) {
            return null;
        }
        return value;
    }
}

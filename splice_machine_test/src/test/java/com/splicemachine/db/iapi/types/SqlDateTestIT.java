package com.splicemachine.db.iapi.types;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

/**
 * Created by dmustafin on 11/23/15.
 */
public class SqlDateTestIT extends DataTypeIT {

    @BeforeClass
    public static void beforeTest() throws Exception {
        SqlDateTestIT it = new SqlDateTestIT();
        it.createTable();
    }


    @AfterClass
    public static void afterClass() {
        watcher.closeAll();
    }

    @Override
    protected String getTableName() {
        return "date_it";
    }

    @Override
    protected String getSqlDataType() {
        return "date";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.DATE);
            return;
        }

        if (value instanceof Date) {
            long date = ((Date) value).getTime();
            java.sql.Date sqlDate = new java.sql.Date(date);
            preparedStatement.setDate(paramIndex, sqlDate);
            return;
        }

        throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
    }

    @Test
    @Override
    public void testMinValue() {

    }

    @Test
    @Override
    public void testMaxValue() {

    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        Date dt = new Date();   // now
        runInsert(dt);
    }

    @Test
    @Override
    public void testBiggerMaxNegative() throws Exception {

    }

    @Test
    @Override
    public void testSmallerMinNegative() throws Exception {

    }


    @Test
    @Override
    public void testNullValue() throws Exception {
        runInsert(null);
    }

 }
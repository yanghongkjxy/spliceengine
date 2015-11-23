package com.splicemachine.db.iapi.types;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by dmustafin on 11/23/15.
 */
public class SqlDateTestIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlDateTestIT it = new SqlDateTestIT();
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


    //TODO: should we look to HH:MM:SS of dates?
    private boolean compareDates(Date d1, Date d2) {
        if (d1 == d2) {
            return true;
        }

        if (d1 == null || d2 == null) {
            return false;
        }

        Calendar c1 = new GregorianCalendar();
        c1.setTime(d1);

        Calendar c2 = new GregorianCalendar();
        c2.setTime(d2);

        if (c1.get(Calendar.YEAR) != c2.get(Calendar.YEAR)) {
            return false;
        }

        if (c1.get(Calendar.MONTH) != c2.get(Calendar.MONTH)) {
            return false;
        }

        if (c1.get(Calendar.DAY_OF_MONTH) != c2.get(Calendar.DAY_OF_MONTH)) {
            return false;
        }

        return true;
    }


    @Test
    @Override
    public void testMinValue() throws Exception {
        Date dt = new Date(-2208988800000L);
        runInsert(dt);
        ResultSet rs = runSelect();
        assert rs.next();
        Date res = rs.getDate(1);
        assert compareDates(dt, res) : "written value (" + dt + ") is not equals read value (" + res + ")";
    }

    @Test
    @Override
    public void testMaxValue() throws Exception  {
        runInsert(new Date(253402214400000L));
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
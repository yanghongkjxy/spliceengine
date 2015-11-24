package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.*;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by dmustafin on 11/23/15.
 */
public class SqlDateTestIT extends DataTypeIT {

    public static final long MIN_DATE =  -2177452800000L;  // -2208988800000L;
    public static final long MAX_DATE = 253402300799000L;  //253402214400000L;
    private static final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;

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

        throwNotSupportedType(value);
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
        checkValue(MIN_DATE);
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        checkValue(MAX_DATE);
    }

    private void checkValue(Long date) throws Exception {
        Date dt = null;
        if (date != null) {
            dt = new Date(date);
        }
        runInsert(dt);
        ResultSet rs = runSelect();
        assert rs.next();
        Date res = rs.getDate(1);
        if (rs.wasNull()) {
            res = null;
        }
        assert compareDates(dt, res) : "written value (" + dt + ") is not equals read value (" + res + ")";
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValue(System.currentTimeMillis());
    }

    @Test(expected = SQLDataException.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        Date dt = new Date(MAX_DATE + MILLIS_IN_DAY);
        runInsert(dt);
    }

    @Ignore  // it seems date BC (-0001-01-01) is transferred to our age (0001-01-01)
    @Test(expected = Exception.class)
    @Override
    public void testSmallerMinNegative() throws Exception {
        Calendar calendar = new GregorianCalendar(-100, 0, 1);
        long l = calendar.getTimeInMillis();
        Date dt = new Date(l);
        System.out.println(dt);
        runInsert(dt);
        /*
        //for (long i = -62167370400001L; l > Long.MIN_VALUE; l = l - 100000000L) {
        for (long i = 0L; i > Long.MIN_VALUE; i = i - 100000000L) {
            System.out.println("---------");
            System.out.println(i);
            Date dt = new Date(i);
            System.out.println(dt);
            runInsert(dt);
        }

        //long l = MIN_DATE - 1;
//        Date dt = new Date(l);
//        System.out.println(dt);
//        runInsert(dt);   */
    }


    @Test
    @Override
    public void testNullValue() throws Exception {
        checkValue(null);
    }

    @Override
    public void testUpdateValue() throws Exception {
        Date d1 = new Date();
        runInsert(d1);

        Date d2 = new Date(0);
        runUpdate(d2);

        ResultSet rs = runSelect();
        assert rs.next();
        Date res = rs.getDate(1);
        if (rs.wasNull()) {
            res = null;
        }
        assert compareDates(d2, res) : "written value (" + d2 + ") is not equals read value (" + res + ")";
    }

}
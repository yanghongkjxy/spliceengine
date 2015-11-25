package com.splicemachine.db.iapi.types;

import org.junit.*;

import javax.sql.rowset.serial.SerialClob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by ochnev on 11/23/15.
 */
public class SqlClobTestIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlClobTestIT it = new SqlClobTestIT();
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
        return "clob_it";
    }

    @Override
    protected String getSqlDataType() {
        return "clob";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.CLOB);
            return;
        }

        if (value instanceof Clob) {
            Clob clob = (Clob) value;
            preparedStatement.setClob(paramIndex, clob);
            return;
        }

        throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        Clob testClob = new SerialClob(new char[] {'a'});
        runInsert(testClob); // just test that the insert is executed successfully
    }

    @Ignore
    @Test
    @Override
    public void testMaxValue() throws Exception {
        // Too huge to test it on a developer's machine
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        Clob testClob = getSomeClob(100_000);
        runInsert(testClob); // just test that the insert is executed successfully
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
        runInsert(null);
    }

    /**
     * Produce a simple CLOB value for testing purposes
     *
     * @param size how many characters
     * @return
     * @throws SQLException
     */
    private Clob getSomeClob(int size) throws SQLException {
        char[] ch = new char[size];
        for(int i = 0; i < size; i++) {
            ch[i] = 'a';
        }
        return new SerialClob(ch);
    }

    @Ignore
    @Test
    @Override
    public void testUpdateValue() throws Exception {
    }

}

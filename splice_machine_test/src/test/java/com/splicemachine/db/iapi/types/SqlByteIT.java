package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;


/**
 * Byte provides 1 bytes of storage for TINYINT values.
 * This test was disabled because SpliceDB is not supported TinyInt data type. The same issue exist at the
 * <a href="https://issues.apache.org/jira/browse/DERBY-695">Derby</a>
 */
@Ignore
public class SqlByteIT extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlByteIT it = new SqlByteIT();
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
        return "byte_it";
    }

    @Override
    protected String getSqlDataType() {
        return "TINYINT";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.TINYINT);
        } else if (value instanceof Byte) {
            preparedStatement.setInt(1, (Byte) value);
        } else {
            throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
        }
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        runInsert(Byte.MIN_VALUE);

        ResultSet rs = runSelect();
        assert rs.next();
        Byte res = rs.getByte(1);
        Assert.assertEquals(Byte.MIN_VALUE, res.byteValue());
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        runInsert(Byte.MAX_VALUE);

        ResultSet rs = runSelect();
        assert rs.next();
        Byte res = rs.getByte(1);
        Assert.assertEquals(Byte.MAX_VALUE, res.byteValue());
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        runInsert((byte) (Byte.MAX_VALUE / 2));

        ResultSet rs = runSelect();
        assert rs.next();
        Byte res = rs.getByte(1);
        Assert.assertEquals(Byte.MAX_VALUE / 2, res.byteValue());
    }

    @Override
    public void testBiggerMaxNegative() throws Exception {

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
        Byte res = rs.getByte(1);
        Assert.assertTrue(rs.wasNull());

    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        runInsert((byte) (Byte.MAX_VALUE / 2));
        runUpdate((byte) (Byte.MAX_VALUE / 3));


        ResultSet rs = runSelect();
        assert rs.next();
        Byte res = rs.getByte(1);
        Assert.assertEquals(Byte.MAX_VALUE / 3, res.byteValue());
    }
}

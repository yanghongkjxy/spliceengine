package com.splicemachine.db.iapi.types;

import org.apache.commons.lang3.RandomUtils;
import org.junit.*;

import java.sql.*;


/**
 * The LONG VARCHAR FOR BIT DATA type allows storage of bit strings up to 32,700 bytes.
 * It is identical to VARCHAR FOR BIT DATA, except that you do not have to specify a maximum length when creating columns of this type.
 * Created by akorotenko on 11/24/15.
 */
public class SqlLongvarbitIT extends DataTypeIT {

    public static final int LONG_VARCHAR_FOR_BIT_DATA_MIN_LEN = 1;
    public static final int LONG_VARCHAR_FOR_BIT_DATA_MAX_LEN = 32700;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SqlLongvarbitIT it = new SqlLongvarbitIT();
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
        return "longvarchar_bit_it";
    }

    @Override
    protected String getSqlDataType() {
        return "long varchar for bit data";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.LONGVARBINARY);
        } else if (value instanceof byte[]) {
            preparedStatement.setBytes(1, (byte[]) value);
        } else {
            throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
        }
    }

    private void validate(byte[] generated) throws Exception {
        ResultSet rs = runSelect();
        assert rs.next();
        byte[] res = rs.getBytes(1);
        Assert.assertArrayEquals(generated, res);
    }

    @Test
    @Override
    public void testMinValue() throws Exception {
        byte[] generated = RandomUtils.nextBytes(LONG_VARCHAR_FOR_BIT_DATA_MIN_LEN);
        runInsert(generated);

        validate(generated);
    }

    @Test
    @Override
    public void testMaxValue() throws Exception {
        byte[] generated = RandomUtils.nextBytes(LONG_VARCHAR_FOR_BIT_DATA_MAX_LEN);
        runInsert(generated);

        validate(generated);
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        byte[] generated = RandomUtils.nextBytes(LONG_VARCHAR_FOR_BIT_DATA_MAX_LEN / 2);
        runInsert(generated);

        validate(generated);
    }

    @Test(expected = SQLDataException.class)
    @Override
    public void testBiggerMaxNegative() throws Exception {
        runInsert(RandomUtils.nextBytes(LONG_VARCHAR_FOR_BIT_DATA_MAX_LEN +1));
    }

    @Override
    public void testSmallerMinNegative() throws Exception {
    }

    @Test
    @Override
    public void testNullValue() throws Exception {
        runInsert(null);

        validate(null);
    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        runInsert(RandomUtils.nextBytes(LONG_VARCHAR_FOR_BIT_DATA_MAX_LEN / 2));

        byte[] generated = RandomUtils.nextBytes(LONG_VARCHAR_FOR_BIT_DATA_MAX_LEN / 3);
        runUpdate(generated);

        validate(generated);
    }
}

package com.splicemachine.db.iapi.types;

import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by ochnev on 11/25/15.
 */
public class RowIdTest extends DataTypeIT {

    @BeforeClass
    public static void beforeClass() throws Exception {
        RowIdTest it = new RowIdTest();
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
        return "rowid_it";
    }

    @Override
    protected String getSqlDataType() {
        return "int";
    }

    @Override
    protected void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(1, Types.INTEGER);
            return;
        }

        if (value instanceof Integer) {
            Integer i = (Integer) value;
            preparedStatement.setInt(paramIndex, i);
            return;
        }

        throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
    }

    @Override
    public void testMinValue() throws Exception {
    }

    @Override
    public void testMaxValue() throws Exception {
    }

    @Test
    @Override
    public void testNormalValue() throws Exception {
        checkValueWithRowId(15000);
    }

    private void checkValueWithRowId(Integer value) throws Exception {
        runInsert(value);

        ResultSet rs = runSelectWithRowId();
        assert rs.next();
        String rowId = rs.getString(1);
        Integer res = rs.getInt(2);
        if (rs.wasNull()) {
            res = null;
        }

        if (value == null && res == null) {
            return;
        }

        assert Integer.compare(value, res) == 0 : "the value written (" + value + ") is not equal to the value read (" + res + ")";
        assert rowId != null : "ROWID should not be null";
    }

    @Override
    public void testBiggerMaxNegative() throws Exception {
    }

    @Override
    public void testSmallerMinNegative() throws Exception {
    }

    @Override
    public void testNullValue() throws Exception {
    }

    @Test
    @Override
    public void testUpdateValue() throws Exception {
        runInsert(12000);
        ResultSet rs1 = runSelectWithRowId();
        assert rs1.next();
        String rowId1 = rs1.getString(1);

        runUpdateWithRowId(12000, 17000);

        ResultSet rs2 = runSelectWithRowId();
        assert rs2.next();
        String rowId2 = rs2.getString(1);
        Integer actualValue = rs2.getInt(2);

        Assert.assertEquals(17000, actualValue.intValue());
        Assert.assertEquals(rowId1, rowId2);
    }

    protected ResultSet runSelectWithRowId() throws Exception {
        String tableName = getTableName();
        String sql = "select rowid, col1 from " + tableName;
        ResultSet rs = watcher.executeQuery(sql);
        return rs;
    }

    protected void runUpdateWithRowId(Object oldValue, Object newValue) throws Exception {
        String tableName = getTableName();
        String sql = "update " + tableName + " set col1 = ? WHERE rowid = (SELECT rowid FROM " + tableName + " WHERE col1 = ?)";
        PreparedStatement statement = watcher.prepareStatement(sql);
        setParameterValue(statement, newValue, 1);
        setParameterValue(statement, oldValue, 2);
        int count = statement.executeUpdate();
        assert count == 1;
    }

}

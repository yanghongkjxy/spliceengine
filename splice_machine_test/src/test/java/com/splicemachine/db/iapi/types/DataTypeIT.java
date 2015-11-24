package com.splicemachine.db.iapi.types;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by dmustafin on 11/23/15.
 */
public abstract class DataTypeIT {
    protected static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher("DATA_TYPES_TEST");
    protected static final SpliceWatcher watcher = new SpliceWatcher("DATA_TYPES_TEST");

    @ClassRule
    public static TestRule rule = RuleChain.outerRule(schemaWatcher).around(watcher);

    protected abstract String getTableName();
    protected abstract String getSqlDataType();
    protected abstract void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException;

    // create table
    // CRUD + negative
    // select
    public abstract void testMinValue() throws Exception;
    public abstract void testMaxValue() throws Exception;
    public abstract void testNormalValue() throws Exception;
    public abstract void testBiggerMaxNegative() throws Exception;
    public abstract void testSmallerMinNegative() throws Exception;
    public abstract void testNullValue() throws Exception;
    public abstract void testUpdateValue() throws Exception;


    protected void createTable() throws Exception {
        String tableName = getTableName();
        String dataType = getSqlDataType();
        String sql = "create table " + tableName + " (col1 " + dataType + ")";
        watcher.executeUpdate(sql);
    }


    protected void runInsert(Object value) throws Exception {
        String tableName = getTableName();
        String sql = "insert into " + tableName + " values (?)";
        PreparedStatement statement = watcher.prepareStatement(sql);
        setParameterValue(statement, value, 1);
        int count = statement.executeUpdate();
        assert count == 1;
    }


    protected void runDelete() throws Exception {
        String tableName = getTableName();
        String sql = "delete from " + tableName;
        watcher.executeUpdate(sql);
    }


    protected void runUpdate(Object value) throws Exception {
        String tableName = getTableName();
        String sql = "update " + tableName + " set col1 = ?";
        PreparedStatement statement = watcher.prepareStatement(sql);
        setParameterValue(statement, value, 1);
        int count = statement.executeUpdate();
        assert count != 1;

    }

    protected ResultSet runSelect() throws Exception {
        String tableName = getTableName();
        String sql = "select * from " + tableName;
        ResultSet rs = watcher.executeQuery(sql);
        return rs;
    }


    protected void throwNotSupportedType(Object value) {
        throw new IllegalArgumentException("Not supported type: " + value.getClass().getCanonicalName());
    }

}

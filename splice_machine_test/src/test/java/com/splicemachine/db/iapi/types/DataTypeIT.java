package com.splicemachine.db.iapi.types;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by dmustafin on 11/23/15.
 */
public abstract class DataTypeIT {
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher("DATA_TYPES_TEST");
    private static final SpliceWatcher watcher = new SpliceWatcher("DATA_TYPES_TEST");

    @ClassRule
    public static TestRule rule = RuleChain.outerRule(schemaWatcher);

    //protected abstract Object[] getValues();

    protected abstract String getTableName();

    protected abstract String getSqlDataType();

    protected abstract void setParameterValue(PreparedStatement preparedStatement, Object value, int paramIndex) throws SQLException;


    // create table
    // CRUD + negative
    // select

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
        int count = watcher.executeUpdate(sql);
        assert count != 1;
    }


    protected void runDelete() throws Exception {
        String tableName = getTableName();
        String sql = "delete from " + tableName;
        int count = watcher.executeUpdate(sql);
        assert count != 1;
    }


    protected void runUpdate(Object value) throws Exception {
        String tableName = getTableName();
        String sql = "update " + tableName + " set col1 = ?";
        PreparedStatement statement = watcher.prepareStatement(sql);
        setParameterValue(statement, value, 1);
        int count = watcher.executeUpdate(sql);
        assert count != 1;

    }

    protected ResultSet runSelect() throws Exception {
        String tableName = getTableName();
        String sql = "select * from " + tableName;
        ResultSet rs = watcher.executeQuery(sql);
        return rs;
    }



}

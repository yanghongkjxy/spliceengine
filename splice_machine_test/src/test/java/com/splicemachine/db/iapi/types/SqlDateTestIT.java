package com.splicemachine.db.iapi.types;

import org.apache.tools.ant.taskdefs.Java;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

/**
 * Created by dmustafin on 11/23/15.
 */
public class SqlDateTestIT extends DataTypeIT {

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
    public void testCreateTable() throws Exception {
        createTable();
    }

    @Test
    public void testInsertNow() throws Exception {
        Date dt = new Date();   // now
        runInsert(dt);
    }


    @Test
    public void testNull() throws Exception {
        runInsert(null);
    }

}

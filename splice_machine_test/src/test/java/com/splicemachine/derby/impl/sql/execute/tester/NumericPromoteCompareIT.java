package com.splicemachine.derby.impl.sql.execute.tester;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the implicit promotion and cast logic as implemented
 * in BinaryComparisonOperatorNode. There is some indirect
 * coverage in NumericConstantsIT and DataTypeCorrectnessIT too.
 */
public class NumericPromoteCompareIT {

	// This test class was added as part of fix for DB-1001.
	
    private static final String CLASS_NAME = NumericPromoteCompareIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private static String tableName1 = "customer";
    private static String TABLE_NAME_2 = "tableA";

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher tableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2, CLASS_NAME, "(id int, val decimal(12,2))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(tableWatcher2)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/NumericPromoteCompareIT.sql", CLASS_NAME));

    @Test
    public void testMultipleTypes() throws Exception {
    	// Compare INT to the rest
        assertCount(5, tableName1, "cust_id_int", "=", "cust_id_int");
        assertCount(5, tableName1, "cust_id_int", "=", "cust_id_sml");
        assertCount(4, tableName1, "cust_id_int", "=", "cust_id_dec");
        assertCount(4, tableName1, "cust_id_int", "=", "cust_id_num");

        // Compare SMALLINT to the rest
        assertCount(5, tableName1, "cust_id_sml", "=", "cust_id_int");
        assertCount(5, tableName1, "cust_id_sml", "=", "cust_id_sml");
        assertCount(4, tableName1, "cust_id_sml", "=", "cust_id_dec");
        assertCount(4, tableName1, "cust_id_sml", "=", "cust_id_num");

        // Compare DECIMAL to the rest
        assertCount(4, tableName1, "cust_id_dec", "=", "cust_id_int");
        assertCount(4, tableName1, "cust_id_dec", "=", "cust_id_sml");
        assertCount(5, tableName1, "cust_id_dec", "=", "cust_id_dec");
        assertCount(5, tableName1, "cust_id_dec", "=", "cust_id_num");

        // Compare NUMERIC (which is same as DECIMAL) to the rest
        assertCount(4, tableName1, "cust_id_num", "=", "cust_id_int");
        assertCount(4, tableName1, "cust_id_num", "=", "cust_id_sml");
        assertCount(5, tableName1, "cust_id_num", "=", "cust_id_dec");
        assertCount(5, tableName1, "cust_id_num", "=", "cust_id_num");
    }

    // TODO move to proper place on refactoring
    // SQLDecimal rounding ITs

    @Test
    public void testSQLDecimalRoundingInline() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("values cast (1.445 as DECIMAL(5,2))");
        rs.next();
        BigDecimal result = rs.getBigDecimal(1);
        assertThat("Rounding must be half-up", result, equalTo(new BigDecimal(BigInteger.valueOf(145), 2)));
    }

    @Test
    public void testSQLDecimalRoundingAdding() throws Exception {
        methodWatcher.execute(format("insert into %s values (1, 4.4449 + 5.0001)", tableWatcher2));
        ResultSet rs = methodWatcher.executeQuery(format("select val from %s where id = 1", tableWatcher2));
        rs.next();
        BigDecimal value = rs.getBigDecimal(1);
        assertThat("Values must be rounded half-up", value, equalTo(new BigDecimal(BigInteger.valueOf(945), 2)));
    }

    @Test
    public void testSQLDecimalRoundingDivision() throws Exception {
        methodWatcher.execute(format("insert into %s values (2, 10.0/3)", tableWatcher2));
        ResultSet rs = methodWatcher.executeQuery(format("select val from %s where id = 2", tableWatcher2));
        rs.next();
        BigDecimal value = rs.getBigDecimal(1);
        assertThat("Values must be rounded half-up", value, equalTo(new BigDecimal(BigInteger.valueOf(333), 2)));
    }

    @Test
    public void testSQLDecimalRoundingDivision2() throws Exception {
        methodWatcher.execute(format("insert into %s values (3, 9.375/3)", tableWatcher2));
        ResultSet rs = methodWatcher.executeQuery(format("select val from %s where id = 3", tableWatcher2));
        rs.next();
        BigDecimal value = rs.getBigDecimal(1);
        assertThat("Values must be rounded half-up", value, equalTo(new BigDecimal(BigInteger.valueOf(313), 2)));
    }

    // The following were lifted form NumericConstantsIT.
    // Might be good to consolidate them.
    
    /**
     * Executes two queries:
     *
     * SELECT * FROM [table] WHERE [operandOne] [operator] [operandTwo]
     * AND
     * SELECT * FROM [table] WHERE [operandTwo] [operator] [operandOne]
     */
    private void assertCount(int expectedCount, String table, String operandOne, String operator, Object operandTwo) throws Exception {
        String SQL_TEMPLATE = "select * from %s where %s %s %s";
        assertCount(expectedCount, format(SQL_TEMPLATE, table, operandOne, operator, operandTwo));
        String operatorTwo = newOperator(operator);
        assertCount(expectedCount, format(SQL_TEMPLATE, table, operandTwo, operatorTwo, operandOne));
    }

    private void assertCount(int expectedCount, String sql) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(format("count mismatch for sql='%s'", sql), expectedCount, count(rs));
    }

    private void assertException(String sql, Class expectedException, String expectedMessage) throws Exception {
        try {
            methodWatcher.executeQuery(sql);
            fail();
        } catch (Exception e) {
            assertEquals(expectedException, e.getClass());
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    private static int count(ResultSet rs) throws SQLException {
        int count = 0;
        while (rs.next()) {
            count++;
        }
        return count;
    }

    private static String newOperator(String operator) {
        if ("<".equals(operator.trim())) {
            return ">";
        }
        if (">".equals(operator.trim())) {
            return "<";
        }
        if ("<=".equals(operator.trim())) {
            return ">=";
        }
        if (">=".equals(operator.trim())) {
            return "<=";
        }
        return operator;
    }

}

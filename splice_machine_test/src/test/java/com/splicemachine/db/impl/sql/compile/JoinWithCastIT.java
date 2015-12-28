package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.Rows;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;

/**
 * Test to check join by columns with different types
 * @author apaslavsky
 */
public class JoinWithCastIT extends SpliceUnitTest {
    private static final String CLASS_NAME = JoinWithCastIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).around(spliceSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    @SuppressWarnings("unchecked")
    public static void createTable() throws Exception {
        new TableCreator(spliceClassWatcher.createConnection()).
                withCreate("CREATE TABLE TS_VARCHAR (TEXT1 VARCHAR(100), VARCHAR_REF VARCHAR(50))").
                withInsert("INSERT INTO TS_VARCHAR VALUES (?, ?)").withRows(
                Rows.rows(
                        Rows.row("TEST1", "123456"),
                        Rows.row("TEST2", "654321")
                )
        ).create();

        new TableCreator(spliceClassWatcher.createConnection()).
                withCreate("CREATE TABLE TS_BIGINT (TEXT2 VARCHAR(100), BIGINT_REF BIGINT)").
                withInsert("INSERT INTO TS_BIGINT VALUES (?, ?)").withRows(
                Rows.rows(
                        Rows.row("TEST3", 123456),
                        Rows.row("TEST4", 654321)
                )
        ).create();
    }

    @Ignore("DB-4377")
    @Test
    public void testSelectWithOneCast() throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery("SELECT * FROM TS_BIGINT, TS_VARCHAR WHERE CAST(VARCHAR_REF AS BIGINT)=BIGINT_REF");
        Assert.assertEquals(2, resultSetSize(resultSet));
    }

    @Test
    public void testSelectWithTwoCasts() throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery("SELECT * FROM TS_BIGINT, TS_VARCHAR WHERE CAST(VARCHAR_REF AS BIGINT)=CAST(BIGINT_REF AS BIGINT)");
        Assert.assertEquals(2, resultSetSize(resultSet));
    }

    @Test
    public void testSelectWithSpliceProperties() throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery("SELECT * FROM TS_VARCHAR, TS_BIGINT --splice-properties joinStrategy=NESTEDLOOP \nWHERE CAST(VARCHAR_REF AS BIGINT)=BIGINT_REF");
        Assert.assertEquals(2, resultSetSize(resultSet));
    }
}
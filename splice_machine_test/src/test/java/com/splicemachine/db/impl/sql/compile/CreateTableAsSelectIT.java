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
 * Test to check table creation from another select
 * @author apaslavsky
 */
public class CreateTableAsSelectIT extends SpliceUnitTest {
    private static final String CLASS_NAME = CreateTableAsSelectIT.class.getSimpleName().toUpperCase();
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

    @Test
    public void testCreateAsSelectWithoutProperties() throws Exception {
        methodWatcher.execute("CREATE TABLE TS_NEW1 AS " +
                "SELECT TEXT1, TEXT2 FROM TS_VARCHAR, TS_BIGINT " +
                "WHERE CAST(VARCHAR_REF AS BIGINT) = CAST(BIGINT_REF AS BIGINT) " +
                "WITH DATA");
        ResultSet resultSet = methodWatcher.executeQuery("SELECT count(*) FROM TS_NEW1");
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(2, resultSet.getInt(1));
        resultSet.close();
    }

    @Ignore("DB-4362")
    @Test
    public void testCreateAsSelectWithProperties() throws Exception {
        methodWatcher.execute("CREATE TABLE TS_NEW2 AS " +
                "SELECT TEXT1, TEXT2 FROM TS_VARCHAR, TS_BIGINT --splice-properties joinStrategy=NESTEDLOOP\n" +
                "WHERE CAST(VARCHAR_REF AS BIGINT) = BIGINT_REF " +
                "WITH DATA");
        ResultSet resultSet = methodWatcher.executeQuery("SELECT count(*) FROM TS_NEW2");
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(2, resultSet.getInt(1));
        resultSet.close();
    }
}
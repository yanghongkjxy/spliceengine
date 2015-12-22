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
 * Integration tests for {@link OrNode}
 * @author Andrey Paslavsky
 */
public class OrNodeIT extends SpliceUnitTest {
    private static final String CLASS_NAME = OrNodeIT.class.getSimpleName().toUpperCase();
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
                withCreate("create table t_date_with_pk (tst_data date primary key)").
                withInsert("insert into t_date_with_pk (tst_data) values ?").withRows(
                Rows.rows(
                        Rows.row("2013-03-01 00:00:00"),
                        Rows.row("2013-03-02 00:00:00"),
                        Rows.row("2013-03-03 00:00:00"),
                        Rows.row("2013-03-04 00:00:00"),
                        Rows.row("2013-03-05 00:00:00"),
                        Rows.row("2013-03-06 00:00:00"),
                        Rows.row("2013-03-07 00:00:00"),
                        Rows.row("2013-03-08 00:00:00"),
                        Rows.row("2013-03-09 00:00:00"),
                        Rows.row("2013-03-10 00:00:00")
                )
        ).create();
    }

    @Test
    public void testOrBetweenTwoDateColumns() throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where tst_data = '2013-03-02 00:00:00' or tst_data = '2013-03-01 00:00:00'", "t_date_with_pk"));
        Assert.assertEquals(2, resultSetSize(resultSet));
    }

    @Test
    public void testExplainOrBetweenTwoDateColumns() throws Exception {
        thirdRowContainsQuery(
                format("explain select * from %s where tst_data = '2013-03-02 00:00:00' or tst_data = '2013-03-01 00:00:00'", "t_date_with_pk"),
                "TST_DATA[0:1] IN (2013-03-02 00:00:00,2013-03-01 00:00:00)",
                methodWatcher);
    }
}

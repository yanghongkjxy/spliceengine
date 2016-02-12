package com.splicemachine.derby.impl.stats;

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;

/**
 * Created by akorotenko on 1/25/16.
 */
public class IndexSelectivityIT extends SpliceUnitTest {

    public static final String TABLE_SCAN = "TableScan";
    public static final String INDEX_NAME = "FOO_IX";
    public static final String INDEX_LOOKUP = "IndexLookup";


    public static final String CLASS_NAME = IndexSelectivityIT.class.getSimpleName().toUpperCase();
    public static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    public static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    public static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("FOO_INDEX", CLASS_NAME,
            "(col1 int, col2 int)");

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);


    @ClassRule
    public static TestRule rule = RuleChain.outerRule(spliceSchemaWatcher)
            .around(spliceTableWatcher).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.setAutoCommit(true);

                        spliceClassWatcher.execute(format("create index foo_ix on %s(col2)", spliceTableWatcher));

                        spliceClassWatcher.executeUpdate(format("insert into %s (col1, col2) values (1,1)",
                                spliceTableWatcher));

                        int index = 1;
                        for (int i = 0; i < 10; i++) {
                            spliceClassWatcher.executeUpdate(format("insert into %s select %s from %s",
                                    spliceTableWatcher,
                                    "col1, col2" + "+" + (index = index * 2) + " as col2",
                                    spliceTableWatcher));
                            System.out.println(index);
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });


    @Ignore("DB-3377")
    @Test
    public void testShouldUseIndexAfterStatistics() throws Exception {
        thirdRowContainsQuery(format("explain select * from %s " +
                " where col2 = 1000 ", spliceTableWatcher), TABLE_SCAN, methodWatcher);


        fourthRowContainsQuery(format("explain select * from %s --splice-properties index=foo_ix\n " +
                " where col2 = 1000 ", spliceTableWatcher), INDEX_NAME, methodWatcher);

        methodWatcher.execute(format("analyze table %s", spliceTableWatcher));

        thirdRowContainsQuery(format("explain select * from %s " +
                " where col2 = 1000 ", spliceTableWatcher), INDEX_LOOKUP, methodWatcher);

        fourthRowContainsQuery(format("explain select * from %s " +
                " where col2 = 1000 ", spliceTableWatcher), INDEX_NAME, methodWatcher);

    }
}

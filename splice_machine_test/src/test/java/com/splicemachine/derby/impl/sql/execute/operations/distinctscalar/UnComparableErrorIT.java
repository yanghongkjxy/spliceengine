package com.splicemachine.derby.impl.sql.execute.operations.distinctscalar;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLSyntaxErrorException;

public class UnComparableErrorIT {
    private static final String CLASS_NAME = UnComparableErrorIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    public static final String TABLE_NAME = "pictures";
    private static SpliceTableWatcher tableWatcher =
            new SpliceTableWatcher(TABLE_NAME, CLASS_NAME, "(name varchar(32) not null primary key, pic blob(16M), msg clob(1m))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).around(spliceSchemaWatcher).around(tableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testMessageBetweenClobAndBlob() throws Exception {
        try {
            spliceClassWatcher.executeQuery(String.format("select a.name as double_one, b.name as double_two " +
                    "from %s as a, %s as b " +
                    "where a.name < b.name and a.pic = b.pic", TABLE_NAME, TABLE_NAME));
            Assert.fail();
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals("42822", e.getSQLState());
        }
    }

    @Test
    public void testMessageBetweenBlobAndBlob() throws Exception {
        try {
            spliceClassWatcher.executeQuery(String.format("select a.name as double_one, b.name as double_two " +
                    "from %s as a, %s as b " +
                    "where a.name < b.name and a.msg = b.pic", TABLE_NAME, TABLE_NAME));
            Assert.fail();
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals("42818", e.getSQLState());
        }
    }
}
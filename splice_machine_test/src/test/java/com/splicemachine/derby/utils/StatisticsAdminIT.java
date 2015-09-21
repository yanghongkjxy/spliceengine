package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
@Category(SerialTest.class)
public class StatisticsAdminIT{
    private static final String SCHEMA=StatisticsAdminIT.class.getSimpleName().toUpperCase();
    private static final String SCHEMA2=SCHEMA+"2";

    private static final SpliceWatcher spliceClassWatcher=new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher2=new SpliceSchemaWatcher(SCHEMA2);

    private static final String TABLE_EMPTY="EMPTY";
    private static final String TABLE_OCCUPIED="OCCUPIED";
    private static final String TABLE_OCCUPIED2="OCCUPIED2";
    private static final String TABLE_MIXED_CASE = "\"MixedCase\"";

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher).around(spliceSchemaWatcher2);


    @Rule
    public final SpliceWatcher methodWatcher=new SpliceWatcher(SCHEMA);

    private TestConnection conn;

    @BeforeClass
    public static void createSharedTables() throws Exception{
        Connection conn=spliceClassWatcher.getOrCreateConnection();
        conn.setSchema(SCHEMA);
        doCreateSharedTables(conn);
        conn.setSchema(SCHEMA2);
        doCreateSharedTables(conn);
        conn.setSchema(SCHEMA);
    }

    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }


    @Test
    public void testCanCollectForMixedCaseTable() throws Exception{
        //regression test for DB-4188
        Connection conn = methodWatcher.getOrCreateConnection();
        try(CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?)")){
            cs.setString(1,SCHEMA);
            cs.setString(2,TABLE_MIXED_CASE);
            cs.setBoolean(3,false);

            //all we need to do here is verify that no errors are thrown, in order to protect against regression
            cs.execute();
        }
    }

    @Test
    public void testTableStatisticsAreCorrectForEmptyTable() throws Exception{
        conn.collectStats(SCHEMA,TABLE_EMPTY);
        /*
         * Now we need to make sure that the statistics were properly recorded.
         *
         * There are 2 tables in particular of interest: the Column stats, and the row stats. Since
         * the emptyTable is not configured to collect any column stats, we only check the row stats for
         * values.
         */
        long conglomId=SpliceAdmin.getConglomNumbers(conn,SCHEMA,TABLE_EMPTY)[0];
        try(PreparedStatement check=conn.prepareStatement("select * from sys.systablestats where conglomerateId = ?")){
            check.setLong(1,conglomId);
            try(ResultSet resultSet=check.executeQuery()){
                Assert.assertTrue("Unable to find statistics for table!",resultSet.next());
                Assert.assertEquals("Incorrect row count!",0l,resultSet.getLong(6));
                Assert.assertEquals("Incorrect partition size!",0l,resultSet.getLong(7));
                Assert.assertEquals("Incorrect row width!",0l,resultSet.getInt(8));
            }
        }
    }

    @Test
    public void testTableStatisticsCorrectForOccupiedTable() throws Exception{
        conn.collectStats(SCHEMA,TABLE_OCCUPIED);
        /*
         * Now we need to make sure that the statistics were properly recorded.
         *
         * There are 2 tables in particular of interest: the Column stats, and the row stats. Since
         * the emptyTable is not configured to collect any column stats, we only check the row stats for
         * values.
         */
        long conglomId=SpliceAdmin.getConglomNumbers(conn,SCHEMA,TABLE_OCCUPIED)[0];
        try(PreparedStatement check=conn.prepareStatement("select * from sys.systablestats where conglomerateId = ?")){
            check.setLong(1,conglomId);
            try(ResultSet resultSet=check.executeQuery()){
                Assert.assertTrue("Unable to find statistics for table!",resultSet.next());
                Assert.assertEquals("Incorrect row count!",1l,resultSet.getLong(6));
                /*
                 * We would love to assert specifics about the size of the partition and the width
                 * of the row, but doing so results in a fragile test--the size of the row changes after the
                 * transaction system performed read resolution, so if you wait for long enough (i.e. have a slow
                 * enough system) this test will end up breaking. However, we do know that there is only a single
                 * row in this table, so the partition size should be the same as the avgRowWidth
                 */
                long partitionSize=resultSet.getLong(7);
                long rowWidth=resultSet.getLong(8);
                Assert.assertTrue("partition size != row width!",partitionSize==rowWidth);
            }
        }
    }

    @Test
    public void testCanEnableColumnStatistics() throws Exception{

        try(CallableStatement cs=conn.prepareCall("call SYSCS_UTIL.ENABLE_COLUMN_STATISTICS(?,?,?)")){
            cs.setString(1,SCHEMA);
            cs.setString(2,TABLE_EMPTY);
            cs.setString(3,"A");

            cs.execute(); //shouldn't get an error
        }

        //make sure it's enabled
        try(PreparedStatement ps=conn.prepareStatement(
                "select c.* from "+
                        "sys.sysschemas s, sys.systables t, sys.syscolumns c "+
                        "where s.schemaid = t.schemaid "+
                        "and t.tableid = c.referenceid "+
                        "and s.schemaname = ?"+
                        "and t.tablename = ?"+
                        "and c.columnname = ?")){
            ps.setString(1,SCHEMA);
            ps.setString(2,TABLE_EMPTY);
            ps.setString(3,"A");
            try(ResultSet resultSet=ps.executeQuery()){
                Assert.assertTrue("No columns found!",resultSet.next());
                boolean statsEnabled=resultSet.getBoolean("collectstats");
                Assert.assertTrue("Stats were not enabled!",statsEnabled);
            }

            //now verify that it can be disabled as well
            try(CallableStatement cs=conn.prepareCall("call SYSCS_UTIL.DISABLE_COLUMN_STATISTICS(?,?,?)")){
                cs.setString(1,SCHEMA);
                cs.setString(2,TABLE_EMPTY);
                cs.setString(3,"A");

                cs.execute(); //shouldn't get an error
            }

            //make sure it's disabled
            try(ResultSet resultSet=ps.executeQuery()){
                Assert.assertTrue("No columns found!",resultSet.next());
                Assert.assertFalse("Stats were still enabled!",resultSet.getBoolean("collectstats"));
            }
        }
    }

    @Test
    public void testDropSchemaStatistics() throws Exception{

        conn.collectStats(SCHEMA,null);
        conn.collectStats(SCHEMA2,null);

        // Check collected stats for both schemas
        verifyStatsCounts(conn,SCHEMA,null,5,3);
        verifyStatsCounts(conn,SCHEMA2,null,5,3);

        // Drop stats for schema 1
        try(CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.DROP_SCHEMA_STATISTICS(?)")){
            callableStatement.setString(1,SCHEMA);
            callableStatement.execute();
        }

        // Make sure only schema 1 stats were dropped, not schema 2 stats
        verifyStatsCounts(conn,SCHEMA,null,0,0);
        verifyStatsCounts(conn,SCHEMA2,null,5,3);

        // Drop stats again for schema 1 to make sure it works with no stats
        try(CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.DROP_SCHEMA_STATISTICS(?)")){
            callableStatement.setString(1,SCHEMA);
            callableStatement.execute();
        }

        // Make sure only schema 1 stats were dropped, not schema 2 stats
        verifyStatsCounts(conn,SCHEMA,null,0,0);
        verifyStatsCounts(conn,SCHEMA2,null,5,3);

        try(CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.DROP_SCHEMA_STATISTICS(?)")){
            callableStatement.setString(1,SCHEMA2);
            callableStatement.execute();
        }

        // Make sure stats are gone for both schemas
        verifyStatsCounts(conn,SCHEMA,null,0,0);
        verifyStatsCounts(conn,SCHEMA2,null,0,0);
    }

    @Test
    public void testDropTableStatistics() throws Exception{
        conn.collectStats(SCHEMA,null);

        // Check collected stats for both schemas
        verifyStatsCounts(conn,SCHEMA,null,5,3);

        // Drop stats for schema 1, table 1
        try(CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.DROP_TABLE_STATISTICS(?,?)")){
            callableStatement.setString(1,SCHEMA);
            callableStatement.setString(2,TABLE_OCCUPIED);
            callableStatement.execute();
        }

        // Make sure stats for both table and index were dropped in schema 1.
        verifyStatsCounts(conn,SCHEMA,null,3,2);
        verifyStatsCounts(conn,SCHEMA,TABLE_OCCUPIED,0,0);
        verifyStatsCounts(conn2, SCHEMA2, null, 5, 3);

        // Drop stats again for schema 1 to make sure it works with no stats
        try(CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.DROP_TABLE_STATISTICS(?,?)")){
            callableStatement.setString(1,SCHEMA);
            callableStatement.setString(2,TABLE_OCCUPIED);
            callableStatement.execute();
        }

        // Same as prior check
        verifyStatsCounts(conn,SCHEMA,null,3,2);
        verifyStatsCounts(conn,SCHEMA,TABLE_OCCUPIED,0,0);
        verifyStatsCounts(conn2, SCHEMA2, null, 5, 3);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void verifyStatsCounts(Connection conn,String schema,String table,int tableStatsCount,int colStatsCount) throws Exception{
        if(table==null)
            verifySchemaStatsCount(conn,schema,tableStatsCount,colStatsCount);
        else
            verifyTableStatsCount(conn,schema,table,tableStatsCount,colStatsCount);
    }

    private void verifyTableStatsCount(Connection conn,String schema,String table,int tableStatsCount,int colStatsCount) throws Exception{
        assert table!=null: "Programmer error: cannot verify stats with no table name!";
        try(PreparedStatement check=
                conn.prepareStatement("select count(*) from sys.systablestatistics where schemaname = ? and tablename = ?")){
            check.setString(1,schema);
            check.setString(2,table);
            assertCorrectStatsCount(tableStatsCount,check);
        }

        try(PreparedStatement check=
                conn.prepareStatement("select count(*) from sys.syscolumnstatistics where schemaname = ? and tablename = ?")){
            check.setString(1,schema);
            check.setString(2,table);
            assertCorrectStatsCount(colStatsCount,check);
        }
    }

    private void verifySchemaStatsCount(Connection conn,String schema,int tableStatsCount,int colStatsCount) throws Exception{
        String schemaTableStatsCountSql="select count(*) from sys.systablestatistics where schemaname = ?";
        try(PreparedStatement check= conn.prepareStatement(schemaTableStatsCountSql)){
            check.setString(1,schema);
            assertCorrectStatsCount(tableStatsCount,check);
        }

        String schemaColumnStatsCountSql="select count(*) from sys.syscolumnstatistics where schemaname = ?";
        try(PreparedStatement check= conn.prepareStatement(schemaColumnStatsCountSql)){
            check.setString(1,schema);
            assertCorrectStatsCount(colStatsCount,check);
        }
    }

    private void assertCorrectStatsCount(int colStatsCount,PreparedStatement check) throws SQLException{
        try(ResultSet resultSet=check.executeQuery()){
            Assert.assertTrue("Unable to count stats for schema",resultSet.next());
            Assert.assertEquals("Incorrect row count",colStatsCount,resultSet.getInt(1));
        }
    }

    @SuppressWarnings("unchecked")
    private static void doCreateSharedTables(Connection conn) throws Exception{
        new TableCreator(conn)
                .withCreate("create table "+TABLE_OCCUPIED+" (a int)")
                .withInsert("insert into "+TABLE_OCCUPIED+" (a) values (?)")
                .withIndex("create index idx_o on "+TABLE_OCCUPIED+" (a)")
                .withRows(rows(row(1)))
                .create();

        new TableCreator(conn)
                .withCreate("create table "+TABLE_OCCUPIED2+" (a int)")
                .withInsert("insert into "+TABLE_OCCUPIED2+" (a) values (?)")
                .withIndex("create index idx_o2 on "+TABLE_OCCUPIED2+" (a)")
                .withRows(rows(row(101)))
                .create();

        new TableCreator(conn)
                .withCreate("create table " + TABLE_EMPTY + " (a int)")
                .create();

        new TableCreator(conn)
                .withCreate("create table " + TABLE_MIXED_CASE + " (a int)")
                .create();
    }
}

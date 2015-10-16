package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigDecimal;
import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

public class UpdateOperationIT{

    private static final String SCHEMA=UpdateOperationIT.class.getSimpleName().toUpperCase();

    private static final SpliceWatcher spliceClassWatcher=new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher=new SpliceWatcher(SCHEMA);

    private TestConnection conn;

    @BeforeClass
    public static void createSharedTables() throws Exception{
        Connection connection=spliceClassWatcher.getOrCreateConnection();

        new TableCreator(connection)
                .withCreate("create table LOCATION (num int, addr varchar(50), zip char(5))")
                .withInsert("insert into LOCATION values(?,?,?)")
                .withRows(rows(
                                row(100,"100","94114"),
                                row(200,"200","94509"),
                                row(300,"300","34166"))
                )
                .create();

        new TableCreator(connection)
                .withCreate("create table NULL_TABLE (addr varchar(50), zip char(5))")
                .create();

        new TableCreator(connection)
                .withCreate("create table SHIPMENT (cust_id int)")
                .withInsert("insert into SHIPMENT values(?)")
                .withRows(rows(row(102),row(104)))
                .create();

        new TableCreator(connection)
                .withCreate("create table CUSTOMER (cust_id int, status boolean, level int)")
                .withInsert("insert into CUSTOMER values(?,?,?)")
                .withRows(rows(row(101,true,1),row(102,true,2),row(103,true,3),row(104,true,4),row(105,true,5)))
                .create();

        new TableCreator(connection)
                .withCreate("create table CUSTOMER_TEMP (cust_id int, status boolean, level int)")
                .withInsert("insert into CUSTOMER_TEMP values(?,?,?)")
                .withRows(rows(row(101,true,1),row(102,true,2),row(103,true,3),row(104,true,4),row(105,true,5)))
                .create();

        new TableCreator(connection)
                .withCreate("create table NULL_TABLE2 (col1 varchar(50), col2 char(5), col3 int)")
                .create();

        try(CallableStatement cs=connection.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
            cs.setString(1,SCHEMA);
            cs.execute();
        }
    }


    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDownTest() throws Exception{
        conn.rollback();
    }

    /*regression test for DB-2204*/
    @Test
    public void testUpdateDoubleIndexFieldWorks() throws Exception{
        int updated;
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table dc (dc decimal(10,2))");
            s.executeUpdate("insert into dc values (10)");

            s.execute("create index didx on dc (dc)");
            s.execute("create unique index duniq on dc (dc)");

            updated=s.executeUpdate("update dc set dc = dc+1.1");

            //make sure that the update worked
            BigDecimal expected=new BigDecimal("11.10");
            String[] queries=new String[]{
                    "select dc from dc",
                    "select dc from dc --SPLICE-PROPERTIES index=DIDX\n",
                    "select dc from dc --SPLICE-PROPERTIES index=DUNIQ\n"
            };
            for(String query : queries){
                try(ResultSet actualRs=s.executeQuery(query)){
                    Assert.assertTrue("No response returned!",actualRs.next());
                    BigDecimal actualVal=actualRs.getBigDecimal(1);
                    assertEquals("Incorrect value returned!",expected,actualVal);
                }
            }
        }

        //make base table and index have expected number of rows
        assertEquals("Incorrect number of reported updates!",1,updated);
        assertEquals(1L,conn.getCount("select count(*) from dc"));
        assertEquals(1L,conn.getCount("select count(*) from dc --SPLICE-PROPERTIES index=DIDX\n"));
        assertEquals(1L,conn.getCount("select count(*) from dc --SPLICE-PROPERTIES index=DUNIQ\n"));
    }

    /*regression test for Bug 889*/
    @Test
    public void testUpdateSetNullLong() throws Exception{

        new TableCreator(conn)
                .withCreate("create table c (k int, l int)")
                .withInsert("insert into c (k,l) values (?,?)")
                .withRows(rows(row(1,2),row(2,4)))
                .create();

        try(Statement s=conn.createStatement()){
            int updated=s.executeUpdate("update c set k = NULL where l = 2");

            assertEquals("incorrect num rows updated!",1L,updated);

            ResultSet rs=s.executeQuery("select * from c");
            assertEquals(""+
                            "K  | L |\n"+
                            "----------\n"+
                            "  2  | 4 |\n"+
                            "NULL | 2 |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
        assertEquals("incorrect num rows present!",2L,conn.getCount("select count(*) from c"));
    }

    @Test
    public void testUpdate() throws Exception{
        try(Statement s=conn.createStatement()){
            int updated=s.executeUpdate("update LOCATION set addr='240' where num=100");
            assertEquals("Incorrect num rows updated!",1,updated);
            ResultSet rs=s.executeQuery("select * from LOCATION where num = 100");
            assertEquals(""+
                            "NUM |ADDR | ZIP  |\n"+
                            "-------------------\n"+
                            " 100 | 240 |94114 |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testUpdateMultipleColumns() throws Exception{
        try(Statement s=conn.createStatement()){
            int updated=s.executeUpdate("update LOCATION set addr='900',zip='63367' where num=300");
            assertEquals("incorrect number of records updated!",1,updated);
            ResultSet rs=s.executeQuery("select * from LOCATION where num=300");
            assertEquals(""+
                            "NUM |ADDR | ZIP  |\n"+
                            "-------------------\n"+
                            " 300 | 900 |63367 |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testWithoutWhere() throws Exception{
        try(PreparedStatement ps=conn.prepareStatement("insert into NULL_TABLE values (null,null)")){
            ps.execute();
        }

        try(PreparedStatement ps=conn.prepareStatement("update NULL_TABLE set addr=?,zip=?")){
            ps.setString(1,"2269 Concordia Drive");
            ps.setString(2,"65203");
            int updated=ps.executeUpdate();
            assertEquals("Incorrect number of records updated",1,updated);
        }

        try(Statement s=conn.createStatement()){
            ResultSet rs=s.executeQuery("select addr,zip from NULL_TABLE");
            assertEquals(""+
                            "ADDR         | ZIP  |\n"+
                            "-----------------------------\n"+
                            "2269 Concordia Drive |65203 |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    /* Regression test for Bug #286 */
    @Test
    public void testUpdateFromValues() throws Exception{
        try(Statement s=conn.createStatement()){
            int updated=s.executeUpdate("update LOCATION set addr=(values '5') where num=100");
            assertEquals("Incorrect num rows updated!",1,updated);
            ResultSet rs=s.executeQuery("select * from LOCATION where num = 100");
            assertEquals(""+
                            "NUM |ADDR | ZIP  |\n"+
                            "-------------------\n"+
                            " 100 |  5  |94114 |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    /* regression test for Bug 289 */
    @Test
    public void testUpdateFromSubquery() throws Exception{
        new TableCreator(conn)
                .withCreate("create table b (num int, addr varchar(50), zip char(5))")
                .withInsert("insert into b values(?,?,?)")
                .withRows(rows(row(25,"100","94114")))
                .create();
        try(Statement s=conn.createStatement()){

            int updated=s.executeUpdate("update b set num=(select LOCATION.num from LOCATION where LOCATION.num = 100)");
            assertEquals("Incorrect num rows updated!",1,updated);
            ResultSet rs=s.executeQuery("select * from b where num = 100");
            assertEquals(""+
                            "NUM |ADDR | ZIP  |\n"+
                            "-------------------\n"+
                            " 100 | 100 |94114 |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    /* Regression test for Bug 682. */
    @Test
    public void testUpdateSetNullValues() throws Exception{
        try(PreparedStatement ps=conn.prepareStatement("insert into NULL_TABLE values (?,?)")){
            ps.setString(1,"900 Green Meadows Road");
            ps.setString(2,"65201");
            ps.execute();
        }

        //get initial count
        Long originalCount=conn.getCount("select count(*) from NULL_TABLE where zip = '65201'");

        //update to set a null entry
        try(Statement s=conn.createStatement()){
            int numChanged=s.executeUpdate("update NULL_TABLE set zip = null where zip = '65201'");
            assertEquals("Incorrect rows changed",1,numChanged);

            ResultSet rs=s.executeQuery("select * from NULL_TABLE where zip is null");
            assertEquals(""+
                            "ADDR          | ZIP |\n"+
                            "------------------------------\n"+
                            "900 Green Meadows Road |NULL |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        //make sure old value isn't there anymore
        long finalCount=conn.getCount("select count(*) from NULL_TABLE where zip = '65201'");
        assertEquals("Row was not removed from original set",originalCount-1l,finalCount);
    }

    /* Regression test for Bug 2572. */
    @Test
    public void testUpdateSetALLNullValues() throws Exception{
        try(PreparedStatement ps=conn.prepareStatement("insert into NULL_TABLE2 values (?,?,?)")){
            ps.setString(1,"900 Green Meadows Road");
            ps.setString(2,"65201");
            ps.setInt(3,10);
            ps.execute();
        }

        //get initial count
        Long count=conn.getCount("select count(*) from NULL_TABLE2 where col3 = 10");

        try(Statement s=conn.createStatement()){
            //update to set 1st column null
            int numChanged=s.executeUpdate("update NULL_TABLE2 set col1 = null where col3 = 10");
            assertEquals("Incorrect rows changed",count.longValue(),numChanged);

            ResultSet rs=s.executeQuery("select * from NULL_TABLE2 where col3 = 10");
            assertEquals(""+
                            "COL1 |COL2  |COL3 |\n"+
                            "-------------------\n"+
                            "NULL |65201 | 10  |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));

            numChanged=s.executeUpdate("update NULL_TABLE2 set col2 = null where col3 = 10");
            assertEquals("Incorrect rows changed",count.longValue(),numChanged);
            rs=s.executeQuery("select * from NULL_TABLE2 where col3 = 10");
            assertEquals(""+
                            "COL1 |COL2 |COL3 |\n"+
                            "------------------\n"+
                            "NULL |NULL | 10  |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));

            numChanged=s.executeUpdate("update NULL_TABLE2 set col3 = null where col3 = 10");
            assertEquals("Incorrect rows changed",count.longValue(),numChanged);
            rs=s.executeQuery("select * from NULL_TABLE2 where col3 is null");
            assertEquals(""+
                            "COL1 |COL2 |COL3 |\n"+
                            "------------------\n"+
                            "NULL |NULL |NULL |",
                    TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    /* Regression test for DB-1481, DB-2189 */
    @Test
    public void testUpdateOnAllColumnIndex() throws Exception{
        new TableCreator(conn)
                .withCreate("create table tab2 (c1 int not null primary key, c2 int, c3 int)")
                .withIndex("create index ti on tab2 (c1,c2 desc,c3)")
                .withInsert("insert into tab2 values(?,?,?)")
                .withRows(rows(
                        row(1,10,1),
                        row(2,20,1),
                        row(3,30,2),
                        row(4,40,2),
                        row(5,50,3),
                        row(6,60,3),
                        row(7,70,3)
                ))
                .create();

        try(Statement s=conn.createStatement()){
            int rows=s.executeUpdate("update tab2 set c2=99 where c3=2");

            assertEquals("incorrect num rows updated!",2L,rows);
            assertEquals("incorrect num rows present!",7L,conn.getCount("select count(*) from tab2"));
            assertEquals("incorrect num rows present!",7L,conn.getCount("select count(*) from tab2 --SPLICE-PROPERTIES index=ti"));

            ResultSet rs1=s.executeQuery("select * from tab2 where c3=2");
            String baseResult=TestUtils.FormattedResult.ResultFactory.toString(rs1);
            ResultSet rs2=s.executeQuery("select * from tab2 --SPLICE-PROPERTIES index=ti \n where c3=2");
            String indexResult=TestUtils.FormattedResult.ResultFactory.toString(rs2);

            String expected=""+
                    "C1 |C2 |C3 |\n"+
                    "------------\n"+
                    " 3 |99 | 2 |\n"+
                    " 4 |99 | 2 |";

            assertEquals("verify without index",expected,baseResult);
            assertEquals("verify using index",expected,indexResult);
        }
    }

    /* Test for DB-3160 */
    @Test
    public void testUpdateCompoundPrimaryKeyWithMatchingCompoundIndex() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table tab4 (c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3))");
            s.executeUpdate("create index tab4_idx on tab4 (c1,c2,c3)");
            s.executeUpdate("insert into tab4 values (10,10,10,10),(20,20,20,20),(30,30,30,30)");

            int rows=s.executeUpdate("update tab4 set c2=88, c3=888, c4=8888 where c1=10 or c1 = 20");

            assertEquals("incorrect num rows updated!",2L,rows);
            assertEquals("incorrect num rows present!",3L,conn.getCount("select count(*) from tab4"));
            assertEquals("incorrect num rows present!",3L,conn.getCount("select count(*) from tab4 --SPLICE-PROPERTIES index=tab4_idx"));
            assertEquals("expected this row to be deleted from index",0L,conn.getCount("select count(*) from tab4 --SPLICE-PROPERTIES index=tab4_idx\n where c2=10"));

            ResultSet rs1=s.executeQuery("select * from tab4 where c1=10 or c1 = 20");
            String baseResults=TestUtils.FormattedResult.ResultFactory.toString(rs1);
            ResultSet rs2=s.executeQuery("select * from tab4 --SPLICE-PROPERTIES index=tab4_idx \n where c1=10 or c1 = 20");
            String indexResults=TestUtils.FormattedResult.ResultFactory.toString(rs2);

            String expected=""+
                    "C1 |C2 |C3  | C4  |\n"+
                    "-------------------\n"+
                    "10 |88 |888 |8888 |\n"+
                    "20 |88 |888 |8888 |";

            assertEquals("verify without index",expected,baseResults);
            assertEquals("verify using index",expected,indexResults);
        }
    }

    @Test
    public void updateColumnWhichIsDescendingInIndexAndIsAlsoPrimaryKey() throws Exception{
        new TableCreator(conn)
                .withCreate("create table tab3 (a int primary key, b int, c int, d int)")
                .withIndex("create index index_tab3 on tab3 (a desc, b desc, c)")
                .withInsert("insert into tab3 values(?,?,?,?)")
                .withRows(rows(
                        row(10,10,10,10),
                        row(20,20,10,20),
                        row(30,30,20,30)
                ))
                .create();

        try(Statement s=conn.createStatement()){
            int rows=s.executeUpdate("update tab3 set a=99,b=99,c=99 where d=30");

            assertEquals("incorrect num rows updated!",1L,rows);
            assertEquals("incorrect num rows present!",3L,conn.getCount("select count(*) from tab3"));
            assertEquals("incorrect num rows present!",3L,conn.getCount("select count(*) from tab3 --SPLICE-PROPERTIES index=index_tab3"));

            ResultSet rs1=s.executeQuery("select * from tab3 where d=30");
            String baseTableResults=TestUtils.FormattedResult.ResultFactory.toString(rs1);
            ResultSet rs2=s.executeQuery("select * from tab3 --SPLICE-PROPERTIES index=index_tab3 \n where d=30");
            String indexResults=TestUtils.FormattedResult.ResultFactory.toString(rs2);

            String expected=""+
                    "A | B | C | D |\n"+
                    "----------------\n"+
                    "99 |99 |99 |30 |";

            assertEquals("verify without index",expected,baseTableResults);
            assertEquals("verify using index",expected,indexResults);
        }
    }

    /*
     * Regression test for DB-2007. Assert that this doesn't explode, and that
     * the NULL,NULL row isn't modified, so the number of rows modified = 0
     */
    @Test
    public void testUpdateOverNullIndexWorks() throws Exception{
        new TableCreator(conn)
                .withCreate("create table nt (id int primary key, a int, b int)")
                .withInsert("insert into nt values(?,?,?)")
                .withRows(rows(row(1,null,null),row(2,null,null),row(3,null,null),row(4,1,1)))
                .withIndex("create unique index nt_idx on nt (a)").create();


        try(Statement s=conn.createStatement()){
            // updates to null should remain null.  So we are testing setting null to null on base and index conglom.
            // Even though null to null rows are not physically changed they should still contribute to row count.
            assertEquals(4L,s.executeUpdate("update NT set a = a + 9"));

            // same thing, but updating using primary key
            assertEquals(1L,s.executeUpdate("update NT set a = a + 9 where id=1"));
        }

        assertEquals(4L,conn.getCount("select count(*) from nt"));
        assertEquals(4L,conn.getCount("select count(*) from nt --SPLICE-PROPERTIES index=nt_idx"));
        assertEquals(3L,conn.getCount("select count(*) from nt where a is null"));
        assertEquals(3L,conn.getCount("select count(*) from nt --SPLICE-PROPERTIES index=nt_idx\n where a is null"));
    }

    // If you change one of the following 'update over join' tests,
    // you probably need to make a similar change to DeleteOperationIT.

    @Test
    public void testUpdateOverNestedLoopJoin() throws Exception {
						doTestUpdateOverJoin("NESTEDLOOP",conn);
        }
    public void testUpdateOverBroadcastJoin() throws Exception{
        doTestUpdateOverJoin("BROADCAST",conn);
    }

    @Test
    public void testUpdateOverMergeSortJoin() throws Exception{
        doTestUpdateOverJoin("SORTMERGE",conn);
    }

    private void doTestUpdateOverJoin(String hint,TestConnection connection) throws Exception{
        String query=String.format("update CUSTOMER customer set CUSTOMER.status = 'false' \n"
                +"where not exists ( \n"
                +"  select 1 \n"
                +"  from SHIPMENT shipment --SPLICE-PROPERTIES joinStrategy=%s \n"
                +"  where CUSTOMER.cust_id = SHIPMENT.cust_id \n"
                +") \n",hint);
        int rows=connection.createStatement().executeUpdate(query);
        assertEquals("incorrect num rows updated!",3,rows);
    }

    @Test
    public void testUpdateMultiColumnMultiSubSyntax() throws Exception{
        String query="update customer \n"
                +"  set status = 'false', \n"
                +"  level = ( \n"
                +"    select level \n"
                +"    from customer_temp custtemp \n"
                +"    where custtemp.cust_id = customer.cust_id \n"
                +"  )";
        int rows=methodWatcher.executeUpdate(query);
        Assert.assertEquals("incorrect num rows updated!",5,rows);
    }

    @Test
    public void testUpdateMultiColumnOneSubSyntaxNoOuterWhere() throws Exception{
        int rows=doTestUpdateMultiColumnOneSubSyntax(null);
        Assert.assertEquals("incorrect num rows updated!",5,rows);
    }

    @Test
<<<<<<< HEAD
    public void testUpdateMultiColumnOneSubSyntaxWithOuterWhere() throws Exception {
        int rows = doTestUpdateMultiColumnOneSubSyntax(" where customer.cust_id <> 105");
        Assert.assertEquals("incorrect num rows updated!", 4, rows);
=======
    @Ignore("DB-")
    public void testUpdateMultiColumnOneSubSyntaxWithOuterWhere() throws Exception{
        int rows=doTestUpdateMultiColumnOneSubSyntax(" where customer.cust_id <> 105");
        Assert.assertEquals("incorrect num rows updated!",4,rows);
>>>>>>> 13eda46... Fixing UpdateOperationIT to be transactionally sane
    }

    // Used by previous tests (testUpdateMultiColumnOneSub*)
    private int doTestUpdateMultiColumnOneSubSyntax(String outerWhere) throws Exception{
        StringBuilder sb=new StringBuilder(200);
        sb.append("update customer \n");
        sb.append("  set (status, level) = ( \n");
        sb.append("    select customer_temp.status, customer_temp.level \n");
        sb.append("    from customer_temp \n");
        sb.append("    where customer_temp.cust_id = customer.cust_id \n");
        sb.append("  ) ");
        if(outerWhere!=null){
            sb.append(outerWhere);
        }
        String query=sb.toString();
        try(Statement statement=conn.createStatement()){
            return statement.executeUpdate(query);
        }
    }

}

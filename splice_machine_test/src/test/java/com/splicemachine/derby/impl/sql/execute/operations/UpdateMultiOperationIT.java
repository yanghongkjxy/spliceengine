package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.metrics.*;
import com.splicemachine.test_tools.Rows;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 8/18/15
 */
public class UpdateMultiOperationIT{

    private static final String SCHEMA = UpdateMultiOperationIT.class.getSimpleName().toUpperCase();

    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createTables() throws Exception{
        Connection conn = spliceClassWatcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table WAREHOUSE (w_id int NOT NULL,w_ytd DECIMAL(12,2), PRIMARY KEY(w_id))")
                .withInsert("insert into WAREHOUSE (w_id,w_ytd) values (?,?)")
                .withRows(Rows.rows(Rows.row(292,new BigDecimal("300000.00")),
                        Rows.row(293,new BigDecimal("1.00")),
                        Rows.row(300,new BigDecimal("1.00"))))
                .create();
    }

    @Test
    public void testCanRepeatedlyUpdateTheSameRowWithoutError() throws Exception{

        TestConnection conn = spliceClassWatcher.getOrCreateConnection();
        /*
         * DB-3676 means that we need to ensure that autocommit is on, because otherwise
         *  this test will take 900 years to finish, and that would suck
         */
        conn.setAutoCommit(true);
        try(PreparedStatement ps = conn.prepareStatement("update warehouse set w_ytd = w_ytd+? where w_id = ?")){
            ps.setInt(2,292);
            File f = new File(SpliceUnitTest.getResourceDirectory()+"updateValues.raw");
            try(BufferedReader br = new BufferedReader(new FileReader(f))){
                String line;
                while((line = br.readLine())!=null){
                    BigDecimal bd = new BigDecimal(line.trim());
                    ps.setBigDecimal(1,bd);
                    ps.execute(); //perform the update
                }
            }
        }

        try(ResultSet rs = conn.query("select * from warehouse where w_id = 292")){
            long rowCount = 0l;
            while(rs.next()){
                int wId = rs.getInt(1);
                Assert.assertFalse("Returned null!",rs.wasNull());
                Assert.assertEquals("Incorrect wId!",292,wId);

                BigDecimal value = rs.getBigDecimal(2);
                Assert.assertFalse("Returned null!",rs.wasNull());
                /*
                 * Note: this "correct" value is taken from Derby, which may not always be correct
                 * in reality(see DB-3675 for more information)
                 */
                Assert.assertEquals("Incorrect return value!",new BigDecimal("5428906.39"),value);
                rowCount++;
            }
            Assert.assertEquals("Incorrect row count!",1l,rowCount);
        }
    }

    @Test
    public void testRepeatedUpdateAndSelectIsCorrectIndexed() throws Exception{
        /*
         * Test that we can repeatedly update a record by 1 and then select it and the following is true:
         *
         * 1. We always get back the correct answer
         * 2. We return within a bounded amount of time (say, 100 ms) for 99% of runs
         *
         */
        long maxUpdateRuntime=TimeUnit.MILLISECONDS.toNanos(100);
        long maxSelectRuntime=TimeUnit.MILLISECONDS.toNanos(50);
        int numRuns = 10000;

        LatencyTimer updateTimer =Metrics.sampledLatencyTimer(Math.min(numRuns+1,10000));
        LatencyTimer selectTimer =Metrics.sampledLatencyTimer(Math.min(numRuns+1,10000));
        TestConnection conn = spliceClassWatcher.getOrCreateConnection();

        conn.setAutoCommit(true);
        String updateSql = "update warehouse set w_ytd = w_ytd+1 where w_id = ?";
        String selectSql = "select * from warehouse where w_id = ?";
        try(Statement s = conn.createStatement()){
            s.execute("create index YTD_IDX on warehouse(w_ytd)");
        }

        try(PreparedStatement updatePs = conn.prepareStatement(updateSql);
            PreparedStatement selectPs = conn.prepareStatement(selectSql)){
            int w_id=300;
            updatePs.setInt(1,w_id);
            selectPs.setInt(1,w_id);


            BigDecimal correct;
            try(ResultSet rs = selectPs.executeQuery()){
                Assert.assertTrue("No rows returned!",rs.next());
                correct = rs.getBigDecimal(2);
                Assert.assertFalse("Returned a null value!",rs.wasNull());

                long k = rs.getLong(1);
                Assert.assertFalse("Returned a null value!",rs.wasNull());
                Assert.assertEquals("Incorrect first value!",w_id,k);
                Assert.assertFalse("Too many rows returned!",rs.next());
            }

            for(int i=0;i<numRuns;i++){
                updateTimer.startTiming();
                int updatedRowCount=updatePs.executeUpdate();
                Assert.assertEquals("Incorrect updated row count!",1,updatedRowCount);
                updateTimer.tick(1);

                selectTimer.startTiming();
                try(ResultSet rs = selectPs.executeQuery()){
                    Assert.assertTrue("No rows returned!",rs.next());
                    BigDecimal n = rs.getBigDecimal(2);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertEquals("Incorrect updated value!",correct.add(BigDecimal.ONE),n);
                    correct = n;

                    long k = rs.getLong(1);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertEquals("Incorrect first value!",w_id,k);

                    Assert.assertFalse("Too many rows returned!",rs.next());
                }
                selectTimer.tick(1);
            }
        }finally{
            try(Statement s = conn.createStatement()){
                s.execute("drop index YTD_IDX");
            }
        }

        /*
         * We have verified correctness through all of our updates, now let's verify performance
         */
        String errorFormat = "%s latency did not stay bounded by %d ns. Was instead %d ns";
        LatencyView updateLatency=updateTimer.getDistribution().wallLatency();
        long p99UpdateLatency=updateLatency.getP99Latency();
        Assert.assertTrue(String.format(errorFormat,"Update",maxUpdateRuntime,p99UpdateLatency),p99UpdateLatency<maxUpdateRuntime);
        LatencyView selectLatency=selectTimer.getDistribution().wallLatency();
        long p99SelectLatency = selectLatency.getP99Latency();
        Assert.assertTrue(String.format(errorFormat,"Update",maxSelectRuntime,p99SelectLatency),p99SelectLatency<maxSelectRuntime);
    }
    @Test
    public void testRepeatedUpdateAndSelectIsCorrect() throws Exception{
        /*
         * Test that we can repeatedly update a record by 1 and then select it and the following is true:
         *
         * 1. We always get back the correct answer
         * 2. We return within a bounded amount of time (say, 100 ms) for 99% of runs
         *
         */
        long maxUpdateRuntime=TimeUnit.MILLISECONDS.toNanos(100);
        long maxSelectRuntime=TimeUnit.MILLISECONDS.toNanos(50);
        int numRuns = 10000;

        LatencyTimer updateTimer =Metrics.sampledLatencyTimer(Math.min(numRuns+1,10000));
        LatencyTimer selectTimer =Metrics.sampledLatencyTimer(Math.min(numRuns+1,10000));
        TestConnection conn = spliceClassWatcher.getOrCreateConnection();

        conn.setAutoCommit(true);
        String updateSql = "update warehouse set w_ytd = w_ytd+1 where w_id = ?";
        String selectSql = "select * from warehouse where w_id = ?";
        try(PreparedStatement updatePs = conn.prepareStatement(updateSql);
                PreparedStatement selectPs = conn.prepareStatement(selectSql)){
            int w_id=300;
            updatePs.setInt(1,w_id);
            selectPs.setInt(1,w_id);


            BigDecimal correct;
            try(ResultSet rs = selectPs.executeQuery()){
                Assert.assertTrue("No rows returned!",rs.next());
                correct = rs.getBigDecimal(2);
                Assert.assertFalse("Returned a null value!",rs.wasNull());

                long k = rs.getLong(1);
                Assert.assertFalse("Returned a null value!",rs.wasNull());
                Assert.assertEquals("Incorrect first value!",w_id,k);
                Assert.assertFalse("Too many rows returned!",rs.next());
            }

            for(int i=0;i<numRuns;i++){
                updateTimer.startTiming();
                int updatedRowCount=updatePs.executeUpdate();
                Assert.assertEquals("Incorrect updated row count!",1,updatedRowCount);
                updateTimer.tick(1);

                selectTimer.startTiming();
                try(ResultSet rs = selectPs.executeQuery()){
                    Assert.assertTrue("No rows returned!",rs.next());
                    BigDecimal n = rs.getBigDecimal(2);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertEquals("Incorrect updated value!",correct.add(BigDecimal.ONE),n);
                    correct = n;

                    long k = rs.getLong(1);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertEquals("Incorrect first value!",w_id,k);

                    Assert.assertFalse("Too many rows returned!",rs.next());
                }
                selectTimer.tick(1);
            }
        }

        /*
         * We have verified correctness through all of our updates, now let's verify performance
         */
        String errorFormat = "%s latency did not stay bounded by %d ns. Was instead %d ns";
        LatencyView updateLatency=updateTimer.getDistribution().wallLatency();
        long p99UpdateLatency=updateLatency.getP99Latency();
        Assert.assertTrue(String.format(errorFormat,"Update",maxUpdateRuntime,p99UpdateLatency),p99UpdateLatency<maxUpdateRuntime);
        LatencyView selectLatency=selectTimer.getDistribution().wallLatency();
        long p99SelectLatency = selectLatency.getP99Latency();
        Assert.assertTrue(String.format(errorFormat,"Update",maxSelectRuntime,p99SelectLatency),p99SelectLatency<maxSelectRuntime);
    }

    @Test
    @Ignore
    public void repeatedUpdateSelectCompact() throws Exception{
        /*
         * Test that we can repeatedly:
         *
         * 1. update a record by 1
         * 2. then select that record
         * 3. compact the table
         *
         * 1. We always get back the correct answer
         * 2. We return within a bounded amount of time (say, 100 ms) for 99% of runs
         *
         */
        long maxUpdateRuntime=TimeUnit.MILLISECONDS.toNanos(100);
        long maxSelectRuntime=TimeUnit.MILLISECONDS.toNanos(50);
        int numRuns = 10000;

        LatencyTimer updateTimer =Metrics.sampledLatencyTimer(Math.min(numRuns+1,10000));
        LatencyTimer selectTimer =Metrics.sampledLatencyTimer(Math.min(numRuns+1,10000));
        TestConnection conn = spliceClassWatcher.getOrCreateConnection();

        conn.setAutoCommit(true);
        String updateSql = "update warehouse set w_ytd = w_ytd+1 where w_id = ?";
        String selectSql = "select * from warehouse where w_id = ?";
        String compactSql = "call SYSCS_UTIL.MAJOR_COMPACT_TABLE(?,?)";
        try(PreparedStatement updatePs = conn.prepareStatement(updateSql);
            PreparedStatement selectPs = conn.prepareStatement(selectSql);
            CallableStatement compact = conn.prepareCall(compactSql) ){
            int w_id=293;
            updatePs.setInt(1,w_id);
            selectPs.setInt(1,w_id);
            compact.setString(1,spliceSchemaWatcher.schemaName);
            compact.setString(2,"WAREHOUSE");


            BigDecimal correct;
            try(ResultSet rs = selectPs.executeQuery()){
                Assert.assertTrue("No rows returned!",rs.next());
                correct = rs.getBigDecimal(2);
                Assert.assertFalse("Returned a null value!",rs.wasNull());

                long k = rs.getLong(1);
                Assert.assertFalse("Returned a null value!",rs.wasNull());
                Assert.assertEquals("Incorrect first value!",w_id,k);
                Assert.assertFalse("Too many rows returned!",rs.next());
            }

            for(int i=0;i<numRuns;i++){
                updateTimer.startTiming();
                int updatedRowCount=updatePs.executeUpdate();
                Assert.assertEquals("Incorrect updated row count!",1,updatedRowCount);
                updateTimer.tick(1);

                selectTimer.startTiming();
                try(ResultSet rs = selectPs.executeQuery()){
                    Assert.assertTrue("No rows returned!",rs.next());
                    BigDecimal n = rs.getBigDecimal(2);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertEquals("Incorrect updated value!",correct.add(BigDecimal.ONE),n);
                    correct = n;

                    long k = rs.getLong(1);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertEquals("Incorrect first value!",w_id,k);

                    Assert.assertFalse("Too many rows returned!",rs.next());
                }
                selectTimer.tick(1);

                if(i%(numRuns/10)==0){
                    compact.execute();
                }
            }
        }

        /*
         * We have verified correctness through all of our updates, now let's verify performance
         */
        String errorFormat = "%s latency did not stay bounded by %d ns. Was instead %d ns";
        LatencyView updateLatency=updateTimer.getDistribution().wallLatency();
        long p99UpdateLatency=updateLatency.getP99Latency();
        Assert.assertTrue(String.format(errorFormat,"Update",maxUpdateRuntime,p99UpdateLatency),p99UpdateLatency<maxUpdateRuntime);
        LatencyView selectLatency=selectTimer.getDistribution().wallLatency();
        long p99SelectLatency = selectLatency.getP99Latency();
        Assert.assertTrue(String.format(errorFormat,"Update",maxSelectRuntime,p99SelectLatency),p99SelectLatency<maxSelectRuntime);
    }

    @Test
    public void repeatedInsertSelectDeleteSelect() throws Exception{
        /*
         * Test that we can repeatedly do the following sequence:
         *
         * 1. insert a row
         * 2. select that row
         * 3. delete the row
         * 4. select the row (it should be missing)
         *
         * And assert the following invariants at every stage
         *
         * 1. We always get back the correct answer
         * 2. We return within a bounded amount of time (say, 100 ms) for 99% of runs
         *
         */
        long maxInsertRuntime=TimeUnit.MILLISECONDS.toNanos(100);
        long maxDeleteRuntime=TimeUnit.MILLISECONDS.toNanos(100);
        long maxSelectRuntime=TimeUnit.MILLISECONDS.toNanos(50);
        int numRuns = 4000;

        LatencyTimer insertTimer =Metrics.sampledLatencyTimer(Math.min(numRuns+1,10000));
        LatencyTimer deleteTimer =Metrics.sampledLatencyTimer(Math.min(numRuns+1,10000));
        LatencyTimer selectTimer =Metrics.sampledLatencyTimer(Math.min(numRuns+1,10000));
        TestConnection conn = spliceClassWatcher.getOrCreateConnection();

        conn.setAutoCommit(true);
        String insertSql = "insert into warehouse (w_id, w_ytd) values (?,?)";
        String deleteSql = "delete from warehouse where w_id = ?";
        String selectSql = "select * from warehouse where w_id = ?";
        try(PreparedStatement insertPs = conn.prepareStatement(insertSql);
            PreparedStatement selectPs = conn.prepareStatement(selectSql);
            PreparedStatement deletePs = conn.prepareStatement(deleteSql)){
            int w_id=294;
            insertPs.setInt(1,w_id);
            selectPs.setInt(1,w_id);
            deletePs.setInt(1,w_id);



            BigDecimal correct;
            for(int i=0;i<numRuns;i++){
                //insert the record
                correct = BigDecimal.valueOf(i);
                insertPs.setBigDecimal(2,correct);
                insertTimer.startTiming();
                int updatedRowCount=insertPs.executeUpdate();
                Assert.assertEquals("Incorrect updated row count!",1,updatedRowCount);
                insertTimer.tick(1);

                //check that it was inserted correctly
                selectTimer.startTiming();
                try(ResultSet rs = selectPs.executeQuery()){
                    Assert.assertTrue("No rows returned!",rs.next());
                    BigDecimal n = rs.getBigDecimal(2);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertTrue("Incorrect updated value! Expected: <"+correct+">, Actual: <"+n+">",correct.compareTo(n)==0);

                    long k = rs.getLong(1);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertEquals("Incorrect first value!",w_id,k);

                    Assert.assertFalse("Too many rows returned!",rs.next());
                }
                selectTimer.tick(1);

                //delete the record
                deleteTimer.startTiming();
                int deletedRowCount = deletePs.executeUpdate();
                Assert.assertEquals("Incorrect deleted row count!",1,deletedRowCount);
                deleteTimer.tick(1l);

                //make sure it's absent
                try(ResultSet rs = selectPs.executeQuery()){
                    Assert.assertFalse("Row is still there!",rs.next());
                }
            }
        }

        /*
         * We have verified correctness through all of our updates, now let's verify performance
         */
        String errorFormat = "%s latency did not stay bounded by %d ns. Was instead %d ns";
        LatencyView insertLatency=insertTimer.getDistribution().wallLatency();
        long p99InsertLatency=insertLatency.getP99Latency();
        Assert.assertTrue(String.format(errorFormat,"insert",maxDeleteRuntime,p99InsertLatency),p99InsertLatency<maxInsertRuntime);
        LatencyView selectLatency=selectTimer.getDistribution().wallLatency();
        long p99SelectLatency = selectLatency.getP99Latency();
        Assert.assertTrue(String.format(errorFormat,"Select",maxSelectRuntime,p99SelectLatency),p99SelectLatency<maxSelectRuntime);
        LatencyView deleteLatency=insertTimer.getDistribution().wallLatency();
        long p99DeleteLatency=deleteLatency.getP99Latency();
        Assert.assertTrue(String.format(errorFormat,"Delete",maxDeleteRuntime,p99DeleteLatency),p99DeleteLatency<maxDeleteRuntime);
    }
}

package com.splicemachine.columnlimit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 8/1/16
 */
public class RandomTableReader{

    private long secondsToSpend; //the amount of time to perform the reads
    private int numReaderThreads;
    private String[] columnNames;
    private String tableNames;
    private DataSource dataSource;
    private long startId;
    private long endId;

    public RandomTableReader timeToSpend(long timeToSpend){
        this.secondsToSpend=timeToSpend;
        return this;
    }

    public RandomTableReader numReaderThreads(int numReaderThreads){
        this.numReaderThreads=numReaderThreads;
        return this;
    }

    public RandomTableReader columnNames(String[] columnNames){
        this.columnNames=columnNames;
        return this;
    }

    public RandomTableReader tableName(String tableNames){
        this.tableNames=tableNames;
        return this;
    }

    public RandomTableReader dataSource(DataSource ds){
        this.dataSource = ds;
        return this;
    }

    public RandomTableReader startId(long startId){
        this.startId=startId;
        return this;
    }

    public RandomTableReader endId(long endId){
        this.endId=endId;
        return this;
    }

    public void runTest() throws Exception{
        ExecutorService es =Executors.newFixedThreadPool(numReaderThreads,new ThreadFactoryBuilder().setDaemon(true).setNameFormat("reader-%d").build());
        CompletionService<Long> completionService = new ExecutorCompletionService<>(es);
        for(int i=0;i<numReaderThreads;i++){
            completionService.submit(new Reader());
        }

        long total = 0L;
        long start = System.nanoTime();
        for(int i=0;i<numReaderThreads;i++){
            Future<Long> take=completionService.take();
            total+=take.get();
        }
        long timeTaken = System.nanoTime()-start;

        printSummaryInfo(total,timeTaken);

    }

    private void printSummaryInfo(long total,long timeTaken){
        System.out.println("--------------------------------------------------------------");
        System.out.println("Read Summary");
        double tSec = timeTaken/1e9d;
        System.out.printf("Total time taken(s):%.4f%n",tSec);
        System.out.printf("Total rows read: %d%n",total);
        System.out.printf("Overall throughput(rows/sec): %.0f%n",total/tSec);
        double tMs = timeTaken/1e6d;
        System.out.printf("Avg Latency(ms/row): %.0f%n",tMs/total);
    }

    private class Reader implements Callable<Long>{

        @Override
        public Long call() throws Exception{
            Random r = new Random();
            long total = 0L;
            long timeRemaining = TimeUnit.SECONDS.toNanos(secondsToSpend);
            try(Connection conn=dataSource.getConnection()){
                do{
                    long s = System.nanoTime();
                    try(PreparedStatement ps=fetchRandomStatement(conn,r)){
                        ps.setLong(1,randomLongInRange(r));

                        try(ResultSet rs=ps.executeQuery()){
                            while(rs.next()){
                                total++;
                            }
                        }
                    }
                    timeRemaining-=(System.nanoTime()-s);
                }while(timeRemaining>0);
            }
            return total;
        }

        private PreparedStatement fetchRandomStatement(Connection conn,Random r) throws SQLException{
            int colCell = r.nextInt(columnNames.length);
            return  conn.prepareStatement("select cust_id,"+columnNames[colCell]+" from "+ tableNames+" where cust_id=?");
        }

        private long randomLongInRange(Random r){
            double d = r.nextDouble(); //in the range [0,1)
            d*=(endId-startId); // in the range [0,end-start)
            d+=startId; //in the range [start,end-start+start) = [start,end)
            return Math.round(d);
        }
    }
}

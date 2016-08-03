package com.splicemachine.columnlimit;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.MultiTimeView;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.stats.random.RandomGenerator;
import com.splicemachine.stats.random.UniformGenerator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 7/28/16
 */
class RandomTableBuilder{

    private static final double NANOS_TO_SEC=1000000000d;
    private static final double NANOS_TO_MS=1000000d;
    private DataSource dataSource;
    private String tableName;
    private int numColumns;

    private int numRows;
    private int numLoaderThreads;
    /*
     * The ratio of non-null columns to total columns to populate (on average). We may use
     * different distributions to mimic different load profiles, but usually we'll use some form
     * of guassian
     */
    private Supplier<RandomGenerator> generatorFactory;

    public RandomTableBuilder dataSource(DataSource dataSource){
        this.dataSource=dataSource;
        return this;
    }

    public RandomTableBuilder tableName(String tableName){
        this.tableName=tableName;
        return this;
    }

    public RandomTableBuilder numColumns(int numColumns){
        this.numColumns=numColumns;
        return this;
    }

    public RandomTableBuilder numRows(int numRows){
        this.numRows=numRows;
        return this;
    }

    public RandomTableBuilder numLoaderThreads(int numLoaderThreads){
        this.numLoaderThreads=numLoaderThreads;
        return this;
    }

    public RandomTableBuilder generatorFactory(Supplier<RandomGenerator> generatorFactory){
        this.generatorFactory=generatorFactory;
        return this;
    }

    /**
     * Build a table up with random columns made up of numeric columns.
     *
     */
    String[] buildTable() throws Exception{
        String[] columns;
        try(Connection conn = dataSource.getConnection()){
            columns = createTable(conn,tableName,numColumns);
        }

        ExecutorService loaderThreads =Executors.newFixedThreadPool(numLoaderThreads,
                new ThreadFactoryBuilder().setNameFormat("loader-%d").setDaemon(true).build());
        CompletionService<TimeView> loader = new ExecutorCompletionService<TimeView>(loaderThreads);
        long sId = 0;
        long eId;
        for(int i=0;i<numLoaderThreads;i++){
            eId = sId+(numRows/numLoaderThreads);
            loader.submit(new Loader(columns,sId,eId));
            sId = eId;
        }

        MultiTimeView summaryView = Metrics.multiTimeView();
        long start = System.nanoTime();
        for(int i=0;i<numLoaderThreads;i++){
            Future<TimeView> take=loader.take();
            TimeView stats = take.get();
            printDebugTimeStats(stats);
            summaryView.update(stats);
        }
        long end = System.nanoTime();

        printSummaryStats(end-start,summaryView);
        return columns;
    }

    private void printSummaryStats(long totalTimeNanos,MultiTimeView summaryView){
        System.out.println("-----------------------------------------");
        System.out.println("Summary statistics:");
        System.out.printf("Overall time(s): %.4f %n",totalTimeNanos/NANOS_TO_SEC);
        printTimeView(totalTimeNanos,numRows,summaryView);
    }

    private void printTimeView(long totalTimeNanos,long numRows,TimeView summaryView ){
        double timeSec=totalTimeNanos/NANOS_TO_SEC;
        double timeMs = totalTimeNanos/NANOS_TO_MS;
        System.out.printf("Total Time(s): %.4f %n",timeSec);
        System.out.printf("Num Rows: %d %n",numRows);
        System.out.printf("Avg throughput (rows/s): %.0f %n",numRows/timeSec);
        System.out.printf("Avg latency (ms/row): %.4f %n",timeMs/numRows);
    }

    private void printDebugTimeStats(TimeView stats){
        printTimeView(stats.getWallClockTime(),numRows/numLoaderThreads,stats);
        System.out.println("###");
    }

    private String[] createTable(Connection conn,String tableName,int numColumns) throws SQLException{
        StringBuilder sb = new StringBuilder("create table ").append(tableName).append("(cust_id bigint primary key");
        String[] colNames = new String[numColumns];
        for(int i=0;i<numColumns;i++){
            sb = sb.append(",");

            String colName = "col"+i;
            colNames[i] = colName;
            sb = sb.append(colName).append(" bigint");
        }
        sb = sb.append(")");
        try(Statement s = conn.createStatement()){
            s.execute("drop table if exists "+ tableName);
            s.execute(sb.toString());
        }
        return colNames;
    }

    private class Loader implements java.util.concurrent.Callable<TimeView>{
        private final String[] colNames;
        private final long startId;
        private final long endId;

        Loader(String[] colNames,long startId,long endId){
            this.colNames=colNames;
            this.startId=startId;
            this.endId=endId;
        }

        @Override
        public TimeView call() throws Exception{
            RandomGenerator gen = generatorFactory.get();
            RandomGenerator uniformRandom = new UniformGenerator(new Random(gen.nextInt()));

            Timer timer =Metrics.newTimer();
            try(Connection conn = dataSource.getConnection()){
                for(long i=startId;i<endId;i++){
                    int numColsTofill=genColumnSize(gen);
                    String query = buildQueryString(numColsTofill,i,uniformRandom);

                    try(Statement s = conn.createStatement()){
                        timer.startTiming();
                        s.executeUpdate(query);
                        timer.tick(1);
                    }
//                    try(PreparedStatement ps = conn.prepareStatement(query)){
//                        ps.setLong(1,i); //set the customer id
//                        for(int c = 2;c<=numColsTofill;c++){
//                            ps.setLong(c,uniformRandom.nextLong());
//                        }
//                        timer.startTiming();
//                        try{
//                            ps.executeUpdate();
//                        }finally{
//                            timer.tick(1);
//                        }
//                    }
                }
            }
            return timer.getTime();
        }

        private String buildQueryString(int numColsTofill,long cId,RandomGenerator valueGenerator){
            StringBuilder sb = new StringBuilder("insert into ").append(tableName).append("(cust_id");
            StringBuilder valuesQ = new StringBuilder(" values (").append(cId);
            BitSet chosenColumns = new BitSet(colNames.length);
            for(int i=1;i<numColsTofill;i++){ //one column is the customer id
                sb = sb.append(",");
                valuesQ = valuesQ.append(",");

                int chosenColumn;
                do{
                    chosenColumn=selectColumn(valueGenerator);
                }while(chosenColumns.get(chosenColumn));
                chosenColumns.set(chosenColumn);

                String colName = colNames[chosenColumn];
                sb = sb.append(colName);
                valuesQ = valuesQ.append(valueGenerator.nextLong());
            }
            sb = sb.append(")").append(valuesQ).append(")");

            return sb.toString();
        }

        private int selectColumn(RandomGenerator valueGenerator){
            double n = valueGenerator.nextDouble(); //in [0,1)
            n = n*numColumns; //in [0,numColumns)
            return (int)n;
        }

        private int genColumnSize(RandomGenerator gen){
            double c;
            do{
                c = gen.nextDouble();
            }while(c<0||c>1);
            return (int)(c*numColumns);
        }

    }
}

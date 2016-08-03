package com.splicemachine.columnlimit;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.splicemachine.db.jdbc.ClientDriver;
import com.splicemachine.stats.random.Generators;
import com.splicemachine.stats.random.RandomGenerator;
import com.splicemachine.stats.random.UniformGenerator;
import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 7/28/16
 */
public class ColumnLimitPerfChecker{

    public static void main(String...args) throws Exception{
        int p = 0;
        int numColumns = -1;
        int numRows = -1;
        long selectTime = -1L;
        float fillFactor = 0.1f;
        int numThreads = 1;
        String tName = "colTable";
        String host = "localhost:1527";
        while(p<args.length){
            switch(args[p]){
                case "-c":
                    p=checkAdvance(p,args);
                    numColumns=parseInteger(p,args,"Number of Columns");
                    p++;
                    break;
                case "-r":
                    p=checkAdvance(p,args);
                    numRows=parseInteger(p,args,"Number of Rows");
                    p++;
                    break;
                case "-tableName":
                    p=checkAdvance(p,args);
                    tName = args[p];
                    p++;
                    break;
                case "-t":
                    p = checkAdvance(p,args);
                    numThreads = parseInteger(p,args,"Number of Threads");
                    p++;
                    break;
                case "-h":
                    p = checkAdvance(p,args);
                    host = args[p];
                    p++;
                    break;
                case "-f":
                    p = checkAdvance(p,args);
                    fillFactor = parseFloat(p,args,"Fill Factor");
                    p++;
                    break;
                case "-s":
                    p = checkAdvance(p,args);
                    selectTime = parseInteger(p,args,"Select Test Time");
                    p++;
                    break;
                default:
                    System.err.println("Unexpected argument: "+ args[p]);
                    System.exit(badArgs(args));
            }
        }

        if(numColumns<0){
            System.err.println("No column limit specified!");
            System.exit(65);
        }

        if(numRows <0){
            System.err.println("No row limit specified!");
            System.exit(65);
        }


        final float avg = 0;
        final float stdDev = fillFactor;
        Supplier<RandomGenerator> genSupplier = new Supplier<RandomGenerator>(){
            @Override
            public RandomGenerator get(){
                return Generators.gaussian(new UniformGenerator(new Random()),avg,stdDev);
            }
        };
        DataSource dataSource = buildDataSource(host);
        RandomTableBuilder dataLoader = new RandomTableBuilder()
                .dataSource(dataSource)
                .numLoaderThreads(numThreads)
                .generatorFactory(genSupplier)
                .numColumns(numColumns)
                .tableName(tName)
                .numRows(numRows);

        String[] columns=dataLoader.buildTable();

        if(selectTime>0){
            RandomTableReader reader=new RandomTableReader()
                    .dataSource(dataSource)
                    .columnNames(columns)
                    .tableName(tName)
                    .startId(0L)
                    .endId(numRows)
                    .numReaderThreads(numThreads)
                    .timeToSpend(selectTime);

            reader.runTest();
        }
    }


    private static DataSource buildDataSource(String host){
        try{
            Class.forName(ClientDriver.class.getCanonicalName()); //make sure we load the driver
        }catch(ClassNotFoundException e){
            System.err.println("Unable to load Splice JDBC driver, please check classpath(error message:"+e.getMessage()+")");
            System.exit(5);
        }
        BasicDataSource ds=new BasicDataSource();
        ds.setDriverClassName(ClientDriver.class.getCanonicalName());
        ds.setUrl("jdbc:splice://"+host+"/splicedb");
        ds.setUsername("splice");
        ds.setPassword("admin");

        return ds;
    }

    private static float parseFloat(int p,String[] args,String argType){
        try{
            return Float.parseFloat(args[p]);
        }catch(NumberFormatException nfe){
            System.err.println("Unable to parse "+argType);
            System.exit(badArgs(args));
            return -1; //never reached
        }
    }

    private static int parseInteger(int p,String[] args,String argType){
        try{
            return Integer.parseInt(args[p]);
        }catch(NumberFormatException nfe){
            System.err.println("Unable to parse "+argType);
            System.exit(badArgs(args));
            return -1; //never reached
        }
    }

    private static int checkAdvance(int p,String[] args){
        if(p==args.length-1)
            System.exit(badArgs(args));
        return p+1;
    }

    private static int badArgs(String[] args){
        System.err.println("Unexpected arg line: "+Joiner.on(" ").join(args));
        printUsage();
        return 65;
    }

    private static void printUsage(){
        System.out.println("Usage: "+ColumnLimitPerfChecker.class.getName()+" -c <numColumns> [-t <tableName>]");
    }
}

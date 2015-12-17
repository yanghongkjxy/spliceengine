package com.splicemachine.hbase.table;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.lucene.util.NamedThreadFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 10/6/14
 */
public class SpliceConnectionPool {
    private static volatile SpliceConnectionPool INSTANCE;
    private final HConnection[] connections;
    private final int numConnections;
    private static AtomicInteger counter = new AtomicInteger(0);

    public SpliceConnectionPool(int numConnections) {
        this(numConnections,SpliceConstants.config);
    }

    public SpliceConnectionPool(int numConnections,Configuration conf) {
        /*
         * To avoid worrying about integer overflow here, we make the number of
         * connections a positive power of 2, and the variable "numConnections" is shifted
         * down by 1 to make an int with all 1s in the first n bits. This effectively traps
         * the results of counter.get() in the range [0,n) (as opposed to a modulus operator which
         * ends up in the range (-n,n))
         */
        int n = 1;
        while(n<numConnections)
            n<<=1;
        this.numConnections = n-1;
        this.connections = new HConnection[n];

        initialize(conf);
    }

    public static SpliceConnectionPool getDefaultPool(){
        SpliceConnectionPool pool = INSTANCE;
        if(pool==null){
            synchronized(SpliceConnectionPool.class){
                pool = INSTANCE;
                if(pool==null){
                    pool = INSTANCE = new SpliceConnectionPool(SpliceConstants.numHConnections);
                }
            }
        }
        return pool;
    }

    private void initialize(Configuration config) {
        ExecutorService connectionPool = createConnectionPool(config);

        if(numConnections==0){
            try{
                connections[0] = HConnectionManager.createConnection(config,connectionPool);
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }else{
            for(int i=0;i<=numConnections;i++){
                Configuration configuration=new Configuration(config);
                configuration.setInt(HConstants.HBASE_CLIENT_INSTANCE_ID,i);
                try{
                    connections[i]=HConnectionManager.createConnection(config,connectionPool);
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public HConnection getConnection(){
        return getConnectionDirect();
    }

    HConnection getConnectionDirect(){
        /*
         * We used to do a modulus operation here, but modulus is bad because eventually
         * the counter is going to overflow: when it does, the modulus will start spitting out negative
         * numbers, which will cause this to explode. Using a power of 2 for the connection size and
         * the & operator here will prevent that, as long as numConnections is a positive power of 2
         */
        return connections[counter.getAndIncrement() & numConnections];
    }

    private static ExecutorService createConnectionPool(Configuration conf) {
        int coreThreads = conf.getInt("hbase.hconnection.threads.core", 10);
        if (coreThreads == 0) {
            coreThreads = 10;
        }
        int maxThreads = conf.getInt("hbase.hconnection.threads.max", 32);
        if (maxThreads == 0) {
            maxThreads = Runtime.getRuntime().availableProcessors() * 8;
        }
        long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
        LinkedBlockingQueue<Runnable> workQueue =
                new LinkedBlockingQueue<>(maxThreads*
                        conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                                HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
        return new ThreadPoolExecutor(coreThreads,  maxThreads, keepAliveTime, TimeUnit.SECONDS,
                workQueue,
                new NamedThreadFactory("connection-pool-"),
                new ThreadPoolExecutor.AbortPolicy());

    }

}
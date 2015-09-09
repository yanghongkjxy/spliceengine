package com.splicemachine.si.impl;

import com.splicemachine.annotations.ThreadSafe;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class which maintains the behavior for keeping track of the minimum active transaction.
 *
 * The <em>Minimum Active Transaction(MAT)</em> is the smallest begin timestamp of all active transactions across the
 * entire cluster.
 *
 * This class uses several heuristics for maintaining the MAT in the event of server failure.
 *
 * First note that the MAT is global across all servers. Therefore, collecting that
 * value requires us to connect with all servers in the cluster. That can be problematic when a subset of servers
 * is behaving poorly or not responding well.
 *
 * Second, it is important to note that the MAT is a value which is absolutely correct
 * only at a single point in time--in truth, it is rarely 100% accurate. However, because transaction ids are
 * monotonically increasing by definition, we do not need to be perfectly accurate at all times.
 *
 * This allows us to be more tolerant to server failure. On first initiation, we attempt to collect the MAT from all
 * servers. If this does not succeed, then we set the value to be -1 (indicating an unknown MAT). Otherwise, we
 * set the MAT to be that which we found. Once an MAT is found, we periodically refresh the value to keep it in sync.
 * If at any point in this background refreshing we are unable to contact a subset of servers, we bail on that attempt
 * and leave the existing MAT in place. This will mean the MAT we serve up is stale, but it will still satisfy
 * nearly every use case, since that timestamp will be <= all active transaction ids.
 *
 * @author Scott Fines
 *         Date: 9/14/15
 */
@ThreadSafe
public class MinimumTransactionWatcher{
    private static final Logger LOG=Logger.getLogger("transactionWatcher");

    public interface MATReader{
        /**
         * @return the begin timestamp of the minimum active transaction, or -1 if the transaction was unable
         * to be returned. If 0 is returned, it is an indication that there are no active transactions on the cluster
         * (which is weird from a SQL perspective, but perfectly acceptable in general on a quiescent cluster).
         *
         * @throws IOException if something is wrong with connections, and some servers are unavailable
         * @throws InterruptedException if the current thread is interrupted during execution
         */
        long minimumActiveBeginTimestamp() throws IOException, InterruptedException;

        /**
         * @return the begin timestamp of the minimum active transaction, or the begin timestamp of the last known
         * active transaction.
         *
         * @throws IOException if something is wrong with connections, and some servers are unavailable
         * @throws InterruptedException if the current thread is interrupted during execution
         */
        long lastActiveBeginTimestamp() throws IOException, InterruptedException;
    }

    private final AtomicLong timestamp = new AtomicLong(-1l);

    private final MATReader reader;
    private final ScheduledExecutorService scheduler;
    private final long readIntervalMs;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MinimumTransactionWatcher(MATReader reader,long readIntervalMs){
        this(reader,Executors.newSingleThreadScheduledExecutor(new ThreadFactory(){
            @Override
            public Thread newThread(Runnable r){
                Thread t = new Thread(r);
                t.setName("minimTransactionWatcher");
                t.setDaemon(true);
                //this is a low-priority thread, so set it as such
                t.setPriority(1);
                return t;
            }
        }),readIntervalMs);
    }

    public MinimumTransactionWatcher(MATReader reader,ScheduledExecutorService ses,long readIntervalMs){
        this.reader=reader;
        this.readIntervalMs=readIntervalMs;
        this.scheduler =ses;
    }

    public void start(){
        if(!running.compareAndSet(false,true)) return;
        readAndSet(false);

        LOG.info("Scheduling minimum active transaction reader");
        scheduler.scheduleAtFixedRate(new Runnable(){
            @Override
            public void run(){
                readAndSet(true);
            }
        },readIntervalMs,readIntervalMs,TimeUnit.MILLISECONDS);
    }


    public void shutdown(){
        if(!running.compareAndSet(true,false)) return;
        scheduler.shutdownNow();
    }

    /**
     * Equivalent to {@link #minimumActiveTimestamp(boolean)}, with {@code forceRefresh=false}. In other words, this
     * will fetch directly from the cache and bypass any IO. This is very fast, but may result in results which are
     * much more stale than other results.
     *
     * @return a timestamp {@code T} such that {@code T <= B}, where {@code B} is the begin timestamp of any currently
     * active transaction;
     */
    public long minimumActiveTimestamp(){
       return minimumActiveTimestamp(false);
    }

    /**
     * @param forceRefresh if {@code true}, then we will first attempt to fetch the timestamp from all servers (to
     *                     ensure the freshest result), but will use the cached value as a fallback if some servers
     *                     are unavailable.
     * @return a timestamp {@code T} such that {@code T <= B}, where {@code B} is the begin timestamp of any currently
     * active transaction;
     */
    public long minimumActiveTimestamp(boolean forceRefresh){
        if(forceRefresh)
            return readAndSet(false);
        else{
            long t = timestamp.get();
            if(t<=0)
                t = readAndSet(true);
            return t;
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods/classes*/
    private long readAndSet(boolean allowLast){
        //perform a fetch to initialize
        long newMat = -1l;
        try{
            LOG.trace("Fetching minimum active begin timestamp");
            if(allowLast)
                newMat = reader.lastActiveBeginTimestamp();
            else
                newMat = reader.minimumActiveBeginTimestamp();
        }catch(IOException e){
            LOG.warn("Error encountered fetching timestamp:",e);
        }catch(InterruptedException e){
            LOG.info("Interrupted during fetch, cancelling");
        }
        /*
         * If we were unable to obtain the current MAT, then use the old one instead.
         *
         * There are two possible "error" codes: -1 and 0. 0 means that there are no active timestamps
         * found, which in turn means that you should pull your transaction, because that's the minimum active
         * one. However, if you get -1, it means that you weren't able to connect to every server, so you should
         * return the OLD number (it may still be active)
         */
        if(newMat<0) return timestamp.get();
        else if(newMat==0){
            return newMat;
        }
        boolean shouldContinue;
        do{
            long curr=timestamp.get();
                /*
                 * If the value currently stashed is greater than us, then we were in a race and someone
                 * else has given us more up-to-date information (because of the monotonic nature of timestamps).
                 * In this case, just return their value and don't worry about what we fetched
                 */
            if(newMat<curr) return curr;
            shouldContinue = !timestamp.compareAndSet(curr,newMat);
        }while(shouldContinue);
        return newMat;
    }
}

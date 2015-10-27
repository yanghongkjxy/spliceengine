package com.splicemachine.si.impl.checkpoint;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.splicemachine.si.api.Partition;
import com.splicemachine.utils.ByteSlice;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 11/3/15
 */
public class AsyncCheckpointResolver{
    private static final Logger LOG = Logger.getLogger(AsyncCheckpointResolver.class);
    private final RingBuffer<CheckpointEvent> ringBuffer;
    private final WorkerPool<CheckpointEvent> disruptor;

    private final ThreadPoolExecutor consumerThreads;
    private volatile boolean stopped;
    private final SharedCheckpointResolver resolver;

    public AsyncCheckpointResolver(int maxThreads, int bufferSize,
                                   SharedCheckpointResolver resolver){
        this.resolver = resolver;
        consumerThreads=new ThreadPoolExecutor(maxThreads,maxThreads,
                60,TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("checkpointResolver-%d").setDaemon(true).build());

        int bSize = 1;
        while(bSize<bufferSize)
            bSize<<=1;
        this.disruptor = new WorkerPool<>(new CheckpointFactory(),new LoggingExceptionHandler(),workers(maxThreads));
        this.ringBuffer = disruptor.start(consumerThreads);
    }

    public CheckpointResolver checkpointResolver(Partition partition){
        return new PartResolver(partition);
    }

    public void shutdown(){
        stopped=true;
        disruptor.halt();
        consumerThreads.shutdownNow();
    }
    /* ********************************************************************************************************/
    /*private helper methods and classes*/
    private WorkHandler<CheckpointEvent>[] workers(int maxThreads){
        WorkHandler<CheckpointEvent>[] workers = new WorkHandler[maxThreads];
        for(int i=0;i<maxThreads;i++){
            workers[i] = new CheckpointHandler();
        }
        return workers;
    }

    private class CheckpointHandler implements WorkHandler<CheckpointEvent>{

        @Override
        public void onEvent(CheckpointEvent event) throws Exception{
            resolver.checkpoint(event.partition,event.checkpoint);
        }
    }

    private class CheckpointEvent{
        private Partition partition;
        private Checkpoint checkpoint;
    }

    private class CheckpointFactory implements EventFactory<CheckpointEvent>{
        @Override
        public CheckpointEvent newInstance(){
            return new CheckpointEvent();
        }
    }

    private class LoggingExceptionHandler implements ExceptionHandler{
        @Override
        public void handleEventException(Throwable ex,long sequence,Object event){
           LOG.info("Unexpected exception processing Checkpoint",ex);
        }

        @Override
        public void handleOnStartException(Throwable ex){
            LOG.info("Unexpected startup exception",ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex){
            LOG.info("Unexpected shutdown exception",ex);
        }
    }

    private class PartResolver implements CheckpointResolver{
        private final Partition partition;

        public PartResolver(Partition partition){
            this.partition=partition;
        }

        @Override
        public void resolveCheckpoint(Checkpoint... checkpoint) throws IOException{
            if(stopped) return;
            long sequence;
            try{
                sequence = ringBuffer.tryNext(checkpoint.length);
            }catch(InsufficientCapacityException e){
                if(LOG.isTraceEnabled())
                    LOG.trace("Unable to submit for checkpointing",e);
                return;
            }

            long lo = sequence-checkpoint.length+1;
            long l = lo;
            try{
                int i=0;
                while(l<=sequence){
                    CheckpointEvent ce=ringBuffer.get(l);
                    ce.partition=partition;
                    Checkpoint cp;
                    if(ce.checkpoint==null){
                        cp=ce.checkpoint=new Checkpoint();
                    }else cp = ce.checkpoint;
                    cp.set(checkpoint[i]);
                    l++;
                    i++;
                }
            }finally{
                ringBuffer.publish(lo,l);
            }
        }

        @Override
        public void resolveCheckpoint(ByteSlice rowKey,long checkpointTimestamp) throws IOException{
            resolveCheckpoint(rowKey.array(),rowKey.offset(),rowKey.length(),checkpointTimestamp);
        }

        @Override
        public void resolveCheckpoint(byte[] rowKeyArray,int offset,int length,long checkpointTimestamp) throws IOException{
            if(stopped) return;
            long sequence;
            try{
                sequence = ringBuffer.tryNext();
            }catch(InsufficientCapacityException e){
                if(LOG.isTraceEnabled())
                    LOG.trace("Unable to submit for checkpointing",e);
                return;
            }

            try{
                CheckpointEvent ce=ringBuffer.get(sequence);
                ce.partition=partition;
                Checkpoint cp;
                if(ce.checkpoint==null){
                    cp=ce.checkpoint=new Checkpoint();
                }else cp = ce.checkpoint;
                cp.set(rowKeyArray,offset,length,checkpointTimestamp);
            }finally{
                ringBuffer.publish(sequence);
            }
        }
    }
}

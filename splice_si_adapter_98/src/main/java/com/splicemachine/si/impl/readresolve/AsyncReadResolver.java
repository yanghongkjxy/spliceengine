package com.splicemachine.si.impl.readresolve;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.RollForward;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.TrafficControl;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Read-Resolver which asynchronously submits regions for execution, discarding
 * any entries which exceed the size of the processing queue.
 *
 * This implementation uses an LMAX disruptor to asynchronously pass Read-resolve events
 * to a background thread, which in turn uses a SynchronousReadResolver to actually perform the resolution.
 *
 * @see com.splicemachine.si.impl.readresolve.SynchronousReadResolver
 * @author Scott Fines
 * Date: 7/1/14
 */
@ThreadSafe
public class AsyncReadResolver{
    private static final Logger LOG=Logger.getLogger(AsyncReadResolver.class);
    private volatile RingBuffer<ResolveEvent> ringBuffer;
    private final WorkerPool<ResolveEvent> disruptor;

		private final ThreadPoolExecutor consumerThreads;
		private volatile boolean stopped;
    private final TxnSupplier txnSupplier;
    private final RollForwardStatus status;
		private final TrafficControl trafficControl;

    public AsyncReadResolver(int maxThreads,int bufferSize,
                             TxnSupplier txnSupplier,
                             RollForwardStatus status,
                             TrafficControl trafficControl){
        this.txnSupplier=txnSupplier;
        this.trafficControl=trafficControl;
        this.status=status;
        consumerThreads=new ThreadPoolExecutor(maxThreads,maxThreads,
                60,TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("readResolver-%d").setDaemon(true).build());

        int bSize=1;
        while(bSize<bufferSize)
            bSize<<=1;

        disruptor =new WorkerPool<>(new ResolveEventFactory(),new LoggingExceptionHandler(),workers(maxThreads));
    }

    public void start(){
        ringBuffer = disruptor.start(consumerThreads);
    }

    public void shutdown(){
        stopped=true;
        disruptor.drainAndHalt();
        consumerThreads.shutdownNow();
    }

    public
    @ThreadSafe
    ReadResolver getResolver(HRegion region,RollForward rollForward){
        return new RegionReadResolver(region,rollForward);
    }

    private WorkHandler<ResolveEvent>[] workers(int maxThreads){
        @SuppressWarnings("unchecked") WorkHandler<ResolveEvent>[] workers = new WorkHandler[maxThreads];
        for(int i=0;i<maxThreads;i++){
            workers[i] = new ResolveEventHandler();
        }
        return workers;
    }

    private static class ResolveEvent{
        HRegion region;
        long txnId;
        ByteSlice rowKey=new ByteSlice();
        RollForward rollForward;
        RegionReadResolver resolver;
        long addSeq; //the sequence that we were added with
    }

    private static class ResolveEventFactory implements EventFactory<ResolveEvent>{
        @Override public ResolveEvent newInstance(){ return new ResolveEvent(); }
    }

    private class ResolveEventHandler implements WorkHandler<ResolveEvent>{

        @Override
        public void onEvent(ResolveEvent event) throws Exception{
            /*
             * Pausing is a bit weird with the Asynchronous structure.
             *
             * Obviously, we cannot add items to the ringBuffer while paused(handled in RegionReadResolver), but
             * we also cannot *process* items during compaction. Otherwise, we run into race conditions with items
             * which are already present in the buffer. Thus, we must avoid processing anything which was sequentially
             * id-ed as "before" the resume sequence id. In effect, this means discarding all items which were present
             * on the ring buffer when pause was called, and any item which was attempted to be added before
             * resume was called.
             *
             */
            if(event==null || event.resolver==null) return; //programmer safety valve to avoid NPE--discard if we aren't able to test
            long resumeSeqId=event.resolver.resumeSequenceId;
            if(resumeSeqId<0) return; //still paused
            else if(event.addSeq<=resumeSeqId){
                return; //we are not paused, but we still need to discard
            }
            try{
                if(SynchronousReadResolver.INSTANCE.resolve(event.region,
												event.rowKey,
												event.txnId,
												txnSupplier,
												status,
												false,
												trafficControl)){
                    event.rollForward.recordResolved(event.rowKey,event.txnId);
                }
            }catch(Exception e){
                LOG.info("Error during read resolution",e);
                throw e;
            }
        }
    }

		private class RegionReadResolver implements ReadResolver {
				private final HRegion region;
        private final RollForward rollForward;
        public volatile long resumeSequenceId = 0;

				public RegionReadResolver(HRegion region,RollForward rollForward) {
						this.region = region;
            this.rollForward = rollForward;
				}

        @Override
        public void resolve(ByteSlice rowKey,long txnId){
            if(resumeSequenceId<0) return; //we are paused, so do not add to queue, do not pass go, do not collect 200 dollars
            if(stopped) return; //we aren't running, so do nothing
            long sequence;
            try {
                sequence = ringBuffer.tryNext();
            } catch (InsufficientCapacityException e) {
                if (LOG.isTraceEnabled())
                    LOG.trace("Unable to submit for read resolution");
                return;
            }

            try{
                ResolveEvent event=ringBuffer.get(sequence);
                event.region=region;
                event.resolver=this;
                event.txnId=txnId;
                event.rowKey.set(rowKey.getByteCopy());
                event.rollForward=rollForward;
                event.addSeq = sequence;
            }finally{
                ringBuffer.publish(sequence);
            }
        }

        @Override public void pauseResolution(){
            resumeSequenceId = -1;
        }

        @Override
        public void resumeResolution(){
            long sequence=ringBuffer.next();

            try{
                resumeSequenceId = sequence;
            }finally{
                ringBuffer.publish(sequence);
            }
        }
    }

    private static class LoggingExceptionHandler implements ExceptionHandler{
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
>>>>>>> a85dc58... Pausing transactional maintenance operators during compaction.
}

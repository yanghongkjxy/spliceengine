package com.splicemachine.derby.tools;

import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 9/24/15
 */
public class IteratorRegionScanner implements MeasuredRegionScanner<Cell>{
    private final Iterable<List<Cell>> data;
    private final Timer timer;
    private final Counter outputBytesCounter;
    private final Counter visitedCounter;
    private final Counter filterCounter;
    private Iterator<List<Cell>> rows;

    public IteratorRegionScanner(Iterable<List<Cell>> data){
       this(data,Metrics.noOpMetricFactory());
    }

    public IteratorRegionScanner(Iterable<List<Cell>> data,MetricFactory metricFactory){
        this.data=data;
        this.timer = metricFactory.newTimer();
        this.outputBytesCounter = metricFactory.newCounter();
        this.visitedCounter = metricFactory.newCounter();
        this.filterCounter = metricFactory.newCounter();
    }

    @Override
    public void start(){
        this.rows = data.iterator();
    }

    @Override public TimeView getReadTime(){ return timer.getTime(); }
    @Override public long getBytesOutput(){ return outputBytesCounter.getTotal(); }
    @Override public long getBytesVisited(){ return visitedCounter.getTotal(); }
    @Override public long getRowsOutput(){ return timer.getNumEvents(); }
    @Override public long getRowsFiltered(){ return filterCounter.getTotal(); }
    @Override public long getRowsVisited(){ return timer.getNumEvents(); }

    @Override
    public Cell next() throws IOException{
       throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean internalNextRaw(List<Cell> results) throws IOException{
        if(rows==null) start();
        if(!rows.hasNext()) return false;
        results.addAll(rows.next());
        return rows.hasNext();
    }

    @Override
    public HRegionInfo getRegionInfo(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean isFilterDone() throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean reseek(byte[] row) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public long getMaxResultSize(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public long getMvccReadPoint(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException{
        return internalNextRaw(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result,int limit) throws IOException{
        return internalNextRaw(result);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException{
        return internalNextRaw(results);
    }

    @Override
    public boolean next(List<Cell> result,int limit) throws IOException{
        return internalNextRaw(result);
    }

    @Override
    public void close() throws IOException{

    }
}

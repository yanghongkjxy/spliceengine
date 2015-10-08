package com.splicemachine.derby.hbase;

import com.google.common.base.Suppliers;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.MergingReader;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 10/12/15
 */
class CheckpointReader{
    interface ReadHandler{
       void onDelete (ByteSlice rowKey);
    }

    private final MergingReader<Cell> mr;
    private final RowAccumulator<Cell> accumulator;

    private transient long highestTimestamp;
    private transient TxnView highTxn;

    public CheckpointReader(final long minimumActiveTimestamp,
                            final ReadHandler deleteHandler,
                            RowAccumulator<Cell> accumulator,
                            SIFilter<Cell> siFilter,
                            SDataLib dataLib,
                            MeasuredRegionScanner mrs,
                            MetricFactory metricFactory){
        this.accumulator =accumulator;
        final TxnSupplier txnSupplier = siFilter.unwrapFilter().getTxnSupplier();
        final ByteSlice deleteRowKey = new ByteSlice();
        this.mr = new MergingReader<Cell>(accumulator,mrs,dataLib,
                MergingReader.RowKeyFilter.NOOPFilter,Suppliers.ofInstance(siFilter),metricFactory){
            @Override
            protected void resetStateForNextRow(SIFilter<Cell> filter){
                highestTimestamp = -1l;
                highTxn = null;
                super.resetStateForNextRow(filter);
            }

            @Override
            protected boolean fetchNextBatch(List<Cell> data) throws IOException{
                boolean b= super.fetchNextBatch(data);
                if(data.size()<=0) return b;
                long currMaxTs = highestTimestamp;
                for(Cell c:data){
                    long timestamp=c.getTimestamp();
                    if(timestamp<=minimumActiveTimestamp){
                        if(currMaxTs<timestamp){
                            highestTimestamp = timestamp;
                            highTxn = txnSupplier.getTransaction(timestamp);
                        }
                        break;
                    }
                }
                return b;
            }

            @Override
            protected boolean rowChanged(SIFilter<Cell> filter,List<Cell> kvs,boolean priorProcessed){
                if(!priorProcessed){
                    Cell cell=kvs.get(0);
                    deleteRowKey.set(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                    deleteHandler.onDelete(deleteRowKey);
                }
                return super.rowChanged(filter,kvs,priorProcessed);
            }
        };
    }

    public TxnView currentMatTxn(){
        return highTxn;
    }

    public ByteSlice currentRowKey(){
        return mr.currentRowKey();
    }

    public byte[] currentCheckpointValue(){
        return accumulator.result();
    }

    public boolean readNext() throws IOException{
        return mr.readNext();
    }

    public long getRowsVisited(){
        return mr.getRowsVisited();
    }
}

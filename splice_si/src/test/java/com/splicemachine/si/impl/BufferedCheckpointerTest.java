package com.splicemachine.si.impl;

import com.splicemachine.concurrent.ConcurrentTicker;
import com.splicemachine.concurrent.TickingClock;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.Checkpointer;
import com.splicemachine.storage.InMemoryPartition;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 10/2/15
 */
@SuppressWarnings("deprecation")
public class BufferedCheckpointerTest{

    @Test
    public void testCheckpointsASingleRow() throws Exception{

        TickingClock clock = new ConcurrentTicker(0l);
        InMemoryPartition partition = new InMemoryPartition(clock);


        clock.tickMillis(1l);
        byte[] originalValue = Encoding.encode("Hello");
        byte[] rowKey = Encoding.encode("rowKey");
        Put p = new Put(rowKey);
        p.add(FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSpliceConstants.PACKED_COLUMN_BYTES,clock.currentTimeMillis(),originalValue);
        partition.mutate(p);

        Checkpointer checkpointer = new BufferedCheckpointer(partition,16);
        byte[] newValue = Encoding.encode("Goodbye");
        checkpointer.checkpoint(ByteSlice.wrap(rowKey),newValue,clock.currentTimeMillis(),-1l);



    }
}
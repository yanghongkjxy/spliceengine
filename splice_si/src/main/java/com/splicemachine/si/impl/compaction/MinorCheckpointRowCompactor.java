package com.splicemachine.si.impl.compaction;

import com.splicemachine.si.api.RollForward;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.data.hbase.NoOpAccumulator;
import com.splicemachine.si.impl.CellTypeParser;
import org.apache.hadoop.hbase.Cell;

import java.io.IOException;

/**
 * A RowCompactor for handling minor compactions. This will discard any versions
 * which fall <em>below</em> the first checkpoint below the MAT, but will not accumulate
 * any user data (so it won't create new checkpoints), and it won't remove any deleted
 * rows (so tombstones below the MAT but above the first checkpoint below will not
 * adjust the discard point). This is to maintain data integrity, since there may
 * be files not being compacted which include intermediate versions.
 *
 * @author Scott Fines
 *         Date: 11/24/15
 */
public class MinorCheckpointRowCompactor extends CheckpointRowCompactor{
    public MinorCheckpointRowCompactor(long mat,
                                       CellTypeParser ctParser,
                                       TxnSupplier transactionStore){
        //we cannot perform accumulations in a minor compaction
        super(mat,ctParser,NoOpAccumulator.<Cell>instance(),transactionStore);
    }

    public MinorCheckpointRowCompactor(long mat,
                                       CellTypeParser ctParser,
                                       TxnSupplier transactionStore,
                                       RollForward rollForward){
        //we cannot perform accumulations in a minor compaction
        super(mat,ctParser,NoOpAccumulator.<Cell>instance(),transactionStore,rollForward);
    }

    @Override
    protected boolean accumulateUserData(Cell cell) throws IOException{
        /*
         * Because we are in a minor compaction, we cannot merge together multiple
         * rows (since we may be missing a file which contains intermediate versions).
         * Thus, we cannot accumulate user data, and this record should be appended
         * (we know it's not rolled back, and that it's version falls above the discard
         * point or else we would not be in this call).
         */
        return false;
    }

    @Override
    protected boolean adjustTombstoneDiscardPoint(long ts){
        /*
         * Because we are in a minor compaction, we cannot adjust the discard point
         * (because we may be missing a file which contains intermediate versions).
         * Thus, we cannot remove tombstoned rows entirely, and we should append
         * this tombstone to the compacted row.
         *
         * The intuitive thought would be "well, we can at least remove records below us,
         * even if we leave the tombstone itself in place". Unfortunately, this is also not possible,
         * because of the following scenario:
         *
         *
         * 1. T1 begins
         * 2. T2 begins
         * 3. T1 writes tombstone
         * 4. T1 commits (moving MAT to T2)
         * 5. T2 reads row. Because T1 commits after T2 starts, T2 does not see tombstone, but sees version
         * below instead.
         * 6. Compaction occurs, and the version below the tombstone is removed.
         * 7. T2 reads same row again. All versions that were in the HFiles being compacted and which were
         * less than the tombstone were removed, but since we are in a minor compaction, there may be some *other*
         * HFile containing more records below the tombstone. When that happens, T2 will see the next highest
         * version less than the tombstone, which will have different values.
         *
         * This scenario is that of a "Non-repeatable read", which is *not* allowed in Snapshot Isolation (it is
         * only allowed in READ_COMMITTED and READ_UNCOMMITTED isolation levels). As a result,
         * even though we would like to, we cannot remove *any* data due to tombstoning during a minor compaction.
         */
        return false;
    }
}

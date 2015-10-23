package com.splicemachine.si.impl.compaction;

import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 10/21/15
 */
public interface RowCompactor{

    void addCell(Cell cell) throws IOException;

    /**
     * Go from an insertion perspective (where you are adding cells) to a reading perspective (where you
     * are pulling cells out). Use this when you are finished pushing a single row, and are ready to begin
     * pulling items off again.
     */
    void reverse() throws IOException;

    /**
     * @return the currently held row key for this RowCompactor. Note that the returned field <em>may</em>
     * contain an entry even if {@code isEmpty()==true}; if that is the case, the entry should be considered
     * stale, and not treated as important. However, if {@code isEmpty()==false}, this should return the current
     * row key of the data contained in the compactor.
     */
    ByteSlice currentRowKey();
    /**
     * @param destination the list to place records into
     * @param limit the maximum number of cells to add to the destination
     *              list
     * @return true if there are more cells left for this row
     * to generate.
     */
    boolean placeCells(List<Cell> destination, int limit);

    boolean isEmpty();
}

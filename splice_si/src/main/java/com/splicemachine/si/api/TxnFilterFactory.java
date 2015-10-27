package com.splicemachine.si.api;

import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.Cell;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/2/15
 */
public interface TxnFilterFactory{
    /**
     * Create a new Transactional Filter for the region.
     * <p/>
     * This filter is "Unpacked", in the sense that it will not attempt to deal with packed
     * data.
     *
     * @param txn the transaction to create a filter for
     * @return a new transactional filter for the region
     * @throws IOException if something goes wrong.
     */
    TxnFilter<Cell> unpackedFilter(TxnView txn) throws IOException;

    TxnFilter<Cell> noOpFilter(TxnView txn) throws IOException;

    TxnFilter<Cell> packedFilter(TxnView txn,EntryPredicateFilter predicateFilter,boolean countStar) throws IOException;

    DDLFilter ddlFilter(Txn ddlTxn) throws IOException;

}

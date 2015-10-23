package com.splicemachine.si.impl;

import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 11/11/15
 */
public class TestTxnSupplier implements TxnSupplier{

        private Map<Long,TxnView> data;

        public TestTxnSupplier(){
            this.data = new HashMap<>();
        }

        @Override
        public TxnView getTransaction(long txnId) throws IOException{
            return getTransactionFromCache(txnId);
        }

        @Override
        public TxnView getTransaction(long txnId,boolean getDestinationTables) throws IOException{
            return getTransaction(txnId);
        }

        @Override
        public boolean transactionCached(long txnId){
            return getTransactionFromCache(txnId)!=null;
        }

        @Override
        public void cache(TxnView toCache){
            data.put(toCache.getTxnId(),toCache);
        }

        @Override
        public TxnView getTransactionFromCache(long txnId){
            return data.get(txnId);
        }
}

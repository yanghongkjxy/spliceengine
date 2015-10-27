package com.splicemachine.si.impl;

import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 10/12/15
 */
public class NoopTxnFilter implements TxnFilter<Cell>{
    private final DataStore dataStore;
    private final TxnSupplier txnSupplier;

    public NoopTxnFilter(DataStore dataStore,TxnSupplier txnSupplier){
        this.dataStore=dataStore;
        this.txnSupplier=txnSupplier;
    }

    @Override
    public Filter.ReturnCode filterKeyValue(Cell keyValue) throws IOException{
        return Filter.ReturnCode.INCLUDE;
    }

    @Override
    public void nextRow(){

    }

    @Override
    public Cell produceAccumulatedKeyValue(){
        return null;
    }

    @Override
    public boolean getExcludeRow(){
        return false;
    }

    @Override
    public CellType getType(Cell keyValue) throws IOException{
        return dataStore.getKeyValueType(keyValue);
    }

    @Override public DataStore getDataStore(){ return dataStore; }

    @Override
    public boolean isPacked(){
        return false;
    }

    @Override
    public TxnSupplier getTxnSupplier(){
        return txnSupplier;
    }

    @Override public TxnView unwrapReadingTxn(){ return null; }
}

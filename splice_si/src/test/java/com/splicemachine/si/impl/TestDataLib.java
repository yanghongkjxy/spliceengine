package com.splicemachine.si.impl;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 10/23/15
 */
public class TestDataLib implements SDataLib{
    @Override
    public byte[] newRowKey(Object[] args){
        return new byte[0];
    }

    @Override
    public byte[] encode(Object value){
        return new byte[0];
    }

    @Override
    public List listResult(Result result){
        return null;
    }

    @Override
    public OperationWithAttributes newPut(byte[] key){
        return null;
    }

    @Override
    public OperationWithAttributes newPut(ByteSlice key){
        return null;
    }

    @Override
    public OperationWithAttributes newPut(byte[] key,Integer lock){
        return null;
    }

    @Override
    public void addKeyValueToPut(OperationWithAttributes operationWithAttributes,byte[] family,byte[] qualifier,long timestamp,byte[] value){

    }

    @Override
    public Iterable listPut(OperationWithAttributes operationWithAttributes){
        return null;
    }

    @Override
    public byte[] getPutKey(OperationWithAttributes operationWithAttributes){
        return new byte[0];
    }

    @Override
    public void setGetTimeRange(OperationWithAttributes operationWithAttributes,long minTimestamp,long maxTimestamp){

    }

    @Override
    public void setGetMaxVersions(OperationWithAttributes operationWithAttributes){

    }

    @Override
    public void setGetMaxVersions(OperationWithAttributes operationWithAttributes,int max){

    }

    @Override
    public void setScanTimeRange(Object get,long minTimestamp,long maxTimestamp){

    }

    @Override
    public void setScanMaxVersions(Object get){

    }

    @Override
    public Object newDelete(byte[] rowKey){
        return null;
    }

    @Override
    public void addFamilyQualifierToDelete(Object o,byte[] family,byte[] qualifier,long timestamp){

    }

    @Override
    public void addDataToDelete(Object o,Object o2,long timestamp){

    }

    @Override
    public OperationWithAttributes toPut(KVPair kvPair,byte[] family,byte[] column,long longTransactionId){
        return null;
    }

    @Override
    public boolean singleMatchingColumn(Object element,byte[] family,byte[] qualifier){
        return false;
    }

    @Override
    public boolean singleMatchingFamily(Object element,byte[] family){
        return false;
    }

    @Override
    public boolean singleMatchingQualifier(Object element,byte[] qualifier){
        return false;
    }

    @Override
    public boolean matchingQualifier(Object element,byte[] qualifier){
        return false;
    }

    @Override
    public boolean matchingValue(Object element,byte[] value){
        return false;
    }

    @Override
    public boolean matchingRowKeyValue(Object element,Object other){
        return false;
    }

    @Override
    public Object newValue(Object element,byte[] value){
        return null;
    }

    @Override
    public Object newValue(byte[] rowKey,byte[] family,byte[] qualifier,Long timestamp,byte[] value){
        return null;
    }

    @Override
    public boolean isAntiTombstone(Object element,byte[] antiTombstone){
        return false;
    }

    @Override
    public Comparator getComparator(){
        return null;
    }

    @Override
    public long getTimestamp(Object element){
        return ((Cell)element).getTimestamp();
    }

    @Override
    public String getFamilyAsString(Object element){
        return null;
    }

    @Override
    public String getQualifierAsString(Object element){
        return null;
    }

    @Override
    public void setRowInSlice(Object element,ByteSlice slice){

    }

    @Override
    public boolean isFailedCommitTimestamp(Object element){
        return false;
    }

    @Override
    public Object newTransactionTimeStampKeyValue(Object element,byte[] value){
        return null;
    }

    @Override
    public long getValueLength(Object element){
        return 0;
    }

    @Override
    public long getValueToLong(Object element){
        return 0;
    }

    @Override
    public long optionalValueToLong(Object element,long defaultValue){
        return 0;
    }

    @Override
    public byte[] getDataFamily(Object element){
        return new byte[0];
    }

    @Override
    public byte[] getDataQualifier(Object element){
        return new byte[0];
    }

    @Override
    public byte[] getDataValue(Object element){
        return new byte[0];
    }

    @Override
    public byte[] getDataRow(Object element){
        return new byte[0];
    }

    @Override
    public byte[] getDataValueBuffer(Object element){
        return ((Cell)element).getValueArray();
    }

    @Override
    public byte[] getDataRowBuffer(Object element){
        return ((Cell)element).getRowArray();
    }

    @Override
    public byte[] getDataQualifierBuffer(Object element){
        return new byte[0];
    }

    @Override
    public int getDataQualifierOffset(Object element){
        return 0;
    }

    @Override
    public int getDataRowOffset(Object element){
        return 0;
    }

    @Override
    public int getDataRowlength(Object element){
        return ((Cell)element).getRowLength();
    }

    @Override
    public int getDataValueOffset(Object element){
        return ((Cell)element).getValueOffset();
    }

    @Override
    public int getDataValuelength(Object element){
        return ((Cell)element).getValueLength();
    }

    @Override
    public int getLength(Object element){
        return 0;
    }

    @Override
    public Result newResult(List element){
        return null;
    }

    @Override
    public Object[] getDataFromResult(Result result){
        return new Object[0];
    }

    @Override
    public Object getColumnLatest(Result result,byte[] family,byte[] qualifier){
        return null;
    }

    @Override
    public boolean regionScannerNext(RegionScanner regionScanner,List data) throws IOException{
        return false;
    }

    @Override
    public void setThreadReadPoint(RegionScanner delegate){

    }

    @Override
    public boolean regionScannerNextRaw(RegionScanner regionScanner,List data) throws IOException{
        return false;
    }

    @Override
    public MeasuredRegionScanner getBufferedRegionScanner(HRegion region,RegionScanner delegate,Object o,int bufferSize,MetricFactory metricFactory){
        return null;
    }

    @Override
    public MeasuredRegionScanner getRateLimitedRegionScanner(HRegion region,RegionScanner delegate,Object o,int bufferSize,int readsPerSecond,MetricFactory metricFactory){
        return null;
    }

    @Override
    public Filter getActiveTransactionFilter(long beforeTs,long afterTs,byte[] destinationTable){
        return null;
    }

    @Override
    public boolean internalScannerNext(InternalScanner internalScanner,List data) throws IOException{
        return false;
    }

    @Override
    public boolean internalScannerNext(InternalScanner internalScanner,List data,int limit) throws IOException{
        return false;
    }

    @Override
    public Object matchKeyValue(Iterable kvs,byte[] columnFamily,byte[] qualifier){
        return null;
    }

    @Override
    public Object matchKeyValue(Object[] kvs,byte[] columnFamily,byte[] qualifier){
        return null;
    }

    @Override
    public Object matchDataColumn(Object[] kvs){
        return null;
    }

    @Override
    public Object matchDataColumn(List kvs){
        return null;
    }

    @Override
    public Object matchDataColumn(Result result){
        return null;
    }

    @Override
    public boolean isDataInRange(Object o,Pair range){
        return false;
    }

    @Override
    public Object newScan(byte[] startRowKey,byte[] endRowKey,List families,List columns,Long effectiveTimestamp){
        return null;
    }

    @Override
    public OperationWithAttributes newGet(byte[] rowKey,List families,List columns,Long effectiveTimestamp,int maxVersions){
        return null;
    }

    @Override
    public OperationWithAttributes newGet(byte[] rowKey,List families,List columns,Long effectiveTimestamp){
        return null;
    }

    @Override
    public Object decode(byte[] value,int offset,int length,Class type){
        return null;
    }

    @Override
    public Object decode(byte[] value,Class type){
        return null;
    }
}

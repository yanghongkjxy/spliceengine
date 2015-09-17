package com.splicemachine.derby.tools;

import com.carrotsearch.hppc.*;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.dvd.V2SerializerMap;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

import java.util.*;
import java.util.BitSet;

/**
 * @author Scott Fines
 *         Date: 9/24/15
 */
public class TestRowEncoder{
    private final KeyEncoder keyEncoder;
    private final DataHash<ExecRow> rowEncoder;
    private Map<Long, List<Cell>> encodedData;
    private NavigableMap<Long, ExecRow> expectedRowData;
    private byte[] rowKey;
    private final KryoPool kryoPool;
    private boolean deleted;

    public Map<Long, List<Cell>> encodedData(){
        return encodedData;
    }

    public Map<Long, ExecRow> expectedData(){
        return expectedRowData;
    }

    public byte[] rowKey(){
        return rowKey;
    }

    public TestRowEncoder(KeyEncoder keyEncoder,DataHash<ExecRow> rowEncoder,KryoPool kp){
        this.keyEncoder=keyEncoder;
        this.rowEncoder=rowEncoder;
        this.kryoPool = kp;
        Comparator<Long> reverseLongEncoder=new Comparator<Long>(){
            @Override
            public int compare(Long o1,Long o2){
                return -1*o1.compareTo(o2);
            }
        };
        this.encodedData=new TreeMap<Long,List<Cell>>(reverseLongEncoder){
            @Override
            public List<Cell> get(Object key){
                List<Cell> o=super.get(key);
                if(o==null){
                    o = new ArrayList<>(4);
                    super.put((Long)key,o);
                }
                return o;
            }
        };
        this.expectedRowData=new TreeMap<>(reverseLongEncoder);
    }

    public void reset(){
        rowKey=null;
        expectedRowData.clear();
        encodedData.clear();
        deleted = false;
    }

    public TestRowEncoder insert(long timestamp,ExecRow rowToInsert) throws Exception{
        byte[] rk=keyEncoder.getKey(rowToInsert);
        if(rowKey==null)
            rowKey = rk;
        rowEncoder.setRow(rowToInsert);
        byte[] value=rowEncoder.encode();

        Cell valueCell=new KeyValue(rowKey,
                FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSpliceConstants.PACKED_COLUMN_BYTES,timestamp,value);
        encodedData.get(timestamp).add(valueCell);
        expectedRowData.put(timestamp,rowToInsert.getClone());
        if(deleted){
            Cell atCell=new KeyValue(rowKey,
                    FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                    FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                    timestamp,FixedSIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES);
            encodedData.get(timestamp).add(atCell);
        }
        return this;
    }

    public TestRowEncoder delete(long timestamp) throws Exception{
        Cell tombstoneCell=new KeyValue(rowKey,FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,timestamp,new byte[]{});
        encodedData.get(timestamp).add(tombstoneCell);
        expectedRowData.put(timestamp,new ValueRow());
        deleted=true;
//        rowKey=null;
        return this;
    }

    public TestRowEncoder update(long timestamp,ExecRow delta) throws Exception{
        int[] explCols =IntArrays.count(delta.nColumns());
        return update(timestamp,delta,explCols);
    }

    public TestRowEncoder update(long timestamp,ExecRow row, int[] colEncodingMap) throws Exception{
        assert rowKey!=null: "No insert was performed before the update!";
        assert !deleted: "The row was deleted!";
        V2SerializerMap v2SerializerMap=new V2SerializerMap(false,kryoPool);
        EntryDataHash updateRe = new EntryDataHash(colEncodingMap,null,kryoPool,v2SerializerMap.getSerializers(row)){
            @Override
            protected com.carrotsearch.hppc.BitSet getNotNullFields(ExecRow row,com.carrotsearch.hppc.BitSet notNullFields){
                notNullFields.clear();
                if(keyColumns!=null){
                    for(int keyColumn:keyColumns){
                        if(keyColumn<0) continue;
                        notNullFields.set(keyColumn);
                    }
                    return notNullFields;
                }else{
                    return super.getNotNullFields(row,notNullFields);
                }
            }
        };
        updateRe.setRow(row);
        byte[] value=updateRe.encode();

        Cell valueCell=new KeyValue(rowKey,FixedSpliceConstants.DEFAULT_FAMILY_BYTES,FixedSpliceConstants.PACKED_COLUMN_BYTES,timestamp,value);
        encodedData.get(timestamp).add(valueCell);
        expectedRowData.put(timestamp,mergeBelow(timestamp,row,colEncodingMap));
        return this;
    }

    private ExecRow mergeBelow(long timestamp,ExecRow delta,int[] colEncodingMap){
        ExecRow mergedRow=delta.getClone();
        SortedMap<Long, ExecRow> priorOperations=expectedRowData.tailMap(timestamp);
        for(ExecRow below : priorOperations.values()){
            merge(mergedRow,below,colEncodingMap);
        }
        return mergedRow;
    }

    private void merge(ExecRow mergedRow,ExecRow below,int[] setCols){
        DataValueDescriptor[] toMerge=mergedRow.getRowArray();
        DataValueDescriptor[] belowDvds=below.getRowArray();
        for(int i=0;i<toMerge.length;i++){
            int mp = setCols[i];
            if(mp<0)
                toMerge[i]=belowDvds[i];
        }
    }

    public TestRowEncoder commit(long timestamp,long commitTimestamp) throws Exception{
        Cell commitTimestampCell=new KeyValue(rowKey,FixedSpliceConstants.DEFAULT_FAMILY_BYTES,
                FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,timestamp,Bytes.toBytes(commitTimestamp));
        encodedData.get(timestamp).add(commitTimestampCell);
        return this;
    }

    public ExecRow expectedFinalOutput(){
        Map.Entry<Long,ExecRow> latest = expectedRowData.firstEntry();
        if(latest==null) return null;
        return latest.getValue();
    }

    public ExecRow expectedFinalOutput(long timestamp){
        Map.Entry<Long,ExecRow> latest = expectedRowData.floorEntry(timestamp);
        if(latest==null) return null;
        return latest.getValue();
    }
}

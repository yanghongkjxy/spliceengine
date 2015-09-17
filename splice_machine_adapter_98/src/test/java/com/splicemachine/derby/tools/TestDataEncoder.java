package com.splicemachine.derby.tools;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.V2SerializerMap;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.kryo.KryoPool;
import com.splicemachine.uuid.Snowflake;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 9/16/15
 */
public class TestDataEncoder{

    private KeyEncoder keyEncoder;
    private EntryDataHash rowHash;
    private List<TestRowEncoder> outstandingEncoders = new LinkedList<>();

    private Map<byte[],List<Cell>> builtData = new TreeMap<byte[],List<Cell>>(Bytes.BYTES_COMPARATOR){
        @Override
        public List<Cell> get(Object key){
            List<Cell> o=super.get(key);
            if(o==null){
                o = new ArrayList<>();
                super.put((byte[])key,o);
            }
            return o;
        }
    };
    private Map<byte[],ExecRow> expectedData = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    private KryoPool kryoPool;

    public TestDataEncoder primaryKeyMap(int[] pkMap,ExecRow template,KryoPool kryoPool){
        this.kryoPool = kryoPool;
        assert template!=null: "No row template available!";
        DescriptorSerializer[] serializers=new V2SerializerMap(true,kryoPool).getSerializers(template);
        if(pkMap!=null){
            DataHash dh=BareKeyHash.encoder(pkMap,null,kryoPool,serializers);
            int[] rowMap=IntArrays.complementMap(pkMap,template.nColumns());
            this.keyEncoder=new KeyEncoder(NoOpPrefix.INSTANCE,dh,NoOpPostfix.INSTANCE);
            this.rowHash=new EntryDataHash(rowMap,null,kryoPool,serializers);
        }else{
            Snowflake snowflake=new Snowflake((short)1);
            this.keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE,NoOpDataHash.INSTANCE,new UniquePostfix(new byte[]{},snowflake.newGenerator(16)));
            this.rowHash = new EntryDataHash(null,null,kryoPool,serializers);

        }
        return this;
    }

    public TestRowEncoder encoder(){
        TestRowEncoder testRowEncoder=new TestRowEncoder(keyEncoder,rowHash,kryoPool){
            @Override
            public void reset(){
                byte[] rowKey=rowKey();
                if(rowKey!=null){
                    List<Cell> cells=builtData.get(rowKey);
                    Map<Long, List<Cell>> data=encodedData();
                    for(List<Cell> versions : data.values()){
                        cells.addAll(versions);
                    }
                    Collections.sort(cells,new KeyValue.KVComparator());

                    expectedData.put(rowKey,expectedFinalOutput());
                }
                super.reset();
            }
        };
        outstandingEncoders.add(testRowEncoder);
        return testRowEncoder;
    }

    public Map<byte[],List<Cell>> encodedData(){
        for(TestRowEncoder tre: outstandingEncoders){
           tre.reset();
        }
        return builtData;
    }

    private static final Predicate<ExecRow> deletePredicate=new Predicate<ExecRow>(){
        @Override
        public boolean apply(ExecRow execRow){
            //remove deleted rows
            return execRow!=null && execRow.nColumns()>0;
        }
    };
    public Collection<ExecRow> expectedData(){
        return Collections2.filter(expectedData.values(),deletePredicate);
    }
}

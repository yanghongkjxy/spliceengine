package com.splicemachine.derby.utils.marshall.dvd;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Throwables;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.shared.common.udt.UDTBase;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.kryo.KryoPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/2/14
 */
public class KryoDescriptorSerializer implements DescriptorSerializer, Closeable{
    private final KryoPool kryoPool;
    private ClassFactory cf;

    private Output output;
    private Input input;

    public KryoDescriptorSerializer(KryoPool kryoPool){
        this.kryoPool=kryoPool;
    }

    public static Factory newFactory(final KryoPool kryoPool){
        return new Factory(){
            @Override
            public DescriptorSerializer newInstance(){
                return new KryoDescriptorSerializer(kryoPool);
            }

            @Override
            public boolean applies(DataValueDescriptor dvd){
                if(dvd==null)
                    return false;

                return applies(dvd.getTypeFormatId());
            }

            @Override
            public boolean applies(int typeFormatId){
                switch(typeFormatId){
                    // Starting with Fuji release, we handle SQL_REF serialization
                    // with RefDescriptorSerilizer class.
                    // case StoredFormatIds.SQL_REF_ID:
                    case StoredFormatIds.SQL_USERTYPE_ID_V3:
                        return true;
                    default:
                        return false;
                }
            }

            @Override
            public boolean isScalar(){
                return false;
            }

            @Override
            public boolean isFloat(){
                return false;
            }

            @Override
            public boolean isDouble(){
                return false;
            }
        };
    }


    @Override
    public void encode(MultiFieldEncoder fieldEncoder,DataValueDescriptor dvd,boolean desc) throws StandardException{
        initializeForWrite();
        Object o=dvd.getObject();
        Kryo kryo=kryoPool.get();
        try{
            kryo.writeClassAndObject(output,o);
            fieldEncoder.encodeNextUnsorted(output.toBytes());
        }finally{
            kryoPool.returnInstance(kryo);
        }
    }


    @Override
    public byte[] encodeDirect(DataValueDescriptor dvd,boolean desc) throws StandardException{
        initializeForWrite();
        Object o=dvd.getObject();
        Kryo kryo=kryoPool.get();
        try{
            kryo.writeClassAndObject(output,o);
            return Encoding.encodeBytesUnsorted(output.toBytes());
        }finally{
            kryoPool.returnInstance(kryo);
        }
    }

    @Override
    public void decode(MultiFieldDecoder fieldDecoder,DataValueDescriptor destDvd,boolean desc) throws StandardException{
        initializeForReads();
        input.setBuffer(fieldDecoder.decodeNextBytesUnsorted());
        Kryo kryo=kryoPool.get();
        Class clazz;
        try{
            clazz= kryo.readClass(input).getType();
            Object o=kryo.readObject(input,clazz);
            destDvd.setValue(o);
        }finally{
            kryoPool.returnInstance(kryo);
        }
    }


    @Override
    public void decodeDirect(DataValueDescriptor dvd,byte[] data,int offset,int length,boolean desc) throws StandardException{
        initializeForReads();
        input.setBuffer(Encoding.decodeBytesUnsortd(data,offset,length));
        Kryo kryo=kryoPool.get();
        try{
            Object o=kryo.readClassAndObject(input);
            dvd.setValue(o);
        }finally{
            kryoPool.returnInstance(kryo);
        }
    }

    @Override
    public boolean isScalarType(){
        return false;
    }

    @Override
    public boolean isFloatType(){
        return false;
    }

    @Override
    public boolean isDoubleType(){
        return false;
    }

    @Override
    public void close() throws IOException{
    }

    private void initializeForReads(){
        if(input==null)
            input=new Input();
    }

    private void initializeForWrite(){
        if(output==null)
            output=new Output(20,-1);
        else
            output.clear();
    }

}

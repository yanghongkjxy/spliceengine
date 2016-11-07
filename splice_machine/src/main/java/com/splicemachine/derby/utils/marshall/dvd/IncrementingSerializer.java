/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.primitives.Bytes;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/7/16
 */
public class IncrementingSerializer implements DescriptorSerializer{
    private final DescriptorSerializer delegate;

    public IncrementingSerializer(DescriptorSerializer delegate){
        this.delegate=delegate;
    }

    @Override
    public void encode(MultiFieldEncoder fieldEncoder,DataValueDescriptor dvd,boolean desc) throws StandardException{
        byte[] bytes=delegate.encodeDirect(dvd,desc);
        Bytes.unsignedIncrement(bytes,bytes.length-1);
        fieldEncoder.setRawBytes(bytes);
    }

    @Override
    public byte[] encodeDirect(DataValueDescriptor dvd,boolean desc) throws StandardException{
        byte[] bytes=delegate.encodeDirect(dvd,desc);
        Bytes.unsignedIncrement(bytes,bytes.length-1);
        return bytes;
    }

    @Override
    public void decode(MultiFieldDecoder fieldDecoder,DataValueDescriptor destDvd,boolean desc) throws StandardException{
        delegate.decode(fieldDecoder,destDvd,desc);
    }

    @Override
    public void decodeDirect(DataValueDescriptor dvd,byte[] data,int offset,int length,boolean desc) throws StandardException{
        delegate.decodeDirect(dvd,data,offset,length,desc);
    }

    @Override
    public boolean isScalarType(){
        return delegate.isScalarType();
    }

    @Override
    public boolean isFloatType(){
        return delegate.isFloatType();
    }

    @Override
    public boolean isDoubleType(){
        return delegate.isDoubleType();
    }

    @Override
    public void close() throws IOException{
        delegate.close();
    }
}

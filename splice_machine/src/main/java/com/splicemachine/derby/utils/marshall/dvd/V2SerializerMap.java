package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.utils.kryo.KryoPool;

/**
 * @author Scott Fines
 *         Date: 4/7/14
 */
public class V2SerializerMap extends V1SerializerMap{

    public static volatile V2SerializerMap SPARSE_MAP;//=new V1SerializerMap(true);
    public static volatile V2SerializerMap DENSE_MAP;//=new V1SerializerMap(false);

    public static V2SerializerMap getSparse(){
        V2SerializerMap s = SPARSE_MAP;
        if(s==null){
            synchronized(V2SerializerMap.class){
                s = SPARSE_MAP;
                if(s==null)
                    s = SPARSE_MAP = new V2SerializerMap(true);
            }
        }
        return s;
    }

    public static V2SerializerMap getDense(){
        V2SerializerMap s = DENSE_MAP;
        if(s==null){
            synchronized(V2SerializerMap.class){
                s = DENSE_MAP;
                if(s==null)
                    s = DENSE_MAP = new V2SerializerMap(false);
            }
        }
        return s;
    }

    public static final String VERSION="2.0";

    public static V2SerializerMap instance(boolean sparse){
        return sparse?getSparse():getDense();
    }

    public V2SerializerMap(boolean sparse){
        super(sparse);
    }

	public V2SerializerMap(boolean sparse,KryoPool kryoPool){
		super(sparse,kryoPool);
	}
	
		@Override
		protected void populateFactories(boolean sparse,KryoPool kp) {
				factories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[1]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.factory(ScalarDescriptorSerializer.INSTANCE_FACTORY, VERSION),sparse);
				factories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[3]  = NullDescriptorSerializer.doubleChecker(DoubleDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[4]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.factory(StringDescriptorSerializer.INSTANCE_FACTORY,VERSION),sparse);
				factories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(kp),sparse);
				factories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[8]  = NullDescriptorSerializer.nullFactory(LazyTimeValuedSerializer.newFactory(TimestampV2DescriptorSerializer.INSTANCE_FACTORY, VERSION),sparse);
				factories[9]  = NullDescriptorSerializer.nullFactory(UnsortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[10] = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.factory(DecimalDescriptorSerializer.INSTANCE_FACTORY,VERSION),sparse);
				factories[11] = NullDescriptorSerializer.nullFactory(RefDescriptorSerializer.INSTANCE_FACTORY, sparse);
//				factories[12] = NullDescriptorSerializer.nullFactory(UDTDescriptorSerializer.INSTANCE_FACTORY, sparse);

			    eagerFactories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[1]=NullDescriptorSerializer.nullFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[2]=NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[3]=NullDescriptorSerializer.doubleChecker(DoubleDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[4]  = NullDescriptorSerializer.nullFactory(StringDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(kp),sparse);
				eagerFactories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[8]  = NullDescriptorSerializer.nullFactory(TimestampV2DescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[9]  = NullDescriptorSerializer.nullFactory(UnsortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[10] = NullDescriptorSerializer.nullFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[11] = NullDescriptorSerializer.nullFactory(RefDescriptorSerializer.INSTANCE_FACTORY,sparse);
//				eagerFactories[12] = NullDescriptorSerializer.nullFactory(UDTDescriptorSerializer.INSTANCE_FACTORY, sparse);

        eagerFactories[0]=NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[1]=NullDescriptorSerializer.nullFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[2]=NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[3]=NullDescriptorSerializer.doubleChecker(DoubleDescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[4]=NullDescriptorSerializer.nullFactory(StringDescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[5]=NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(kryoPool),sparse);
        eagerFactories[6]=NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[7]=NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[8]=NullDescriptorSerializer.nullFactory(TimestampV2DescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[9]=NullDescriptorSerializer.nullFactory(UnsortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[10]=NullDescriptorSerializer.nullFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,sparse);
        eagerFactories[11]=NullDescriptorSerializer.nullFactory(RefDescriptorSerializer.INSTANCE_FACTORY,sparse);
    }
}

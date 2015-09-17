package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.index.BitIndex;

/**
 * @author Scott Fines
 *         Date: 3/11/14
 */
public abstract class BaseEntryAccumulator<T extends EntryAccumulator<T>> implements EntryAccumulator<T>{

    protected EntryAccumulationSet accumulationSet;

    protected final boolean returnIndex;

    protected final EntryPredicateFilter predicateFilter;
    protected long finishCount;

    protected BaseEntryAccumulator(EntryPredicateFilter predicateFilter,boolean returnIndex,BitSet fieldsToCollect){
        this.returnIndex=returnIndex;
        this.predicateFilter=predicateFilter;

        if(fieldsToCollect!=null && !fieldsToCollect.isEmpty()){
            this.accumulationSet=new SparseAccumulationSet(fieldsToCollect);
        }else
            this.accumulationSet=new AlwaysAcceptAccumulationSet();
    }

    @Override
    public void add(int position,byte[] data,int offset,int length){
        if(accumulationSet.get(position))
            return;
        occupy(position,data,offset,length);
        accumulationSet.addUntyped(position);
    }


    @Override
    public void addScalar(int position,byte[] data,int offset,int length){
        if(accumulationSet.get(position))
            return;
        occupyScalar(position,data,offset,length);
        accumulationSet.addScalar(position);
    }

    @Override
    public void addFloat(int position,byte[] data,int offset,int length){
        if(accumulationSet.get(position))
            return;
        occupyFloat(position,data,offset,length);
        accumulationSet.addFloat(position);
    }

    @Override
    public void addDouble(int position,byte[] data,int offset,int length){
        if(accumulationSet.get(position))
            return;
        occupyDouble(position,data,offset,length);
        accumulationSet.addDouble(position);
    }

    protected abstract void occupy(int position,byte[] data,int offset,int length);

    protected abstract void occupyDouble(int position,byte[] data,int offset,int length);

    protected abstract void occupyFloat(int position,byte[] data,int offset,int length);

    protected abstract void occupyScalar(int position,byte[] data,int offset,int length);


    @Override
    public BitSet getRemainingFields(){
        return accumulationSet.remainingFields();
    }

    @Override
    public boolean isFinished(){
        return accumulationSet.isFinished();
    }

    @Override
    public void reset(){
        accumulationSet.reset();
        if(predicateFilter!=null)
            predicateFilter.reset();
    }

    protected abstract boolean matchField(int myFields,T otherAccumulator);

    @Override
    public boolean isInteresting(BitIndex potentialIndex){
        return accumulationSet.isInteresting(potentialIndex);
    }

    @Override
    public void complete(){
        accumulationSet.complete();
    }

    @Override
    public boolean hasAccumulated(){
        return accumulationSet.hasAccumulated();
    }
}

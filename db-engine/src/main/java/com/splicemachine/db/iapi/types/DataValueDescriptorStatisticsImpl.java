package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 *
 * Statistics Container for a column.
 *
 */
public class DataValueDescriptorStatisticsImpl implements DataValueDescriptorStatistics {
    private com.yahoo.sketches.quantiles.ItemsSketch<DataValueDescriptor> quantilesSketch;
    private com.yahoo.sketches.frequencies.ItemsSketch<DataValueDescriptor> frequenciesSketch;
    private Sketch thetaSketch;
    private long nullCount;

    public DataValueDescriptorStatisticsImpl(DataValueDescriptor dvd) throws StandardException {
        this(dvd.getQuantilesSketch(),dvd.getFrequenciesSketch(),dvd.getThetaSketch(),0l);
    }

    public DataValueDescriptorStatisticsImpl(com.yahoo.sketches.quantiles.ItemsSketch quantilesSketch,
                                             com.yahoo.sketches.frequencies.ItemsSketch frequenciesSketch,
                                             Sketch thetaSketch, long nullCount
                                         ) throws StandardException {
        this.quantilesSketch = quantilesSketch;
        this.frequenciesSketch = frequenciesSketch;
        this.thetaSketch = thetaSketch;
        this.nullCount = nullCount;
    }

    @Override
    public DataValueDescriptor minValue() {
        return quantilesSketch.getMinValue();
    }

    @Override
    public long nullCount() {
        return nullCount;
    }

    @Override
    public long notNullCount() {
        return quantilesSketch.getN();
    }

    @Override
    public long cardinality() {
        return (long)thetaSketch.getEstimate();
    }

    @Override
    public DataValueDescriptor maxValue() {
        return quantilesSketch.getMaxValue();
    }

    @Override
    public long totalCount() {
        return quantilesSketch.getN()+nullCount;
    }

    @Override
    public long selectivity(DataValueDescriptor element) {
        // Null
        if (element == null || element.isNull())
            return nullCount;
        // Frequent Items
        long count = frequenciesSketch.getEstimate(element);
        if (count>0)
            return count;
        // Return Cardinality
        return (long) (quantilesSketch.getN()/thetaSketch.getEstimate()); // Should we remove frequent items?
    }

    @Override
    public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
        double startSelectivity = start==null||start.isNull()?0.0d:quantilesSketch.getCDF(new DataValueDescriptor[]{start})[0];
        double stopSelectivity = stop==null||stop.isNull()?1.0d:quantilesSketch.getCDF(new DataValueDescriptor[]{stop})[0];
        return (long) ((stopSelectivity-startSelectivity)*quantilesSketch.getN());
    }

    @Override
    public ItemsSketch getQuantilesSketch() {
        return quantilesSketch;
    }

    @Override
    public com.yahoo.sketches.frequencies.ItemsSketch getFrequenciesSketch() {
        return frequenciesSketch;
    }

    @Override
    public Sketch getThetaSketch() {
        return thetaSketch;
    }

    public static DataValueDescriptorStatistics merge(
            DataValueDescriptorStatistics[] dataValueDescriptorStatisticsArray,
            DataValueDescriptor dvd) throws StandardException {
        Union thetaSketchUnion = Sketches.setOperationBuilder().buildUnion();
        ItemsUnion quantilesSketchUnion = ItemsUnion.getInstance(dvd);
        com.yahoo.sketches.frequencies.ItemsSketch itemsSketch = new com.yahoo.sketches.frequencies.ItemsSketch(1024);
        long nullCount = 0l;
        for (int i = 0; i < dataValueDescriptorStatisticsArray.length; i++) {
            thetaSketchUnion.update(dataValueDescriptorStatisticsArray[i].getThetaSketch());
            quantilesSketchUnion.update(dataValueDescriptorStatisticsArray[i].getQuantilesSketch());
            itemsSketch.merge(dataValueDescriptorStatisticsArray[i].getFrequenciesSketch());
            nullCount += dataValueDescriptorStatisticsArray[i].nullCount();
        }
        return new DataValueDescriptorStatisticsImpl(quantilesSketchUnion.getResult(),
                itemsSketch,
                thetaSketchUnion.getResult(),
                nullCount
        );
    }

    @Override
    public void update(DataValueDescriptor dvd) {
        if (dvd.isNull()) {
            nullCount++;
        } else {
            frequenciesSketch.update(dvd);
            quantilesSketch.update(dvd);
            dvd.updateThetaSketch((UpdateSketch) thetaSketch);
        }
    }

    @Override
    public String toString() {
        return String.format("Statistics{nullCount=%d, frequencies=%s, quantiles=%s, theta=%s}",nullCount,frequenciesSketch,quantilesSketch.toString(true,false),thetaSketch.toString());
    }
}
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
 */
package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 *
 * Statistics Container for a column.
 *
 */
public class ColumnStatisticsImpl implements ItemStatistics<DataValueDescriptor> {
    protected com.yahoo.sketches.quantiles.ItemsSketch<DataValueDescriptor> quantilesSketch;
    protected com.yahoo.sketches.frequencies.ItemsSketch<DataValueDescriptor> frequenciesSketch;
    protected Sketch thetaSketch;
    protected long nullCount;
    protected DataValueDescriptor dvd;

    public ColumnStatisticsImpl(DataValueDescriptor dvd) throws StandardException {
        this(dvd, dvd.getQuantilesSketch(),dvd.getFrequenciesSketch(),dvd.getThetaSketch(),0l);
    }

    public ColumnStatisticsImpl(DataValueDescriptor dvd,
                                com.yahoo.sketches.quantiles.ItemsSketch quantilesSketch,
                                com.yahoo.sketches.frequencies.ItemsSketch frequenciesSketch,
                                Sketch thetaSketch, long nullCount
                                         ) throws StandardException {
        this.dvd = dvd;
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
    /*
    public static ItemStatistics merge(
            ItemStatistics[] dataValueDescriptorStatisticsArray,
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
        return new ColumnStatisticsImpl(quantilesSketchUnion.getResult(),
                itemsSketch,
                thetaSketchUnion.getResult(),
                nullCount
        );
    }
    */
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

    @Override
    public ItemStatisticsBuilder<DataValueDescriptor> mergeInto(ItemStatisticsBuilder<DataValueDescriptor> itemStatisticsBuilder) throws StandardException {
        itemStatisticsBuilder.addFrequenciesSketch(frequenciesSketch)
        .addThetaSketch(thetaSketch)
        .addQuantilesSketch(quantilesSketch,dvd)
        .addNullCount(nullCount);
        return itemStatisticsBuilder;
    }
}
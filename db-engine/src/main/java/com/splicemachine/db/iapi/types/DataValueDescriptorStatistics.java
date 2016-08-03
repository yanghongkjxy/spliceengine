package com.splicemachine.db.iapi.types;

import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;

/**
 *
 * Statistics Container for a column.
 *
 */
public interface DataValueDescriptorStatistics {

    DataValueDescriptor minValue();

    DataValueDescriptor maxValue();

    long totalCount();

    long nullCount();

    long notNullCount();

    long cardinality();

    /**
     * @param element the element to match
     * @return the number of entries which are <em>equal</em> to the specified element.
     */
    long selectivity(DataValueDescriptor element);

    /**
     * @param start the start of the range to estimate. If {@code null}, then scan everything before {@code stop}.
     *              If {@code stop} is also {@code null}, then this will return an estimate to the number of entries
     *              in the entire data set.
     * @param stop the end of the range to estimate. If {@code null}, then scan everything after {@code start}.
     *             If {@code start} is also {@code null}, then this will return an estimate of the number of entries
     *             in the entire data set.
     * @param includeStart if {@code true}, then include entries which are equal to {@code start}
     * @param includeStop if {@code true}, then include entries which are <em>equal</em> to {@code stop}
     * @return the number of rows which fall in the range {@code start},{@code stop}, with
     * inclusion determined by {@code includeStart} and {@code includeStop}
     */
    long rangeSelectivity(DataValueDescriptor start,DataValueDescriptor stop, boolean includeStart,boolean includeStop);

    public ItemsSketch getQuantilesSketch();

    public com.yahoo.sketches.frequencies.ItemsSketch getFrequenciesSketch();

    public Sketch getThetaSketch();

    public void update(DataValueDescriptor dvd);

}
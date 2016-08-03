/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.iapi.sql.dictionary;

import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;

/**
 *
 * Column Statistics Descriptor for the column statistics descriptor.
 *
 *
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class ColumnStatsDescriptor  extends TupleDescriptor {
    private long conglomerateId;
    private String partitionId;
    private int columnId;
    private com.yahoo.sketches.quantiles.ItemsSketch quantileSketch;
    private com.yahoo.sketches.frequencies.ItemsSketch frequencySketch;
    private Sketch thetaSketch;

    public ColumnStatsDescriptor(long conglomerateId,
                                 String partitionId,
                                 int columnId,
                                 com.yahoo.sketches.quantiles.ItemsSketch quantileSketch,
                                 com.yahoo.sketches.frequencies.ItemsSketch frequencySketch,
                                 Sketch thetaSketch) {
        this.conglomerateId = conglomerateId;
        this.partitionId = partitionId;
        this.columnId = columnId;
        this.quantileSketch = quantileSketch;
        this.frequencySketch = frequencySketch;
        this.thetaSketch = thetaSketch;
    }

    public int getColumnId() {
        return columnId;
    }

    public long getConglomerateId() {
        return conglomerateId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public ItemsSketch getQuantileSketch() {
        return quantileSketch;
    }

    public com.yahoo.sketches.frequencies.ItemsSketch getFrequencySketch() {
        return frequencySketch;
    }

    public Sketch getThetaSketch() {
        return thetaSketch;
    }

}

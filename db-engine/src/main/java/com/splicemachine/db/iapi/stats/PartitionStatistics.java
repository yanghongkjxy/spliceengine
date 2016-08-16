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

package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;

import java.util.List;

/**
 * Representation of Partition-level statistics.
 *
 * Partition-level statistics are statistics aboud the partition itself--how many rows,
 * the width of those rows, and some physical statistics about that. These
 * partitions are designed to be mergeable into either a server-level, or a table-level
 * view of that data.
 *
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface PartitionStatistics {
    /**
     * @return the total number of rows in the partition.
     */
    long rowCount();
    /**
     * @return the total size of the partition (in bytes).
     */
    long totalSize();

    /**
     * @return the average width of a single row (in bytes) in this partition. This includes
     * the row key and cell contents.
     */
    int avgRowWidth();
    /**
     * @return a unique identifier for this partition
     */
    String partitionId();

    /**
     * @return the unique identifier for the table to which this partition belongs
     */
    String tableId();

    /**
     * @return Statistics about individual columns (which were most recently collected).
     */
    List<? extends ItemStatistics> getAllColumnStatistics();

    /**
     * @param columnId the identifier of the column to fetch(indexed from 0)
     * @return statistics for the column, if such statistics exist, or {@code null} if
     * no statistics are available for that column.
     */
    ItemStatistics getColumnStatistics(int columnId);

    void mergeItemStatistics(ItemStatisticsBuilder[] itemStatisticsBuilders) throws StandardException;
}


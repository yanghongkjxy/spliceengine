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

import java.util.List;

/**
 * Representation of Table-level statistics.
 *
 * Table statistics are statistics about the overall table itself, accumulated over all partitions.
 *
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface TableStatistics {
    /**
     *
     * The unique identifier for the table
     *
     * @return
     */
    public String tableId();

    /**
     * @return the total number of entries in the table across all partitions
     */
    long rowCount();

    /**
     * @return the total size of the table across all partitions
     */
    long totalSize();

    /**
     * @return the average size of a single partition across all partitions.
     */
    long avgPartitionSize();

    /**
     * @return the average width of a single row (in bytes) across all partitions. This includes
     * the row key and cell contents.
     */
    int avgRowWidth();

    /**
     * @return Detailed statistics for each partition.
     */
    List<? extends PartitionStatistics> getPartitionStatistics();

    PartitionStatistics effectiveTableStatistics();
}


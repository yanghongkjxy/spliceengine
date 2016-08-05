
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
 *
 *
 */
public class TableStatisticsImpl implements TableStatistics {
    private String tableId;
    private List<? extends PartitionStatistics> partitionStatistics;
    private PartitionStatistics effectivePartitionStatistics;

    public TableStatisticsImpl() {

    }

    public TableStatisticsImpl(String tableId,
                               List<? extends PartitionStatistics> partitionStatistics) {
        this.tableId = tableId;
        this.partitionStatistics = partitionStatistics;
    }

    @Override
    public String tableId() {
        return tableId;
    }

    @Override
    public long rowCount() {
        return 0;
    }

    @Override
    public long totalSize() {
        return 0;
    }

    @Override
    public long avgPartitionSize() {
        return 0;
    }

    @Override
    public int avgRowWidth() {
        return 0;
    }

    @Override
    public List<? extends PartitionStatistics> getPartitionStatistics() {
        return partitionStatistics;
    }

    @Override
    public PartitionStatistics effectiveTableStatistics() {
        ItemStatisticsBuilder[] itemStatisticsBuilder = null;
        if (effectivePartitionStatistics==null) {
            for (PartitionStatistics partStats: partitionStatistics) {
                List itemStatistics = partStats.getAllColumnStatistics();
                if (itemStatisticsBuilder ==null)
                    itemStatisticsBuilder = new ItemStatisticsBuilder[itemStatistics.size()];

            }
        }
        return effectivePartitionStatistics;
    }
}

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
 * Created by jleach on 8/4/16.
 */
public class PartitionStatisticsImpl implements PartitionStatistics {
    private String partitionId;
    private String tableId;
    private List<? extends ItemStatistics> itemStatistics;

    public PartitionStatisticsImpl() {

    }

    public PartitionStatisticsImpl(String partitionId,
                                   String tableId,
                                   List<? extends ItemStatistics> itemStatistics) {
        this.partitionId = partitionId;
        this.tableId = tableId;
        this.itemStatistics = itemStatistics;
    }

    @Override
    public long rowCount() {
        return itemStatistics.get(0).nullCount()+itemStatistics.get(0).notNullCount();
    }

    @Override
    public long totalSize() {
        return 0;
    }

    @Override
    public int avgRowWidth() {
        return 0;
    }

    @Override
    public String partitionId() {
        return partitionId;
    }

    @Override
    public String tableId() {
        return tableId;
    }

    @Override
    public List<? extends ItemStatistics> getAllColumnStatistics() {
        return itemStatistics;
    }

    /**
     *
     * This is 1 based with the 0 entry being the key
     *
     * @param columnId the identifier of the column to fetch(indexed from 0)
     * @return
     */
    @Override
    public ItemStatistics getColumnStatistics(int columnId) {
        return itemStatistics.get(columnId);
    }

    @Override
    public void mergeItemStatistics(ItemStatisticsBuilder[] itemStatisticsBuilders) throws StandardException {
        for (int i = 0; i<itemStatistics.size();i++) {
            itemStatisticsBuilders[i] = itemStatistics.get(i).mergeInto(itemStatisticsBuilders[i]);
        }
    }
}

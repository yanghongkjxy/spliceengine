package com.splicemachine.db.iapi.sql.dictionary;


import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class PartitionStatisticsDescriptor extends TupleDescriptor {
    private long conglomerateId;
    private String partitionId;
    private long timestamp;
    private boolean stale;
    private int meanRowWidth;
    private long partitionSize;
    private long rowCount;
    private boolean inProgress;
    private List<ColumnStatsDescriptor> columnStatsDescriptors;

    public PartitionStatisticsDescriptor(long conglomerateId,
                                         String partitionId,
                                         long timestamp,
                                         boolean stale,
                                         boolean inProgress,
                                         long rowCount,
                                         long partitionSize,
                                         int meanRowWidth) {
        this.conglomerateId = conglomerateId;
        this.partitionId = partitionId;
        this.timestamp = timestamp;
        this.stale = stale;
        this.meanRowWidth = meanRowWidth;
        this.partitionSize = partitionSize;
        this.rowCount = rowCount;
        this.inProgress = inProgress;
    }

    public long getConglomerateId() { return conglomerateId; }
    public String getPartitionId() { return partitionId; }
    public long getTimestamp() { return timestamp; }
    public boolean isStale() { return stale; }
    public int getMeanRowWidth() { return meanRowWidth; }
    public long getPartitionSize() { return partitionSize; }
    public long getRowCount() { return rowCount; }
    public boolean isInProgress() { return inProgress; }

    public List<ColumnStatsDescriptor> getColumnStatsDescriptors() {
        return columnStatsDescriptors;
    }

    public void setColumnStatsDescriptors(List<ColumnStatsDescriptor> columnStatsDescriptors) {
        this.columnStatsDescriptors = columnStatsDescriptors;
    }

}
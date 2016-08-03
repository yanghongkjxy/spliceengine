package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptorStatistics;
import com.splicemachine.db.iapi.types.DataValueDescriptorStatisticsImpl;
/**
 * Created by jleach on 8/1/16.
 */
public class StatisticsRow extends ValueRow {
    private DataValueDescriptorStatistics[] statistics;

    public StatisticsRow(ExecRow execRow) throws StandardException {
        assert execRow!=null:"ExecRow passed in is null";
        this.setRowArray(execRow.getRowArray());
        statistics = new DataValueDescriptorStatistics[execRow.nColumns()];
        for (int i = 0; i< execRow.nColumns(); i++) {
            DataValueDescriptor dvd = execRow.getColumn(i+1);
            statistics[i] = new DataValueDescriptorStatisticsImpl(dvd);
        }
    }

    /**
     *
     * Sets the statistical values as well.
     *
     * @param position
     * @param col
     */
    @Override
    public void setColumn(int position, DataValueDescriptor col) {
        super.setColumn(position, col);
        statistics[position-1].update(col);
    }
}
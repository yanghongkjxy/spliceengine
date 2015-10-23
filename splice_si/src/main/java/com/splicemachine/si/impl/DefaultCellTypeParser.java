package com.splicemachine.si.impl;

import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.util.ArrayUtil;

/**
 * @author Scott Fines
 *         Date: 10/22/15
 */
public class DefaultCellTypeParser implements CellTypeParser{
    public static final CellTypeParser INSTANCE = new DefaultCellTypeParser();

    private DefaultCellTypeParser(){ }

    @Override
    public CellType parseCellType(Cell c){
        if (singleMatchingQualifier(c,FixedSIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES)) {
            return CellType.COMMIT_TIMESTAMP;
        } else if (singleMatchingQualifier(c, FixedSpliceConstants.PACKED_COLUMN_BYTES)) {
            return CellType.USER_DATA;
        } else if (singleMatchingQualifier(c, FixedSIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES)) {
            if (matchingValue(c, FixedSIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES)) {
                return CellType.ANTI_TOMBSTONE;
            } else{
                return CellType.TOMBSTONE;
            }
        } else if (singleMatchingQualifier(c, FixedSIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return CellType.FOREIGN_KEY_COUNTER;
        }else if(singleMatchingQualifier(c,FixedSIConstants.SNAPSHOT_ISOLATION_CHECKPOINT_COLUMN_BYTES)){
            return CellType.CHECKPOINT;
        }
        return CellType.OTHER;
    }

    private static boolean singleMatchingQualifier(Cell keyValue, byte[] qualifier) {
        assert qualifier!=null: "Qualifiers should not be null";
        assert qualifier.length==1: "Qualifiers should be of length 1 not " + qualifier.length + " value --" + Bytes.toString(qualifier) + "--";
        return keyValue.getQualifierArray()[keyValue.getQualifierOffset()] == qualifier[0];
    }

    private static boolean matchingValue(Cell keyValue, byte[] value) {
        return !(value == null || keyValue == null || value.length != keyValue.getValueLength()) && ArrayUtil.equals
                (keyValue.getValueArray(),keyValue.getValueOffset(),value,0,keyValue.getValueLength());
    }
}

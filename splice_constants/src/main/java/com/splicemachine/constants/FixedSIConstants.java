package com.splicemachine.constants;

import com.splicemachine.db.iapi.util.UTF8Util;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * SI Constants which are not affected by configuration. Mainly this is useful to avoid construction of a Configuration
 * in testing.
 * @author Scott Fines
 *         Date: 9/16/15
 */
public class FixedSIConstants{
    public static final byte[] TRUE_BYTES = Bytes.toBytes(true);
    public static final byte[] FALSE_BYTES = Bytes.toBytes(false);
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final byte[] SNAPSHOT_ISOLATION_FAILED_TIMESTAMP = new byte[] {-1};
    public static final int TRANSACTION_START_TIMESTAMP_COLUMN = 0;
    public static final int TRANSACTION_PARENT_COLUMN = 1;
    public static final int TRANSACTION_DEPENDENT_COLUMN = 2;
    public static final int TRANSACTION_ALLOW_WRITES_COLUMN = 3;
    public static final int TRANSACTION_READ_UNCOMMITTED_COLUMN = 4;
    public static final int TRANSACTION_READ_COMMITTED_COLUMN = 5;
    public static final int TRANSACTION_STATUS_COLUMN = 6;
    public static final int TRANSACTION_COMMIT_TIMESTAMP_COLUMN = 7;
    public static final int TRANSACTION_KEEP_ALIVE_COLUMN = 8;
    public static final int TRANSACTION_ID_COLUMN = 14;
    public static final int TRANSACTION_COUNTER_COLUMN = 15;
    public static final int TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN = 16;
    public static final int TRANSACTION_ADDITIVE_COLUMN = 17;
    public static final int WRITE_TABLE_COLUMN = 18;
    // Snowflake logic
    public static final byte[] COUNTER_COL = Bytes.toBytes("c");
    public static final String MACHINE_ID_COUNTER = "MACHINE_IDS";
    public static final long MAX_MACHINE_ID = 0xffff; //12 bits of 1s is the maximum machine id available

    /**
     * Splice Columns
     *
     * 0 = contains commit timestamp (optionally written after writing transaction is final)
     * 1 = tombstone (if value empty) or anti-tombstone (if value "0")
     * 7 = encoded user data
     * 9 = column for causing write conflicts between concurrent transactions writing to parent and child FK tables
     */
    public static final byte[] SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES = Bytes.toBytes("0");
    public static final byte[] SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES = Bytes.toBytes("1");
    public static final byte[] SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES = Bytes.toBytes("9");
    public static final byte[] SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES = Bytes.toBytes("0");

    public static final byte[] SI_NEEDED_VALUE_BYTES = Bytes.toBytes((short) 0);

    public static final String SI_TRANSACTION_ID_KEY = "A";
    public static final String SI_NEEDED = "B";
    public static final String SI_DELETE_PUT = "D";
    public static final String SI_COUNT_STAR = "M";

    public static void main(String...args) throws Exception{
        System.out.printf("value:<%s>%n",Bytes.toString(new byte[]{0x47}));
        System.out.printf("value:<%s>%n",Arrays.toString(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES));
    }
}
